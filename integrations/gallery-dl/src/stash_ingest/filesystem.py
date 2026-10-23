"""Reviewed local roots, shared destination locks and flushed final file claims."""

from contextlib import contextmanager
from abc import ABC, abstractmethod
import fcntl
import hashlib
import os
from pathlib import Path
import stat

from .encoding import InvalidData
from .publication_lock import PublicationBusy


class Directory(ABC):
    """Confined file operations with deployment-specific root verification."""

    def __init__(self, path):
        self.path = Path(path).resolve(strict=True)
        self.verify()

    @property
    @abstractmethod
    def binding(self):
        """Opaque comparable binding for profiles sharing one local directory."""

    @abstractmethod
    def verify_descriptor(self, fd):
        """Verify the directory actually opened for this operation."""

    def verify(self):
        fd = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            self.verify_descriptor(fd)
        finally:
            os.close(fd)

    def relative(self, path):
        self.verify()
        try:
            value = Path(path).resolve().relative_to(self.path).as_posix()
        except ValueError:
            raise InvalidData("Output path escapes the reviewed media root") from None
        if "\\" in value or any(ord(c) < 32 for c in value):
            raise InvalidData("Output path is not portable")
        return value

    def completed(self, path):
        relative = self.relative(path)
        if relative == "." or relative.lower().endswith(".part"):
            raise InvalidData("Only a final file can be queued")
        parts = relative.split("/")
        directories = [os.open(self.path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)]
        file = None
        try:
            self.verify_descriptor(directories[0])
            for part in parts[:-1]:
                directories.append(os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                           dir_fd=directories[-1]))
            file = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directories[-1])
            before = os.fstat(file)
            if not stat.S_ISREG(before.st_mode) or before.st_size < 1:
                raise InvalidData("Completed output is not a nonempty regular file")
            os.fsync(file)
            sha = hashlib.sha256()
            while block := os.read(file, 1 << 20):
                sha.update(block)
            after = os.fstat(file)
            current = os.stat(relative, dir_fd=directories[0], follow_symlinks=False)

            def snapshot(value):
                return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns

            if snapshot(before) != snapshot(after) or snapshot(after) != snapshot(current):
                raise InvalidData("Completed output changed during hashing")
            if self.relative(path) != relative:
                raise InvalidData("Completed output moved during hashing")
            for directory in reversed(directories):
                os.fsync(directory)
            self.verify()
            opened, current_root = os.fstat(directories[0]), os.stat(self.path, follow_symlinks=False)
            if (opened.st_dev, opened.st_ino) != (current_root.st_dev, current_root.st_ino):
                raise InvalidData("Output root moved during hashing")
            return relative, before.st_size, sha.hexdigest()
        finally:
            if file is not None:
                os.close(file)
            for directory in reversed(directories):
                os.close(directory)


class Root(Directory):
    """Historical inline directory pins; new profiles use storage bindings."""

    def __init__(self, path, identity, *, filesystem_id=None):
        if (not isinstance(identity, (list, tuple)) or len(identity) != 2
                or any(type(n) is not int or n < 0 for n in identity)):
            raise InvalidData("Invalid reviewed directory identity")
        self.identity = tuple(identity)
        if filesystem_id is not None and (type(filesystem_id) is not int or not 0 < filesystem_id < 2**64):
            raise InvalidData("Invalid reviewed filesystem identity")
        self.filesystem_id = filesystem_id
        super().__init__(path)

    @property
    def binding(self):
        return self.path, self.identity

    @staticmethod
    def probe(path):
        status = Path(path).stat()
        if not stat.S_ISDIR(status.st_mode):
            raise InvalidData("Media root is not a directory")
        return (status.st_dev, status.st_ino)

    def verify_descriptor(self, fd):
        info = os.fstat(fd)
        valid = (info.st_dev, info.st_ino) == self.identity
        if self.filesystem_id is not None:
            valid = info.st_ino == self.identity[1] and self._filesystem_id(fd) == self.filesystem_id
        if not stat.S_ISDIR(info.st_mode) or not valid:
            raise InvalidData("Media root changed or its mount is unavailable")

    @staticmethod
    def _filesystem_id(fd):
        value = getattr(os.fstatvfs(fd), "f_fsid", 0)
        if type(value) is not int or not 0 < value < 2**64:
            raise InvalidData("Filesystem does not expose a persistent identity")
        return value

    @staticmethod
    def review(path):
        """Probe an explicitly reviewed legacy binding; never upgrade a pin."""
        path = Path(path).resolve(strict=True)
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            info = os.fstat(fd)
            return {"path": str(path), "identity": [info.st_dev, info.st_ino],
                    "filesystem_id": Root._filesystem_id(fd)}
        finally:
            os.close(fd)


def inherit_filesystem_identity(value, reviewed):
    """Add current mount proof only to the exact same retained directory pin.

    Frozen scrape rules and their original document stay unchanged. A changed
    UUID, path, inode, or legacy device identity is a different review and cannot
    authorize historical work through this narrow identity-format upgrade.
    """
    if (isinstance(value, dict) and isinstance(reviewed, dict)
            and "filesystem_id" not in value and "filesystem_id" in reviewed
            and all(value.get(key) == reviewed.get(key) for key in ("uuid", "path", "identity"))):
        return {**value, "filesystem_id": reviewed["filesystem_id"]}
    return value


@contextmanager
def destination_lock(directory, root_uuid, relative, check):
    """Shared by host and container using the same physical lock directory."""
    directory = Path(directory).resolve(strict=True)
    bucket = int(hashlib.sha256((root_uuid + "\x00" + relative).encode()).hexdigest()[:8], 16) % 4096
    fd = os.open(directory / f"{bucket:03x}.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        # Do not block indefinitely with a dead source lease behind an old
        # worker's still-running download. The caller can retry its native run.
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            # Different websites can legitimately converge on one output, or
            # merely share a bounded lock bucket. Preserve the source run for
            # retry instead of treating contention as invalid configuration.
            raise PublicationBusy("Destination is held by another download worker") from None
        check()
        yield
    finally:
        os.close(fd)
