"""Application-owned directory identity, independent of mounts and libc."""

import os
from pathlib import Path
import stat
import uuid

from .encoding import InvalidData, identifier
from .filesystem import Directory

NAME = '.stash-storage-id'
PREFIX = b'stash-storage-directory-v1\n'
SIZE = len(PREFIX) + 37


def body(value):
    return PREFIX + identifier(value).encode('ascii') + b'\n'


def read(fd):
    """Read only a bounded regular marker in the already opened directory."""
    marker = os.open(NAME, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
    try:
        before = os.fstat(marker)
        if not stat.S_ISREG(before.st_mode) or before.st_size != SIZE:
            raise InvalidData('Invalid storage directory marker')
        value = os.read(marker, SIZE + 1)
        after = os.fstat(marker)
        if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise InvalidData('Storage directory marker changed while reading')
        try:
            token = value[len(PREFIX):-1].decode('ascii')
            if value != body(token):
                raise InvalidData('Invalid storage directory marker')
        except UnicodeError:
            raise InvalidData('Invalid storage directory marker') from None
        return token
    finally:
        os.close(marker)


def provision(path):
    """Explicit setup only. Atomically create or reuse a valid marker."""
    path = Path(path).resolve(strict=True)
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        try:
            return read(fd)
        except FileNotFoundError:
            pass
        token = str(uuid.uuid4())
        temporary = NAME + '.' + token + '.tmp'
        created = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644, dir_fd=fd)
        try:
            with os.fdopen(created, 'wb') as output:
                output.write(body(token))
                output.flush()
                os.fsync(output.fileno())
            try:
                os.link(temporary, NAME, src_dir_fd=fd, dst_dir_fd=fd, follow_symlinks=False)
            except FileExistsError:
                # Concurrent explicit setup must converge on the winner's ID.
                pass
            os.fsync(fd)
        finally:
            os.unlink(temporary, dir_fd=fd)
            os.fsync(fd)
        return read(fd)
    finally:
        os.close(fd)


class MarkedDirectory(Directory):
    def __init__(self, path, token):
        self.token = identifier(token)
        super().__init__(path)

    @property
    def binding(self):
        return self.path, self.token

    @property
    def verification_file(self):
        return self.path / NAME, body(self.token)

    def verify_descriptor(self, fd):
        try:
            if read(fd) != self.token:
                raise InvalidData('Storage directory marker does not match its binding')
        except OSError:
            raise InvalidData('Storage directory marker is unavailable') from None
