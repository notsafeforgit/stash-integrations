"""Reuse authenticated packed chunks from an interrupted, unpublished export."""
import hashlib
import os
from pathlib import Path

from .storage import (CHUNK_SIZE, HEX, MAX_MANIFEST, InvalidArchive, check_descriptor,
                      decode_json, object_path, open_regular, read_chunk, sync_directory)


class PackedReuse:
    def __init__(self, source):
        self.source = Path(source) if source is not None else None
        self.entries = {}
        if self.source is None:
            return
        from .artwork_pins import directory
        directory(self.source, private=True)
        with open_regular(self.source / "artifacts.jsonl") as stream:
            while body := stream.readline(MAX_MANIFEST + 1):
                # A crash may leave only the last descriptor incomplete.
                if not body.endswith(b"\n") or len(body) > MAX_MANIFEST:
                    break
                try:
                    entry = decode_json(body)
                    if (not isinstance(entry, dict) or entry.get("role") not in ("library", "blob")
                            or not isinstance(entry.get("name"), str)
                            or type(entry.get("size")) is not int or entry["size"] < 0
                            or not isinstance(entry.get("sha256"), str) or not HEX.fullmatch(entry["sha256"])
                            or not isinstance(entry.get("chunks"), list)):
                        continue
                    for chunk in entry["chunks"]:
                        check_descriptor(chunk, chunk_size=CHUNK_SIZE)
                    if sum(c["size"] for c in entry["chunks"]) != entry["size"]:
                        continue
                except (InvalidArchive, ValueError, TypeError, KeyError):
                    continue
                self.entries[(entry["role"], entry["name"])] = entry

    def reuse(self, destination, role, name, expected, metadata):
        entry = self.entries.get((role, name))
        # Library reuse requires the sealed checkpoint's exact digest. Artwork
        # is authenticated independently by its content-addressed MD5 below.
        if entry is None or (role == "library" and expected is None):
            return None
        if expected is not None and (entry["sha256"], entry["size"]) != expected:
            return None
        if entry.get("sqlite") != metadata:
            return None
        sha, md5, size = hashlib.sha256(), hashlib.md5(usedforsecurity=False), 0
        try:
            for chunk in entry["chunks"]:
                body = read_chunk(self.source, chunk)
                sha.update(body)
                md5.update(body)
                size += len(body)
        except (InvalidArchive, OSError):
            return None
        if sha.hexdigest() != entry["sha256"] or size != entry["size"] or role == "blob" and md5.hexdigest() != name:
            return None
        # Link only checked immutable objects. No per-object compression/fsync
        # is repeated. Existing output must contain identical encoded bytes.
        directories = set()
        for chunk in entry["chunks"]:
            source, target = object_path(self.source, chunk["sha256"]), object_path(destination, chunk["sha256"])
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            try:
                os.link(source, target, follow_symlinks=False)
            except FileExistsError:
                with open_regular(target) as stream:
                    if hashlib.file_digest(stream, "sha256").hexdigest() != chunk["sha256"]:
                        raise InvalidArchive("Reused packed object conflicts with existing bytes")
            directories.add(target.parent)
        for directory in directories:
            sync_directory(directory)
        sync_directory(Path(destination) / "objects")
        return {k: entry[k] for k in ("sha256", "size", "chunks")}
