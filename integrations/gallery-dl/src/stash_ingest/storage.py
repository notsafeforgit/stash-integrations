"""Local storage bindings, independent of retained scrape instructions.

Profiles name a logical media root and a storage registry. Only this adapter
interprets directory proofs. A registry may have different mount paths on the
host and in containers while retaining its UUID and publication locks.
"""

import argparse
import copy
import os
from pathlib import Path
import sys
import uuid

from .encoding import InvalidData, decode, identifier
from .retained_storage import registry_directory
from .storage_marker import MarkedDirectory, provision

SCHEMA = "stash-storage-bindings-v1"
LIMIT = 1 << 20


def local_path(value, base):
    if not isinstance(value, str) or not value or len(value) > 4096 or any(ord(c) < 32 for c in value):
        raise InvalidData("Storage binding requires a local path")
    path = Path(value).expanduser()
    return (path if path.is_absolute() else base / path).resolve()


def read_document(path):
    with Path(path).open('rb') as source:
        return decode(source.read(LIMIT + 1), LIMIT)


def review_directory(path):
    path = Path(path).resolve(strict=True)
    return {"path": str(path), "token": 'marker-v1:' + provision(path)}


def directory(value, base, path_resolver=local_path):
    if not isinstance(value, dict) or set(value) != {"path", "token"}:
        raise InvalidData("Invalid local storage binding")
    token = value["token"]
    if not isinstance(token, str):
        raise InvalidData("Unknown storage verification token")
    path = path_resolver(value["path"], base)
    if token.startswith('marker-v1:'):
        return MarkedDirectory(path, token.removeprefix('marker-v1:'))
    return registry_directory(path, token)


class StorageBindings:
    def __init__(self, document, filename, *, path_resolver=local_path):
        if (not isinstance(document, dict) or set(document) != {"schema", "uuid", "roots", "locks"}
                or document["schema"] != SCHEMA or not isinstance(document["roots"], dict)
                or not 1 <= len(document["roots"]) <= 256):
            raise InvalidData("Invalid storage registry")
        self.uuid = identifier(document["uuid"])
        for root in document["roots"]:
            identifier(root)
        self.document = copy.deepcopy(document)
        self.filename = Path(filename)
        self.path_resolver = path_resolver

    @classmethod
    def load(cls, reference, base, *, path_resolver=local_path, read_document=read_document):
        if not isinstance(reference, dict) or set(reference) != {"path", "uuid"}:
            raise InvalidData("Storage reference requires a registry path and UUID")
        expected = identifier(reference["uuid"])
        filename = path_resolver(reference["path"], base)
        result = cls(read_document(filename), filename, path_resolver=path_resolver)
        if result.uuid != expected:
            raise InvalidData("Storage registry was replaced")
        return result

    @property
    def reference(self):
        return {"path": str(self.filename), "uuid": self.uuid}

    def bind(self, root_uuid):
        root_uuid = identifier(root_uuid)
        if root_uuid not in self.document["roots"]:
            raise InvalidData("Media root is not configured in this storage registry")
        base = self.filename.parent
        return (directory(self.document["roots"][root_uuid], base, self.path_resolver),
                directory(self.document["locks"], base, self.path_resolver))

    def verification_files(self, root_uuid):
        """Small identity files that must accompany configuration in backups."""
        return [bound.verification_file for bound in self.bind(root_uuid) if isinstance(bound, MarkedDirectory)]


def review(roots, locks, *, registry_uuid=None):
    """Provision directory markers and build configuration; never change a queue."""
    if not 1 <= len(roots) <= 256:
        raise InvalidData("Storage registry needs between one and 256 roots")
    registry_uuid = identifier(registry_uuid or str(uuid.uuid4()))
    for key in roots:
        identifier(key)
    return {"schema": SCHEMA, "uuid": registry_uuid,
            "roots": {key: review_directory(path) for key, path in roots.items()},
            "locks": review_directory(locks)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', nargs=2, action='append', required=True, metavar=('UUID', 'PATH'))
    parser.add_argument('--locks', required=True, help='Shared publication-lock directory')
    parser.add_argument('--uuid', help='Existing registry UUID when explicitly reviewing a replacement binding')
    parser.add_argument('--output', required=True, help='New private registry file; never overwrites an existing file')
    args = parser.parse_args(argv)
    try:
        if len(dict(args.root)) != len(args.root):
            raise InvalidData('A root UUID may only be bound once')
        value = review(dict(args.root), args.locks, registry_uuid=args.uuid)
        from .config_migration import publish_profile
        publish_profile(args.output, value)
        print(f'Storage registry {value["uuid"]} written to {os.path.abspath(args.output)}')
        return 0
    except (InvalidData, OSError) as error:
        print(str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
