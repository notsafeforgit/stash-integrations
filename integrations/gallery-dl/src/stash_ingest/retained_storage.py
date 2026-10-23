"""Read inline bindings in already admitted native v1 worker profiles.

New profiles use the local storage registry. Keep these checks unchanged when
executing a v1 installation that has not explicitly selected a registry.
"""

import re

from .encoding import InvalidData, identifier
from .filesystem import Root, inherit_filesystem_identity


def registry_directory(path, token):
    """Read the filesystem token emitted by 0.6.0 without changing its meaning."""
    if not re.fullmatch(r"fs-v1:[1-9a-f][0-9a-f]{0,15}:(?:0|[1-9a-f][0-9a-f]{0,15})", token):
        raise InvalidData("Unknown storage verification token")
    _, filesystem, inode = token.split(":")
    return Root(path, (0, int(inode, 16)), filesystem_id=int(filesystem, 16))


def inline_root(value, base, *, media, path_resolver):
    fields = {"path", "identity", "uuid"} if media else {"path", "identity"}
    if (not isinstance(value, dict) or not fields <= set(value) or set(value) - fields - {"filesystem_id"}
            or not isinstance(value["identity"], list) or len(value["identity"]) != 2
            or any(type(n) is not int or n < 0 for n in value["identity"])):
        raise InvalidData("Worker directories require reviewed device/inode identities")
    if "filesystem_id" in value and value["filesystem_id"] is None:
        raise InvalidData("Invalid reviewed filesystem identity")
    return (identifier(value["uuid"]) if media else None), Root(
        path_resolver(value["path"], base), value["identity"], filesystem_id=value.get("filesystem_id"))


def inline_bindings(document, base, *, reviewed=None, path_resolver):
    reviewed = reviewed or {}
    root_uuid, root = inline_root(inherit_filesystem_identity(document["root"], reviewed.get("root")),
                                  base, media=True, path_resolver=path_resolver)
    _, locks = inline_root(inherit_filesystem_identity(document["locks"], reviewed.get("locks")),
                           base, media=False, path_resolver=path_resolver)
    return root_uuid, root, locks
