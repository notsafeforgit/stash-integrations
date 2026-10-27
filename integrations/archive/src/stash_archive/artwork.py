"""Versioned omission of scene renditions with retained, valid frame selections.

The native database is never edited. Unknown artwork and artwork shared with
any other owner remain required. An omission is recomputed from that exact
snapshot during import/verification, not trusted from an exporter-supplied list.
"""
from datetime import datetime
import math
import uuid

from .storage import InvalidArchive, decode_json, json_bytes, publish_bytes

POLICY = "originals-and-untracked-v1"
ALL = "all"


def policy(value):
    if value not in (ALL, POLICY):
        raise InvalidArchive("Unsupported artwork retention policy")
    return value


def canonical_uuid(value):
    try:
        return str(uuid.UUID(value)) == value
    except (ValueError, TypeError, AttributeError):
        return False


def quoted(value):
    return '"' + value.replace('"', '""') + '"'


def selection(connection):
    """Return checksum -> regeneration records; anything uncertain is retained."""
    tables = {r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    needed = {"scenes", "scene_cover_sources", "scenes_files", "files", "video_files",
              "archive_entities", "files_fingerprints", "performers", "studios", "tags", "groups"}
    if not needed <= tables:
        return {}
    # Retain all other owners, including foreign-key references added by newer
    # schemas. A generated cover shared with a performer/upload is an original.
    protected = set()
    references = {(t, r[3]) for t in tables
                  for r in connection.execute("PRAGMA foreign_key_list(" + quoted(t) + ")")
                  if r[2] == "blobs"}
    references.update({("performers", "image_blob"), ("studios", "image_blob"), ("tags", "image_blob"),
                       ("groups", "front_image_blob"), ("groups", "back_image_blob")})
    for table, column in references - {("scenes", "cover_blob")}:
        protected.update(r[0] for r in connection.execute(
            "SELECT DISTINCT " + quoted(column) + " FROM " + quoted(table) +
            " WHERE " + quoted(column) + " IS NOT NULL"))
    rows = connection.execute("""
        SELECT s.id,s.cover_blob,se.uuid,c.cover_checksum,c.source_file_id,c.source_file_uuid,
               c.at,c.source_fingerprint,f.size,f.mod_time,v.duration,fe.uuid,
               (SELECT fingerprint FROM files_fingerprints WHERE file_id=f.id AND type='md5' LIMIT 1),
               (SELECT fingerprint FROM files_fingerprints WHERE file_id=f.id AND type='oshash' LIMIT 1)
        FROM scenes s JOIN blobs b ON b.checksum=s.cover_blob AND b.blob IS NULL
        LEFT JOIN scene_cover_sources c ON c.scene_id=s.id
        LEFT JOIN scenes_files sf ON sf.scene_id=s.id AND sf.file_id=c.source_file_id
        LEFT JOIN files f ON f.id=sf.file_id
        LEFT JOIN video_files v ON v.file_id=f.id
        LEFT JOIN archive_entities se ON se.scene_id=s.id
        LEFT JOIN archive_entities fe ON fe.file_id=f.id
        ORDER BY s.id
    """)
    generated = {}
    for scene_id, checksum, scene_uuid, saved_checksum, file_id, saved_uuid, at, body, size, modified, duration, file_uuid, md5, oshash in rows:
        try:
            fingerprint = decode_json(body)
            valid = (checksum == saved_checksum and canonical_uuid(scene_uuid)
                     and canonical_uuid(file_uuid) and saved_uuid == file_uuid
                     and type(file_id) is int and file_id > 0
                     and type(at) in (int, float) and math.isfinite(at)
                     and type(duration) in (int, float) and math.isfinite(duration) and 0 <= at < duration
                     and isinstance(fingerprint, dict) and fingerprint.get("version") == 1
                     and type(fingerprint.get("size")) is int and fingerprint["size"] >= 0
                     and fingerprint["size"] == size
                     and type(fingerprint.get("mod_time_ns")) is int
                     and int(datetime.fromisoformat(modified).timestamp()) == fingerprint["mod_time_ns"] // 1_000_000_000
                     and (not fingerprint.get("md5") or fingerprint["md5"] == md5)
                     and (not fingerprint.get("oshash") or fingerprint["oshash"] == oshash))
        except (InvalidArchive, ValueError, TypeError, AttributeError, OverflowError):
            valid = False
        if not valid:
            protected.add(checksum)
            continue
        generated.setdefault(checksum, []).append({"scene_uuid": scene_uuid, "cover_checksum": checksum,
            "source_file_uuid": file_uuid, "at": at, "source_fingerprint": fingerprint})
    return {checksum: records for checksum, records in generated.items() if checksum not in protected}


def descriptor(omitted):
    return {"policy": POLICY, "omitted_blobs": len(omitted),
            "omitted_scenes": sum(len(rows) for rows in omitted.values())}


def validate_descriptor(value):
    if (not isinstance(value, dict) or set(value) != {"policy", "omitted_blobs", "omitted_scenes"}
            or value["policy"] != POLICY
            or any(type(value[k]) is not int or value[k] < 0 for k in ("omitted_blobs", "omitted_scenes"))
            or value["omitted_scenes"] < value["omitted_blobs"]):
        raise InvalidArchive("Invalid generated artwork omission policy")


def verify_inventory(connection, manifest, blobs):
    from .bundle import _blob_rows
    required = set(_blob_rows(connection))
    omitted = selection(connection) if manifest["version"] == 2 else {}
    if manifest["version"] == 2 and descriptor(omitted) != manifest["artwork"]:
        raise InvalidArchive("Artwork omission policy differs from the library snapshot")
    if required - omitted.keys() != blobs:
        raise InvalidArchive("Original artwork inventory does not match the library snapshot")
    return omitted


def restore_instructions(destination, omitted):
    if not omitted:
        return
    # A readable plan is derived from the verified SQLite bytes. It is not an
    # alternate authority for editing the library or trusting missing images.
    path = destination / "generated-covers.jsonl"
    with path.open("xb") as stream:
        import os
        os.chmod(path, 0o600)
        for checksum in sorted(omitted):
            for row in omitted[checksum]:
                stream.write(json_bytes(row))
        stream.flush()
        os.fsync(stream.fileno())
    publish_bytes(destination / "regenerate-covers.graphql", b"""# After restoring media and starting a Stash version supporting coversMissingOnly.
# Only missing covers with saved selections are regenerated; originals are kept.
mutation RestoreGeneratedCovers {
  metadataGenerate(input: {covers: true, coversMissingOnly: true})
}
""")
