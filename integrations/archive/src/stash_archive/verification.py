"""Bind native and producer checks to streamed contents or an isolated restore.

The executable is an explicit, trusted local input, never selected by an archive.
No server, migration, worker or filesystem recovery is started by this protocol.
This still does not establish a coordinated live backup/media boundary.
"""

from contextlib import closing
import hashlib
import math
import os
from pathlib import Path
import selectors
import stat
import subprocess
import tempfile
import time

from .bundle import (CAPTURE_VERIFICATION, FORMAT, SQLITE_ROLES, _blob_rows, _target, connect_readonly,
                     database_metadata, import_archive, iter_artifacts, sqlite_evidence_binding, validate_manifest)
from .storage import (MAX_MANIFEST, InvalidArchive, RESERVE_BYTES, decode_json, json_bytes,
                      load_manifest, open_regular, require_space, write_artifact)

MAX_OUTPUT = 64 << 10
DEFAULT_TIMEOUT = 3600


def load_capture_verification(source, producer_origin):
    """Read the exporter-owned capture evidence, without repeating its audits.

    Historical archives may lack this record. A present but invalid record must
    fail, rather than silently falling back. The publisher also validates its
    complete component binding and verifies remote object checksums before commit.
    """
    from .receipts import origin

    expected_origin = origin(producer_origin)
    try:
        with open_regular(Path(source) / CAPTURE_VERIFICATION) as stream:
            body = stream.read(MAX_MANIFEST + 1)
    except FileNotFoundError:
        return None
    if len(body) > MAX_MANIFEST:
        raise InvalidArchive("Capture verification exceeds its size limit")
    proof = decode_json(body)
    manifest = validate_manifest(load_manifest(source))
    if (not isinstance(proof, dict) or proof.get("verification_method") not in ("captured-contents", "captured-snapshot")
            or proof.get("uuid") != manifest["uuid"]
            or proof.get("manifest_sha256") != hashlib.sha256(json_bytes(manifest)).hexdigest()
            or not isinstance(proof.get("ingestion_receipts"), dict)
            or proof["ingestion_receipts"].get("origin") != expected_origin):
        raise InvalidArchive("Capture verification does not match this archive and producer origin")
    validate_producer_bindings(source, manifest, proof)
    return proof


def validate_producer_bindings(source, manifest, proof):
    """A lightweight producer report requires the exact sealed capture barrier."""
    from .receipts import FORMAT as NATIVE_FORMAT, origin
    from .component_stage import FORMAT as STAGE_FORMAT, MANIFEST, MAX_MANIFEST as STAGE_LIMIT
    from .checkpoint_release import archived_boundary
    from .filesystem_boundary import canonical_uuid
    from .server_checkpoint import ServerCheckpoint
    import io

    report = proof.get("ingestion_receipts", {})
    if report.get("format") != NATIVE_FORMAT + ".ingestion-producer-bindings":
        if report.get("receipts_audited") is False:
            raise InvalidArchive("Unknown lightweight producer validation proof")
        return
    checkpoint = report.get("checkpoint_uuid")
    if (proof.get("verification_method") != "captured-snapshot"
            or proof.get("database_audited") is not False
            or report.get("receipts_audited") is not False
            or report.get("registered_producers_complete") is not True
            or type(report.get("version")) is not int or report["version"] != 1
            or report.get("coverage") != "coordinated-producer-snapshots"
            or not canonical_uuid(checkpoint)):
        raise InvalidArchive("Producer identities cannot claim a historical receipt audit")
    expected_origin = origin(report.get("origin"))
    selected = {(entry["role"], entry["name"]): entry for entry in iter_artifacts(source, manifest)
                if entry["role"] == "producer_outbox" or (entry["role"], entry["name"]) == MANIFEST}
    descriptor = selected.get(MANIFEST)
    if descriptor is None or descriptor["size"] > STAGE_LIMIT:
        raise InvalidArchive("Producer identities require the archived coordinated component stage")
    output = io.BytesIO()
    write_artifact(source, descriptor, output)
    stage = decode_json(output.getvalue())
    if not isinstance(stage, dict):
        raise InvalidArchive("Invalid archived producer component stage")
    components = stage.get("components")
    if (stage.get("format") != STAGE_FORMAT or type(stage.get("version")) is not int or stage["version"] != 1
            or stage.get("uuid") != checkpoint or not isinstance(components, list)):
        raise InvalidArchive("Producer identity stage differs from its checkpoint")
    binding = {"format": STAGE_FORMAT, "version": 1, "uuid": checkpoint,
               "sha256": descriptor["sha256"], "bytes": descriptor["size"], "count": len(components)}
    class ArchivedCheckpoint:
        boundary = True
        validate = ServerCheckpoint.validate
        request_id = checkpoint
    boundary = archived_boundary(source, ArchivedCheckpoint())
    if report.get("external_components") != binding or boundary.get("details", {}).get("components") != binding:
        raise InvalidArchive("Producer identities differ from the sealed component boundary")
    producers, staged = set(), set()
    for component in components:
        if not isinstance(component, dict):
            raise InvalidArchive("Invalid archived staged component")
        if component.get("role") != "producer_outbox":
            continue
        key = (component["role"], component["name"])
        entry = selected.get(key)
        if (key in staged or entry is None or entry["sha256"] != component["sha256"]
                or entry["size"] != component["bytes"] or entry.get("sqlite") != component.get("sqlite")):
            raise InvalidArchive("Producer identity proof lost an exact staged outbox")
        staged.add(key)
        metadata = entry["sqlite"]
        producer = metadata.get("producer_uuid")
        if (not canonical_uuid(producer) or producer in producers
                or origin(metadata.get("endpoint")) != expected_origin):
            raise InvalidArchive("Producer identity proof has a foreign or duplicate producer")
        producers.add(producer)
    if (staged != {key for key in selected if key[0] == "producer_outbox"}
            or report.get("producers") != [{"producer_uuid": value} for value in sorted(producers)]):
        raise InvalidArchive("Producer identity proof does not cover every archived outbox")


def validator_output(executable, database, timeout, *, lock_fd=None):
    """Bound both output streams and runtime, including a child that stops talking."""
    outputs = {"stdout": bytearray(), "stderr": bytearray()}
    if lock_fd is not None:
        if type(lock_fd) is not int or lock_fd < 0 or not stat.S_ISREG(os.fstat(lock_fd).st_mode):
            raise InvalidArchive("The host verification lock must be an open regular file")
    deadline = time.monotonic() + timeout
    with subprocess.Popen([str(executable), "--verify-native-snapshot", str(database)],
                          stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, cwd=database.parent,
                          pass_fds=() if lock_fd is None else (lock_fd,)) as process:
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ, "stdout")
                selector.register(process.stderr, selectors.EVENT_READ, "stderr")
                while selector.get_map():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise InvalidArchive("Native snapshot validation timed out")
                    for key, _ in selector.select(min(remaining, 0.25)):
                        data = os.read(key.fileobj.fileno(), 8192)
                        if not data:
                            selector.unregister(key.fileobj)
                            continue
                        output = outputs[key.data]
                        if len(output) + len(data) > MAX_OUTPUT:
                            raise InvalidArchive("Native snapshot validator exceeded its output limit")
                        output.extend(data)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise InvalidArchive("Native snapshot validation timed out")
            try:
                process.wait(timeout=remaining)
            except subprocess.TimeoutExpired as error:
                raise InvalidArchive("Native snapshot validation timed out") from error
            if process.returncode != 0:
                detail = outputs["stderr"].decode("utf-8", errors="replace").strip()
                raise InvalidArchive(f"Native snapshot validation failed (exit {process.returncode}): {detail[:4096]}")
        except BaseException:
            if process.poll() is None:
                process.kill()
            process.wait()
            raise
    return bytes(outputs["stdout"])


def validator_path(executable, timeout):
    """Reject invalid local options before allocating a potentially large restore."""
    if not isinstance(executable, (str, os.PathLike)) or not os.fspath(executable):
        raise InvalidArchive("Native validation requires an explicit local executable path")
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise InvalidArchive("Native validation timeout must be positive and finite")
    executable = Path(executable).absolute()
    if not stat.S_ISREG(executable.stat().st_mode) or not os.access(executable, os.X_OK):
        raise InvalidArchive("Native validator must be an executable regular file")
    return executable


def verify_native_snapshot(restored, entry, executable, *, timeout=DEFAULT_TIMEOUT, lock_fd=None):
    """Require the matching binary's exact v1 report for the packed library bytes."""
    executable = validator_path(executable, timeout)
    database = Path(restored).absolute() / "library.sqlite"
    body = validator_output(executable, database, timeout, lock_fd=lock_fd)
    report = decode_json(body)
    fields = {"format", "version", "lineage", "schema_version", "sha256", "bytes",
              "database_verified", "pending_file_deletions", "filesystem_recovery_verified"}
    if not isinstance(report, dict) or set(report) != fields:
        raise InvalidArchive("Unsupported native snapshot verification report")
    if (report["format"] != FORMAT + ".snapshot-verification"
            or type(report["version"]) is not int or report["version"] != 1
            or report["lineage"] != FORMAT
            or type(report["schema_version"]) is not int
            or report["schema_version"] != entry["sqlite"]["schema"]
            or report["sha256"] != entry["sha256"]
            or type(report["bytes"]) is not int or report["bytes"] != entry["size"]
            or report["database_verified"] is not True
            or report["filesystem_recovery_verified"] is not False
            or type(report["pending_file_deletions"]) is not int
            or report["pending_file_deletions"] < 0):
        raise InvalidArchive("Native snapshot verification does not match the archive library")
    return report


class _ArtworkDigest:
    """A write_artifact sink that checks artwork without creating a file."""

    def __init__(self):
        self.digest = hashlib.md5(usedforsecurity=False)

    def write(self, data):
        self.digest.update(data)


def _stream_archive(source, destination, reserve):
    manifest = validate_manifest(load_manifest(source))
    # Exhaust the inventory before allocating scratch files or trusting sizes.
    # Only SQLite components need a filesystem representation for their checks.
    database_bytes = sum(entry["size"] for entry in iter_artifacts(source, manifest)
                         if entry["role"] in SQLITE_ROLES)
    require_space(destination.parent, database_bytes, reserve)
    destination.mkdir(mode=0o700)
    blobs = set()
    for entry in iter_artifacts(source, manifest):
        role = entry["role"]
        if role in SQLITE_ROLES:
            path = _target(destination, entry)
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "wb") as output:
                write_artifact(source, entry, output,
                               check_space=lambda size: require_space(destination, size, reserve))
            # These private files exist only for validation and are discarded
            # before publication. There is deliberately no durable restore
            # receipt or per-file/directory fsync for this disposable workspace.
            with closing(connect_readonly(path)) as connection:
                if database_metadata(connection, role) != entry["sqlite"]:
                    raise InvalidArchive("Verified database identity differs from the manifest")
        elif role == "blob":
            sink = _ArtworkDigest()
            write_artifact(source, entry, sink)
            if sink.digest.hexdigest() != entry["name"]:
                raise InvalidArchive("Original artwork checksum mismatch")
            blobs.add(entry["name"])
        else:
            write_artifact(source, entry)
    with closing(connect_readonly(destination / "library.sqlite")) as connection:
        from .artwork import verify_inventory
        verify_inventory(connection, manifest, blobs)
    return manifest


def verify_archive_contents(source, *, native_validator=None, producer_origin=None,
                            timeout=DEFAULT_TIMEOUT, temp_parent=None, reserve=RESERVE_BYTES, lock_fd=None):
    """Check every byte and database, materializing only temporary SQLite files.

    The host publisher uses this without native_validator. An explicitly supplied
    validator adds the full application audit, which is not needed to publish a
    backup. Use verify_archive_proofs for a complete isolated restore drill.
    """
    return _verify_archive(source, restore=False, native_validator=native_validator,
                           producer_origin=producer_origin, timeout=timeout,
                           temp_parent=temp_parent, reserve=reserve, lock_fd=lock_fd)


def verify_archive_proofs(source, *, native_validator=None, producer_origin=None,
                         timeout=DEFAULT_TIMEOUT, temp_parent=None, reserve=RESERVE_BYTES, lock_fd=None):
    """Return success only after every requested check passes on the same restore."""
    return _verify_archive(source, restore=True, native_validator=native_validator,
                           producer_origin=producer_origin, timeout=timeout,
                           temp_parent=temp_parent, reserve=reserve, lock_fd=lock_fd)


def _verify_archive(source, *, restore, native_validator, producer_origin,
                    timeout, temp_parent, reserve, lock_fd):
    from .receipts import origin, verify_restored_receipts

    if producer_origin is not None:
        producer_origin = origin(producer_origin)
    if native_validator is not None:
        native_validator = validator_path(native_validator, timeout)
    with tempfile.TemporaryDirectory(prefix="stash-archive-verify-", dir=temp_parent) as temp:
        restored = Path(temp) / "restored"
        if restore:
            manifest = import_archive(source, restored, reserve=reserve)
        else:
            manifest = _stream_archive(source, restored, reserve)
        # Consume the complete iterator, so the inventory digest and unique
        # library requirement are checked before any additional proof is used.
        databases = [entry for entry in iter_artifacts(source, manifest) if entry["role"] in SQLITE_ROLES]
        library, = [entry for entry in databases if entry["role"] == "library"]
        manifest_sha256 = hashlib.sha256(json_bytes(manifest)).hexdigest()
        result = {"uuid": manifest["uuid"], "manifest_sha256": manifest_sha256,
                  "coverage": manifest["coverage"], "contents_verified": True, "database_audited": True,
                  "verification_method": "isolated-restore" if restore else "streamed-contents"}
        # Both paths checked the reconstructed SQLite bytes before reaching here.
        # Keep this narrower evidence distinct from the optional native audit.
        result["sqlite_snapshots"] = sqlite_evidence_binding(manifest, databases)
        if native_validator is not None:
            proof = verify_native_snapshot(restored, library, native_validator, timeout=timeout, lock_fd=lock_fd)
            result["native_snapshot"] = dict(proof, archive_uuid=manifest["uuid"],
                                             manifest_sha256=manifest_sha256,
                                             component={"role": "library", "name": library["name"]})
        if producer_origin is not None:
            result["ingestion_receipts"] = verify_restored_receipts(source, restored, manifest, producer_origin)
        return result
