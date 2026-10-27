import hashlib
import json
import sqlite3
import unittest
from unittest.mock import patch

from test_bundle import ArchiveTests
from stash_archive.artwork import POLICY, descriptor, selection
from stash_archive.bundle import export_archive, import_archive, iter_artifacts, validate_manifest
from stash_archive.storage import InvalidArchive, json_bytes, object_path
from stash_archive.verification import verify_archive_contents


class ArtworkPolicyTests(ArchiveTests):
    def setUp(self):
        super().setUp()
        self.db.executescript("""
            ALTER TABLE performers ADD COLUMN image_blob TEXT REFERENCES blobs(checksum);
            CREATE TABLE studios(image_blob TEXT REFERENCES blobs(checksum));
            CREATE TABLE tags(image_blob TEXT REFERENCES blobs(checksum));
            CREATE TABLE groups(front_image_blob TEXT REFERENCES blobs(checksum),back_image_blob TEXT REFERENCES blobs(checksum));
            CREATE TABLE scenes(id INTEGER PRIMARY KEY,cover_blob TEXT REFERENCES blobs(checksum));
            CREATE TABLE scene_cover_sources(scene_id INTEGER PRIMARY KEY,cover_checksum TEXT,source_file_id INTEGER,
                source_file_uuid TEXT,at REAL,source_fingerprint BLOB);
            CREATE TABLE scenes_files(scene_id INTEGER,file_id INTEGER);
            CREATE TABLE files(id INTEGER PRIMARY KEY,size INTEGER,mod_time TEXT);
            CREATE TABLE video_files(file_id INTEGER,duration REAL);
            CREATE TABLE files_fingerprints(file_id INTEGER,type TEXT,fingerprint TEXT);
            CREATE TABLE archive_entities(uuid TEXT,scene_id INTEGER,file_id INTEGER);
            INSERT INTO files VALUES(10,123,'2026-10-01T00:00:00+00:00');
            INSERT INTO video_files VALUES(10,60);
            INSERT INTO files_fingerprints VALUES(10,'md5','source-hash');
            INSERT INTO archive_entities VALUES('11111111-1111-4111-8111-111111111111',1,NULL);
            INSERT INTO archive_entities VALUES('22222222-2222-4222-8222-222222222222',NULL,10);
            INSERT INTO scenes_files VALUES(1,10);
        """)
        self.db.execute('INSERT INTO scenes VALUES(1,?)', (self.external_md5,))
        self.fingerprint = {'version': 1, 'size': 123, 'mod_time_ns': 1790812800000000000, 'md5': 'source-hash'}
        self.db.execute('INSERT INTO scene_cover_sources VALUES(1,?,10,?,12.345,?)',
                        (self.external_md5, '22222222-2222-4222-8222-222222222222', json_bytes(self.fingerprint)))
        self.db.commit()

    def selected_export(self, **kwargs):
        return export_archive(self.database, self.output, blob_paths=[self.blobs], reserve=0, artwork_policy=POLICY, **kwargs)

    def test_generated_covers_omitted_database_unchanged_restore_plan_and_deep_verify(self):
        manifest = self.selected_export()
        self.assertEqual(manifest['version'], 2)
        self.assertEqual(manifest['artwork'], {'policy': POLICY, 'omitted_blobs': 1, 'omitted_scenes': 1})
        self.assertFalse(any(e['role'] == 'blob' for e in iter_artifacts(self.output, manifest)))
        restored = self.root / 'restore'
        import_archive(self.output, restored, reserve=0)
        with sqlite3.connect(restored / 'library.sqlite') as db:
            self.assertEqual(db.execute('select cover_blob from scenes').fetchone()[0], self.external_md5)
            self.assertEqual(db.execute('select source_fingerprint from scene_cover_sources').fetchone()[0], json_bytes(self.fingerprint))
        row = json.loads((restored / 'generated-covers.jsonl').read_bytes())
        self.assertEqual(row['at'], 12.345)
        self.assertEqual(row['source_file_uuid'], '22222222-2222-4222-8222-222222222222')
        self.assertIn('coversMissingOnly: true', (restored / 'regenerate-covers.graphql').read_text())
        self.assertEqual(json.loads((restored / 'restore.json').read_bytes())['artwork_regeneration_pending'], 1)
        self.assertTrue(verify_archive_contents(self.output, temp_parent=self.root, reserve=0)['contents_verified'])

    def test_originals_and_shared_unknown_covers_are_retained(self):
        mutations = [
            ("DELETE FROM scene_cover_sources", ()),
            ("UPDATE scene_cover_sources SET cover_checksum='different'", ()),
            ("UPDATE performers SET image_blob=? WHERE id=7", (self.external_md5,)),
            ("INSERT INTO studios VALUES(?)", (self.external_md5,)),
            ("INSERT INTO tags VALUES(?)", (self.external_md5,)),
            ("INSERT INTO groups VALUES(?,NULL)", (self.external_md5,)),
            ("INSERT INTO groups VALUES(NULL,?)", (self.external_md5,)),
            ("INSERT INTO scenes VALUES(2,?)", (self.external_md5,)),
            ("DELETE FROM scenes_files", ()),
            ("UPDATE files SET size=456", ()),
            ("UPDATE files SET mod_time='2026-10-02T00:00:00Z'", ()),
            ("UPDATE files_fingerprints SET fingerprint='new-content'", ()),
            ("UPDATE scene_cover_sources SET at=60", ()),
            ("UPDATE scene_cover_sources SET source_file_uuid='other'", ()),
            ("UPDATE scene_cover_sources SET source_fingerprint=X'00FF'", ()),
        ]
        for sql, args in mutations:
            with self.subTest(sql=sql):
                self.db.execute('SAVEPOINT change')
                self.db.execute(sql, args)
                self.assertEqual(selection(self.db), {})
                self.db.execute('ROLLBACK TO change')
                self.db.execute('RELEASE change')
        self.db.executescript('CREATE TABLE future_owner(image TEXT REFERENCES blobs(checksum));')
        self.db.execute('INSERT INTO future_owner VALUES(?)', (self.external_md5,))
        self.assertEqual(selection(self.db), {})

    def test_unknown_covers_remain_required_and_missing_original_fails(self):
        self.db.execute('DELETE FROM scene_cover_sources')
        self.db.commit()
        blob = self.blobs / self.external_md5[:2] / self.external_md5[2:4] / self.external_md5
        blob.unlink()
        with self.assertRaisesRegex(InvalidArchive, 'Required original artwork'):
            self.selected_export()

    def test_manifest_cannot_lie_about_omissions_or_remove_an_original(self):
        manifest = self.selected_export()
        manifest['artwork']['omitted_blobs'] = 0
        manifest['artwork']['omitted_scenes'] = 0
        (self.output / 'manifest.json').write_bytes(json_bytes(manifest))
        with self.assertRaisesRegex(InvalidArchive, 'omission policy'):
            import_archive(self.output, self.root / 'restore', reserve=0)
        self.assertFalse((self.root / 'restore').exists())
        manifest['version'] = 1
        with self.assertRaises(InvalidArchive):
            validate_manifest(manifest)

    def test_interrupted_pack_keeps_valid_chunks_and_authenticates_reuse(self):
        # A prior full archive supplies an interrupted inventory prefix. Only
        # the blob is reusable without a sealed checkpoint's library hash.
        original = self.export()
        entries = list(iter_artifacts(self.output, original))
        source = self.root / 'partial'
        self.output.rename(source)
        (source / 'manifest.json').unlink()
        with (source / 'artifacts.jsonl').open('ab') as out:
            out.write(b'{"partial":')
        self.db.execute('DELETE FROM scene_cover_sources')
        self.db.commit()
        from stash_archive.bundle import store_file
        calls = []
        def track(*args, **kwargs):
            calls.append(args[1])
            return store_file(*args, **kwargs)
        with patch('stash_archive.bundle.store_file', side_effect=track):
            self.selected_export(reuse_archive=source)
        self.assertEqual(len(calls), 1, 'only the changed library is repacked')
        blob = next(e for e in entries if e['role'] == 'blob')
        old, new = object_path(source, blob['chunks'][0]['sha256']), object_path(self.output, blob['chunks'][0]['sha256'])
        self.assertEqual(old.stat().st_ino, new.stat().st_ino)

    def test_corrupt_reuse_repacked_and_failed_attempt_retained_without_manifest(self):
        from stash_archive.repack import PackedReuse
        manifest = self.export()
        entries = list(iter_artifacts(self.output, manifest))
        blob = next(e for e in entries if e['role'] == 'blob')
        reuse = PackedReuse(self.output)
        object_path(self.output, blob['chunks'][0]['sha256']).write_bytes(b'corrupt')
        self.assertIsNone(reuse.reuse(self.root / 'unused', 'blob', self.external_md5, None, None))
        with self.assertRaises(RuntimeError):
            export_archive(self.database, self.root / 'failed', reserve=0, retain_incomplete=True,
                           progress=lambda _: (_ for _ in ()).throw(RuntimeError('interrupt')))
        self.assertTrue((self.root / 'failed/artifacts.jsonl').exists())
        self.assertFalse((self.root / 'failed/manifest.json').exists())
