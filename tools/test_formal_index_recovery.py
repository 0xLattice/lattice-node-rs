#!/usr/bin/env python3
"""Provenance regression tests; only disposable source trees are modified."""
import importlib.util
import pathlib
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('bridge', pathlib.Path(__file__).with_name('formal-index-recovery-check.py'))
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)


class ProvenanceTests(unittest.TestCase):
    def test_snapshot_rejects_changed_added_and_removed_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory).resolve() / 'node'
            (root / 'src').mkdir(parents=True)
            source = root / 'src/lib.rs'
            source.write_text('original')
            manifest = root / 'Cargo.toml'
            manifest.write_text('[package]')
            tool = root / 'runner.py'
            tool.write_text('runner')
            with patch.object(bridge, 'ROOT', root), patch.object(bridge, '__file__', str(tool)):
                before = bridge.source_snapshot([root])
                bridge.unchanged(before, bridge.source_snapshot([root]), 'sources')
                source.write_text('changed')
                with self.assertRaisesRegex(RuntimeError, 'changed during validation'):
                    bridge.unchanged(before, bridge.source_snapshot([root]), 'sources')
                source.write_text('original')
                extra = root / 'src/new.rs'
                extra.write_text('new')
                with self.assertRaises(RuntimeError):
                    bridge.unchanged(before, bridge.source_snapshot([root]), 'sources')
                extra.unlink()
                manifest.unlink()
                with self.assertRaises(RuntimeError):
                    bridge.unchanged(before, bridge.source_snapshot([root]), 'sources')

    def test_formal_snapshot_change_is_not_relabeled(self):
        before = ('pin', b'', b'model', b'launcher')
        for changed in [('other-pin', b'', b'model', b'launcher'),
                        ('pin', b'M model', b'model', b'launcher'),
                        ('pin', b'', b'changed-model', b'launcher'),
                        ('pin', b'', b'model', b'changed-launcher')]:
            with self.assertRaises(RuntimeError):
                bridge.unchanged(before, changed, 'formal inputs')


if __name__ == '__main__':
    unittest.main()
