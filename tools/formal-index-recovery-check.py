#!/usr/bin/env python3
"""Finite authenticated child-index observations through unchanged Recovery.tla.
Per-level graph refinement is distinct from production event-footprint assertions.
"""
import copy
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
FORMAL = ROOT.parent / 'lattice-formal'
PIN = 'a9f4c2879b6565ae11c17d35efe14475c5830978'
TEST = 'storage::turn_tests::one_child_index_result_commits_unblocked_headers_proofs_and_cursor_together'
FIELDS = ['beforeRoot', 'beforeChild', 'liveChild', 'recoveredRoot', 'recoveredChild']


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def tla(value):
    if isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list):
        return '<<' + ','.join(map(tla, value)) + '>>'
    if isinstance(value, dict):
        return '[' + ','.join(f'{k} |-> {tla(v)}' for k, v in value.items()) + ']'
    raise TypeError(value)


def aset(values):
    return '{' + ','.join(tla(v) for v in sorted(values, key=lambda x: json.dumps(x, sort_keys=True))) + '}'


def fn(values):
    return '(' + ' @@ '.join(f'{tla(k)} :> {tla(v)}' for k, v in sorted(values.items())) + ')' if values else '[x \\in {} |-> 0]'


def observations(trace):
    return [case[field] for case in trace['rows'] for field in FIELDS if case[field] is not None]


def project(observation):
    """Encode facts only; do not reimplement graph reduction or fork choice.

    This fixture has exactly one block and one securing work fact per batch,
    plus optional validation. Refuse broader shapes rather than silently split
    a production atomic batch into a different model event.
    """
    result = []
    for batch in observation['batches']:
        facts = {}
        for fact in batch['facts']:
            require(len(fact) == 1, 'unsupported fact shape')
            kind, wrapped = next(iter(fact.items()))
            require(kind not in facts, 'fixture requires one fact of each kind per batch')
            facts[kind] = wrapped['_0']
        require(set(facts) in ({'block', 'work'}, {'block', 'work', 'validation'}), 'unsupported batch shape')
        block, work = facts['block'], facts['work']
        require(block['blockHash'] == work['blockHash'], 'work belongs to another block')
        if 'validation' in facts:
            require(facts['validation']['blockHash'] == block['blockHash'], 'validation belongs to another block')
        result.append({'k': 'block', 'b': block['blockHash'], 'm': str(block['timestamp']),
                       'g': work['contribution']['id'], 'q': int(work['contribution']['work'], 16),
                       'v': 'validation' in facts})
    return result


def grouping(trace):
    """Production assertions, NOT an operator from the single-chain model."""
    require({r['failure'] for r in trace['rows']} == {'none', 'second_header', 'second_proof', 'cursor'}, 'missing failure scenarios')
    for case in trace['rows']:
        require(case['committed'] == (case['failure'] == 'none'), 'wrong event outcome')
        empty = {'batches': 0, 'headers': [False, False], 'proofs': 0, 'cursor': 0}
        full = {'batches': 2, 'headers': [True, True], 'proofs': 2, 'cursor': 2}
        require(case['beforeGroup'] == empty, 'fixture already has child facts')
        require(case['afterGroup'] == (full if case['committed'] else empty), 'partial event footprint')
        require(case['recoveredGroup'] == case['afterGroup'], 'reopen changed event footprint')
        require(case['beforeRoot'] == case['recoveredRoot'], 'index event changed parent')
        if not case['committed']:
            require(case['beforeChild'] == case['recoveredChild'], 'failed child event survived reopen')


def model(trace):
    rows = observations(trace)
    blocks, grinds, parents = set(), set(), {}
    logs = [project(row) for row in rows]
    batches = {json.dumps(x, sort_keys=True): x for log in logs for x in log}
    for row, log in zip(rows, logs):
        for batch, x in zip(row['batches'], log):
            block = next(f['block']['_0'] for f in batch['facts'] if 'block' in f)
            cid = block['blockHash']
            parent = block.get('parentBlockHash', 'none')
            require(cid not in parents or parents[cid] == parent, 'conflicting fixture parent')
            parents[cid] = parent
            blocks.add(cid)
            grinds.add(x['g'])
    rank = {cid: index for index, cid in enumerate(sorted(blocks))}
    records = []
    for row, log in zip(rows, logs):
        nodes = row['nodes']
        meta = {b: str(nodes[b]['timestamp']) if b in nodes else 'none' for b in blocks}
        loc, qty = {g: 'none' for g in grinds}, {g: 0 for g in grinds}
        for b, node in nodes.items():
            require(b in blocks, 'unexpected observed block')
            for g, amount in node['contributions'].items():
                require(g in grinds and loc[g] == 'none', 'unexpected or multiply located grind')
                loc[g], qty[g] = b, int(amount, 16)
        graph = ('[held |-> ' + aset(nodes) + ',meta |-> ' + fn(meta) + ',loc |-> ' + fn(loc)
                 + ',qty |-> ' + fn(qty) + ',val |-> ' + aset(b for b in nodes if nodes[b]['validated'])
                 + ',exc |-> ' + aset(b for b in nodes if nodes[b]['excluded']) + ']')
        weights = fn({b: int(n['weight'], 16) for b, n in nodes.items()})
        cumulative = fn({b: int(n['cumulative'], 16) for b, n in nodes.items()})
        tip = row['tip'] if row['tip'] in blocks else 'none'
        require(row['tip'] in blocks or not nodes, 'unknown nonempty tip')
        records.append('[log |-> ' + tla(log) + ',graph |-> ' + graph + ',tip |-> ' + tla(tip)
                       + ',weights |-> ' + weights + ',cumulative |-> ' + cumulative
                       + ',revision |-> ' + str(row['revision']) + ',floor |-> ' + str(row['floor']) + ']')
    genesis = next(x for x in batches.values() if x['v'] and parents[x['b']] == 'none')
    source = r'''---- MODULE MCIndexRecovery ----
EXTENDS Recovery, TLC
VARIABLE i
TBlocks == @BLOCKS@
TParent == @PARENTS@
TGrinds == @GRINDS@
TRank == @RANK@
TBatches == @BATCHES@
TGenesis == @GENESIS@
TRebuild == [x \in TBatches |-> x]
Rows == <<@ROWS@>>
ProductionInit == Init /\ i = 1
ProductionNext == \/ /\ i < Len(Rows) /\ i' = i + 1 /\ UNCHANGED vars
                  \/ /\ i = Len(Rows) /\ UNCHANGED <<vars,i>>
GraphMatches == Rows[i].graph = Fold(EmptyG, Rows[i].log)
TipMatches == Rows[i].tip = CanonicalTip(Fold(EmptyG, Rows[i].log))
WeightsMatch == \A b \in Rows[i].graph.held :
 Rows[i].weights[b] = SumQty(Fold(EmptyG, Rows[i].log), {g \in Grinds : Fold(EmptyG, Rows[i].log).loc[g] = b})
CumulativeMatches == \A b \in Rows[i].graph.held :
 Rows[i].cumulative[b] = TrueCumWork(Fold(EmptyG, Rows[i].log), b)
RevisionFloorMatches == Rows[i].revision >= Rows[i].floor
====
'''
    for key, value in {'BLOCKS': aset(blocks), 'PARENTS': fn(parents), 'GRINDS': aset(grinds),
                       'RANK': fn(rank), 'BATCHES': aset(batches.values()), 'GENESIS': tla(genesis),
                       'ROWS': ',\n'.join(records)}.items():
        source = source.replace('@' + key + '@', value)
    return source, len(rows)


def check(trace, expected=None):
    source, count = model(trace)
    cfg = '''INIT ProductionInit
NEXT ProductionNext
CONSTANTS
 None = "none"
 Blocks <- TBlocks
 Parent <- TParent
 CidRank <- TRank
 Grinds <- TGrinds
 Batches <- TBatches
 Genesis <- TGenesis
 Rebuild <- TRebuild
 MaxLog = 100
 MaxCrashes = 2
 FloorPersisted = TRUE
 SortedReplay = TRUE
INVARIANTS GraphMatches TipMatches WeightsMatch CumulativeMatches RevisionFloorMatches
'''
    with tempfile.TemporaryDirectory(prefix='lattice-index-tlc-') as directory:
        work = pathlib.Path(directory)
        shutil.copy(FORMAL / 'specs/Recovery.tla', work)
        (work / 'MCIndexRecovery.tla').write_text(source)
        (work / 'MCIndexRecovery_Run.cfg').write_text(cfg)
        env = dict(os.environ)
        if 'JAVA' not in env and pathlib.Path('/opt/homebrew/opt/openjdk/bin/java').exists():
            env['JAVA'] = '/opt/homebrew/opt/openjdk/bin/java'
        result = subprocess.run([str(FORMAL / 'scripts/tlc.sh'), 'MCIndexRecovery_Run.cfg', str(work)],
                                env=env, capture_output=True, text=True, timeout=120)
        output = result.stdout + result.stderr
        if expected:
            require(f'Invariant {expected} is violated' in output, 'negative control failed: ' + output)
        else:
            require(result.returncode == 0 and 'No error has been found' in output, output)
    return count


def formal_snapshot():
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=FORMAL, text=True).strip()
    dirty = subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=FORMAL)
    require(revision == PIN, 'formal pin mismatch')
    require(not dirty, 'modified formal sources')
    return (revision, dirty, (FORMAL / 'specs/Recovery.tla').read_bytes(),
            (FORMAL / 'scripts/tlc.sh').read_bytes())


def source_snapshot(package_roots):
    paths = {pathlib.Path(__file__).resolve()}
    for root in package_roots:
        # Include every byte under local Rust source/test/fixture directories,
        # plus manifests, lockfiles and build scripts. Registry/toolchain inputs
        # are outside this deliberately named local-source coverage.
        for name in ['src', 'tests', 'examples']:
            directory = root / name
            if directory.is_dir():
                paths.update(path for path in directory.rglob('*') if path.is_file())
        paths.update(path for path in [root / 'build.rs', root / 'rust-toolchain.toml', root / 'rust-toolchain'] if path.is_file())
        ancestor = root
        while ancestor != ROOT.parent:
            require(ancestor.is_relative_to(ROOT.parent), 'local package outside sibling layout')
            paths.update(path for path in [ancestor / 'Cargo.toml', ancestor / 'Cargo.lock'] if path.is_file())
            ancestor = ancestor.parent
    return {str(path.relative_to(ROOT.parent)): path.read_bytes() for path in sorted(paths)}


def unchanged(before, after, label):
    require(before == after, label + ' changed during validation; discard run and retry')


def main():
    formal_before = formal_snapshot()
    rustc = subprocess.check_output(['rustc', '-vV'], text=True)
    host = next(line.removeprefix('host: ') for line in rustc.splitlines() if line.startswith('host: '))
    metadata = json.loads(subprocess.check_output(['cargo', 'metadata', '--offline', '--locked', '--format-version=1', '--filter-platform', host], cwd=ROOT))
    package_roots = sorted({pathlib.Path(p['manifest_path']).parent for p in metadata['packages'] if p['source'] is None})
    sources_before = source_snapshot(package_roots)
    build = subprocess.run(['cargo', 'test', '--offline', '--locked', '--lib', '--no-run', '--message-format=json'],
                           cwd=ROOT, capture_output=True, text=True, check=True)
    unchanged(sources_before, source_snapshot(package_roots), 'local sources')
    unchanged(formal_before, formal_snapshot(), 'formal inputs')
    artifacts = [json.loads(line) for line in build.stdout.splitlines() if line.startswith('{')]
    executables = [a['executable'] for a in artifacts if a.get('reason') == 'compiler-artifact'
                   and a.get('executable') and a.get('profile', {}).get('test')
                   and a.get('target', {}).get('name') == 'lattice_node']
    require(len(executables) == 1, 'expected one production library test artifact')
    executable = pathlib.Path(executables[0])
    executable_hash = hashlib.sha256(executable.read_bytes()).hexdigest()
    with tempfile.TemporaryDirectory(prefix='lattice-index-trace-') as directory:
        path = pathlib.Path(directory) / 'trace.json'
        subprocess.run([str(executable), TEST, '--exact'],
                       cwd=ROOT, env={**os.environ, 'LATTICE_FORMAL_INDEX_TRACE': str(path)}, check=True)
        trace = json.loads(path.read_text())
    require(hashlib.sha256(executable.read_bytes()).hexdigest() == executable_hash, 'test artifact changed during run')
    unchanged(sources_before, source_snapshot(package_roots), 'local sources')
    unchanged(formal_before, formal_snapshot(), 'formal inputs')
    grouping(trace)
    count = check(trace)
    negatives = []
    for label, invariant, corrupt in [
        ('wrong recovered weight', 'WeightsMatch', lambda r: r['nodes'][next(iter(r['nodes']))].update(weight='ff')),
        ('wrong subtree work', 'CumulativeMatches', lambda r: r['nodes'][next(iter(r['nodes']))].update(cumulative='ff')),
        ('lost durable fact', 'GraphMatches', lambda r: r['batches'].pop()),
        ('wrong canonical tip', 'TipMatches', lambda r: r.update(tip=next(b for b in r['nodes'] if b != r['tip']))),
        ('revision below durable floor', 'RevisionFloorMatches', lambda r: r.update(revision=0)),
        ('altered metadata', 'GraphMatches', lambda r: r['nodes'][next(iter(r['nodes']))].update(timestamp=9999)),
    ]:
        bad = copy.deepcopy(trace)
        corrupt(bad['rows'][0]['recoveredChild'])
        check(bad, invariant)
        negatives.append(label)
    bad = copy.deepcopy(trace)
    bad['rows'][1]['recoveredChild']['nodes'] = copy.deepcopy(trace['rows'][0]['recoveredChild']['nodes'])
    bad['rows'][1]['recoveredChild']['tip'] = trace['rows'][0]['recoveredChild']['tip']
    check(bad, 'GraphMatches')
    negatives.append('failed event visible after reopen')
    for label, field, value in [('partial proof persistence', 'proofs', 1), ('partial cursor advancement', 'cursor', 1)]:
        bad = copy.deepcopy(trace)
        bad['rows'][0]['afterGroup'][field] = value
        try:
            grouping(bad)
        except RuntimeError:
            negatives.append(label)
        else:
            raise RuntimeError('grouping negative control was not rejected')
    unchanged(sources_before, source_snapshot(package_roots), 'local sources')
    unchanged(formal_before, formal_snapshot(), 'formal inputs')
    require(hashlib.sha256(executable.read_bytes()).hexdigest() == executable_hash, 'test artifact changed during validation')
    source_hashes = {p: hashlib.sha256(data).hexdigest() for p, data in sources_before.items()}
    target = ROOT / 'target'
    source_manifest = json.dumps(source_hashes, indent=2) + '\n'
    (target / 'formal-index-recovery-inputs.json').write_text(source_manifest)
    (target / 'formal-index-recovery-trace.json').write_text(json.dumps(trace, indent=2) + '\n')
    inputs = ['src/storage/turn_tests.rs', 'src/storage/turn_tests/formal_index.rs', 'tools/formal-index-recovery-check.py']
    report = {'passed': True, 'formalRevision': PIN, 'observations': count, 'eventScenarios': 4,
              'negativeControls': negatives, 'scope': 'per-level Recovery operators; separate production event-footprint assertions',
              'inputSha256': {p: source_hashes[ROOT.name + '/' + p] for p in inputs},
              'localSourceCount': len(source_hashes),
              'localSourceManifestSha256': hashlib.sha256(source_manifest.encode()).hexdigest(),
              'formalTrackedClean': True,
              'tlcLauncherSha256': hashlib.sha256(formal_before[3]).hexdigest(),
              'testArtifactSha256': executable_hash,
              'modelSha256': hashlib.sha256(formal_before[2]).hexdigest(),
              'traceSha256': hashlib.sha256((target / 'formal-index-recovery-trace.json').read_bytes()).hexdigest()}
    (target / 'formal-index-recovery-report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))


if __name__ == '__main__':
    main()
