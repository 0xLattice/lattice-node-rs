#!/usr/bin/env python3
"""Real author build/push, independent parent commitment and durable restart.

Builds and copies exact binaries. Source changes during the run invalidate the
report. All keys, transactions and mining are disposable localhost fixtures.
"""
import base64
import copy
import hashlib
import json
import pathlib
import runpy
import shutil
import sqlite3
import subprocess
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
helpers = runpy.run_path(str(ROOT / 'tools/child-node-integration.py'))
Node = helpers['Node']
wait_for = helpers['wait_for']
CHILD = helpers['CHILD']


def snapshot():
    result = helpers['snapshot']()
    for p in [pathlib.Path(__file__), ROOT / 'examples/child-author-transaction.rs']:
        result[str(p.relative_to(ROOT.parent))] = hashlib.sha256(p.read_bytes()).hexdigest()
    return result


def main():
    out = ROOT / 'target/child-author-integration'
    out.mkdir(parents=True, exist_ok=True)
    report = {'passed': False, 'started_unix': time.time(), 'rounds': []}
    processes = []
    sources = snapshot()
    report['inputs_sha256'] = sources

    def save():
        temp = out / 'report.tmp'
        temp.write_text(json.dumps(report, indent=2) + '\n')
        temp.replace(out / 'report.json')

    save()
    try:
        with (out / 'build.log').open('w') as log:
            subprocess.run(['cargo', 'build', '--locked', '--bin', 'lattice-node', '--example', 'child-author-transaction'], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
            subprocess.run(['cargo', 'build', '--locked', '-p', 'lattice-validation', '--example', 'mixed_node_signer', '--example', 'nexus_genesis_volumes'], cwd=ROOT.parent / 'lattice-rs', stdout=log, stderr=subprocess.STDOUT, check=True)
        with tempfile.TemporaryDirectory(prefix='lattice-child-author-') as d:
            tmp = pathlib.Path(d)
            binaries = [ROOT / 'target/debug/lattice-node', ROOT / 'target/debug/examples/child-author-transaction', ROOT.parent / 'lattice-rs/target/debug/examples/mixed_node_signer', ROOT.parent / 'lattice-rs/target/debug/examples/nexus_genesis_volumes']
            node, signer, miner, bootstrap = [pathlib.Path(shutil.copy2(p, tmp / p.name)) for p in binaries]
            report['binary_sha256'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in [node, signer, miner, bootstrap]}
            with (tmp / 'bootstrap.json').open('w') as f:
                subprocess.run([bootstrap], stdout=f, check=True)
            identity = json.loads(subprocess.check_output([miner], text=True))
            owner = identity['address']
            parent = Node('parent', tmp, node, out, processes)
            parent.start()
            host, port = parent.ready['overlay'].rsplit(':', 1)
            author = Node('author', tmp, node, out, processes, [{'publicKey': parent.ready['publicKey'], 'host': host, 'port': int(port)}])
            author.start()
            spec = {'maxNumberOfTransactionsPerBlock': 1000, 'maxStateGrowth': 100000, 'maxBlockSize': 1000000, 'premine': 1, 'targetBlockTime': 1000, 'initialReward': 10, 'halvingInterval': 1000, 'halfLife': 120, 'wasmPolicies': []}
            specpath = tmp / 'spec.json'
            specpath.write_text(json.dumps(spec))
            author.host(specpath, owner)

            def mine():
                code, template = parent.rpc('/mining/templates', {'recipients': [{'chainPath': ['Nexus'], 'address': owner}]})
                assert code == 200, (code, template)
                path = tmp / 'template.json'
                path.write_text(json.dumps(template))
                work = json.loads(subprocess.check_output([miner, '--mine', path], text=True))
                digest = hashlib.sha256(bytes(work['prefix']) + work['nonce'].to_bytes(8, 'big')).digest()
                assert digest == bytes(work['hash']) and int.from_bytes(digest, 'big') <= int(template['searchTarget'], 16)
                code, result = parent.rpc('/mining/work', {'workID': template['workID'], 'nonce': work['nonce']})
                assert code == 200 and result['accepted'], (code, result)
                wait_for(lambda: author.state().get('tipCID') == result['tipCID'])
                return result

            mine()
            previous = None
            for height in [0, 1]:
                code, commitment = author.rpc('/child-blocks/commitment?chainPath=Nexus/Alpha')
                assert code == 200 and commitment.get('value') == previous, (code, commitment)
                before = author.state(True)
                code, built = author.rpc('/child-blocks/build', {'chainPath': CHILD, 'recipient': owner})
                assert code == 200, (code, built)
                assert built['parentState'] == commitment['parentState'] and built.get('previous') == previous
                assert ('spec' in built['submit']) == (height == 0)
                assert author.state(True) == before, 'building a candidate must not grant execution'
                code, pushed = author.rpc('/child-blocks', built['submit'])
                assert code == 200 and pushed['status'] == 'dropped', (code, pushed)
                assert author.state(True) == before, 'uncommitted push must not grant execution'
                negative = []
                for body in [{'chainPath': ['Nexus']}, {'chainPath': ['Nexus', 'Missing']}, {'chainPath': CHILD, 'recipient': 'invalid'}]:
                    code, value = author.rpc('/child-blocks/build', body)
                    assert 400 <= code < 500, (code, value)
                    negative.append({'status': code, 'body': value})
                for field in ['block', 'children'] + (['spec'] if height == 0 else []):
                    malformed = copy.deepcopy(built['submit'])
                    malformed[field] = base64.b64encode(b'garbage').decode()
                    code, value = author.rpc('/child-blocks', malformed)
                    assert 400 <= code < 500 and 'malformedBlock' in str(value), (field, code, value)
                    negative.append({'field': field, 'status': code, 'body': value})
                # A trailing byte violates canonical CBOR even if a decoder accepts a prefix.
                malformed = copy.deepcopy(built['submit'])
                malformed['block'] = base64.b64encode(base64.b64decode(malformed['block']) + b'\x00').decode()
                code, value = author.rpc('/child-blocks', malformed)
                assert 400 <= code < 500, (code, value)
                args = [signer, built['blockCID'], built['parentState'], str(height)]
                if previous is not None:
                    args.append(previous)
                tx = json.loads(subprocess.check_output(args, text=True))
                code, submitted = parent.rpc('/transactions', tx)
                assert code == 200, (code, submitted)
                carrier = mine()

                def push():
                    code, value = author.rpc('/child-blocks', built['submit'])
                    assert code == 200, (code, value)
                    return value if value['status'] == 'tree' else None

                if height == 0:
                    # A real securing commitment is present, but the authored
                    # child has not crossed its durable admission barrier.
                    database = tmp / 'author/state.db'
                    with sqlite3.connect(database) as db:
                        baseline = db.execute("SELECT COUNT(*) FROM admission_batches WHERE chain_path='Nexus/Alpha'").fetchone()[0]
                        assert baseline == 0
                        db.execute("CREATE TRIGGER author_fact_failure BEFORE INSERT ON child_proofs WHEN NEW.chain='Nexus/Alpha' BEGIN SELECT RAISE(ABORT,'injected author proof failure'); END")
                    code, failure = author.rpc('/child-blocks', built['submit'])
                    assert code == 503, (code, failure)
                    assert author.process.wait(timeout=15) != 0
                    author.log.close()
                    processes.remove(author)
                    with sqlite3.connect(database) as db:
                        assert db.execute("SELECT COUNT(*) FROM admission_batches WHERE chain_path='Nexus/Alpha'").fetchone()[0] == baseline
                        assert db.execute("SELECT COUNT(*) FROM child_proofs WHERE chain='Nexus/Alpha'").fetchone()[0] == 0
                        db.execute('DROP TRIGGER author_fact_failure')
                    author.start()
                    author.host(specpath, owner)
                    assert author.state(True)['phase'] == 'awaitingGenesis'
                    assert author.state()['tipCID'] == carrier['tipCID']
                    report['author_storage_failure'] = {'status': code, 'fail_stopped': True, 'rolled_back_child': True, 'root_commitment_recovered': True}

                accepted = wait_for(push)
                state = wait_for(lambda: (s if (s := author.state(True)).get('tipCID') == built['blockCID'] else None))
                assert state['height'] == str(height), state
                previous = built['blockCID'] + '@' + built['parentState']
                code, after = author.rpc('/child-blocks/commitment?chainPath=Nexus/Alpha')
                assert code == 200 and after['value'] == previous, (code, after)
                balance = author.balance(built['blockCID'], True, owner)
                assert balance['balance'] == str(10 * (height + 1)), balance
                report['rounds'].append({'built': built, 'uncommitted_push': pushed, 'accepted_push': accepted, 'commitment_before': commitment, 'commitment_after': after, 'parent_carrier': carrier, 'negative': negative, 'child_account': balance})
                save()
                author.stop()
                author.start()
                author.host(specpath, owner)
                code, restarted = author.rpc('/child-blocks', built['submit'])
                assert code == 200 and restarted['status'] == 'tree', (code, restarted)
                assert author.balance(built['blockCID'], True, owner) == balance
                report['rounds'][-1]['restart_push'] = restarted
            author.stop()
            parent.stop()
            current = snapshot()
            assert sources == current, ('source changed during run', [p for p in sources.keys() | current.keys() if sources.get(p) != current.get(p)])
            report.update(passed=True, completed_unix=time.time())
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        for process in list(processes):
            process.stop()
        save()


if __name__ == '__main__':
    main()
