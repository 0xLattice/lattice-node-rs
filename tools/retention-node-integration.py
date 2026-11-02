#!/usr/bin/env python3
"""Real localhost daemon retention modes, sweep, signed state changes and recovery."""
import argparse
import base64
import hashlib
import importlib.util
import json
import pathlib
import shutil
import sqlite3
import subprocess
import tempfile
import time
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('child_integration', ROOT / 'tools/child-node-integration.py')
shared = importlib.util.module_from_spec(spec)
spec.loader.exec_module(shared)


def snapshot():
    result = shared.snapshot()
    path = pathlib.Path(__file__).resolve()
    result[str(path.relative_to(ROOT.parent))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def inventory(node):
    # Observation only: no database writes or admission facts are injected.
    with sqlite3.connect('file:' + str(node.directory / node.name / 'volumes.db') + '?mode=ro', uri=True) as db:
        return {'roots': [v[0] for v in db.execute('SELECT root FROM volume_metadata ORDER BY root')],
                'pins': list(db.execute('SELECT scope,root FROM retained_roots ORDER BY scope,root'))}


def unpin(node, cid):
    request = urllib.request.Request(node.base + '/pins/' + cid, method='DELETE', headers={
        'Authorization': 'Basic ' + base64.b64encode(node.cookie.read_bytes().strip()).decode()})
    with urllib.request.urlopen(request, timeout=10) as response:
        assert response.status == 200
        return json.load(response)


def mine(node, helper, directory, owner, height):
    code, template = node.rpc('/mining/templates', {'recipients': [{'chainPath': ['Nexus'], 'address': owner}]})
    assert code == 200, (code, template)
    path = directory / 'template.json'
    path.write_text(json.dumps(template))
    work = json.loads(subprocess.check_output([str(helper), '--mine', str(path)], text=True))
    digest = hashlib.sha256(bytes(work['prefix']) + work['nonce'].to_bytes(8, 'big')).digest()
    assert digest == bytes(work['hash']) and int.from_bytes(digest, 'big') <= int(template['searchTarget'], 16)
    code, submitted = node.rpc('/mining/work', {'workID': template['workID'], 'nonce': work['nonce']})
    assert code == 200 and submitted['accepted'], (code, submitted)
    status = shared.wait_for(lambda: (s if (s := node.state()).get('height') == str(height) else None))
    return {'height': height, 'cid': status['tipCID'], 'post_state': template['block']['postState']['rawCID'], 'template': template, 'work': work}


def sweep(node):
    code, result = node.rpc('/sweep', {})
    assert code == 200, ('operator sweep unavailable', code, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=pathlib.Path, default=ROOT / 'target/retention-node-integration')
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    report = {'passed': False, 'started_unix': time.time(), 'scope': 'actual daemon retention; SQLite is observed read-only', 'cases': []}
    def save():
        path = out / 'report.tmp'
        path.write_text(json.dumps(report, indent=2) + '\n')
        path.replace(out / 'report.json')
    save()
    sources = snapshot()
    report['inputs_sha256'] = sources
    save()
    processes = []
    try:
        with (out / 'build.log').open('w') as log:
            subprocess.run(['cargo', 'build', '--locked', '--bin', 'lattice-node'], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
            subprocess.run(['cargo', 'build', '--locked', '-p', 'lattice-validation', '--example', 'mixed_node_signer', '--example', 'nexus_genesis_volumes'], cwd=shared.WORKSPACE, stdout=log, stderr=subprocess.STDOUT, check=True)
        with tempfile.TemporaryDirectory(prefix='lattice-retention-') as temp:
            directory = pathlib.Path(temp)
            node_bin = pathlib.Path(shutil.copy2(ROOT / 'target/debug/lattice-node', directory / 'lattice-node'))
            helper = pathlib.Path(shutil.copy2(shared.WORKSPACE / 'target/debug/examples/mixed_node_signer', directory / 'signer'))
            bootstrap = pathlib.Path(shutil.copy2(shared.WORKSPACE / 'target/debug/examples/nexus_genesis_volumes', directory / 'bootstrap'))
            report['binary_sha256'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in [node_bin, helper, bootstrap]}
            with (directory / 'bootstrap.json').open('w') as stream:
                subprocess.run([str(bootstrap)], stdout=stream, check=True)
            owner = json.loads(subprocess.check_output([str(helper)], text=True))['address']
            for state in ['all', 'tip', 'none']:
                for bodies in ['all', 'none']:
                    node = shared.Node(state + '-' + bodies, directory, node_bin, out, processes)
                    node.config.update(state=state, blockBodies=bodies)
                    node.start()
                    blocks = [mine(node, helper, directory, owner, 1)]
                    # Pin two roots already shared with execution/body scopes.
                    pinned = [blocks[0]['cid'], blocks[0]['post_state']]
                    for cid in pinned:
                        code, result = node.rpc('/pins', {'cid': cid})
                        assert code == 200, (code, result)
                    transaction = json.loads(subprocess.check_output([str(helper), '0', '3', shared.GENESIS], text=True))
                    code, submitted = node.rpc('/transactions', transaction)
                    assert code == 200, (code, submitted)
                    blocks.append(mine(node, helper, directory, owner, 2))
                    transaction = json.loads(subprocess.check_output([str(helper), '1', '2', shared.GENESIS], text=True))
                    code, submitted = node.rpc('/transactions', transaction)
                    assert code == 200, (code, submitted)
                    blocks.append(mine(node, helper, directory, owner, 3))
                    swept = sweep(node)
                    held = inventory(node)
                    assert set(pinned).issubset(held['roots'])
                    assert shared.GENESIS in held['roots'] and blocks[-1]['cid'] in held['roots']
                    assert (blocks[1]['cid'] in held['roots']) == (bodies == 'all'), ('body mode', state, bodies, held)
                    assert (blocks[1]['post_state'] in held['roots']) == (state == 'all'), ('state mode', state, bodies, held)
                    expected = str(3 * 1048576 - 5)
                    balance = node.balance(blocks[-1]['cid'], address=owner)
                    assert balance['balance'] == expected and balance['nonce'] == '2', balance
                    receiver = node.balance(blocks[-1]['cid'])
                    assert receiver['balance'] == '5' and receiver['nonce'] == '0', receiver
                    node.stop()
                    node.start()  # bootstrapFiles is now empty.
                    assert node.state()['tipCID'] == blocks[-1]['cid']
                    assert node.balance(blocks[-1]['cid'], address=owner) == balance
                    assert node.balance(blocks[-1]['cid']) == receiver
                    assert set(node.rpc('/pins')[1]['pins']) == set(pinned)
                    sweep(node)
                    assert set(pinned).issubset(inventory(node)['roots'])
                    unpin(node, blocks[0]['cid'])
                    sweep(node)
                    assert (blocks[0]['cid'] in inventory(node)['roots']) == (bodies == 'all')
                    case = {'state': state, 'blockBodies': bodies, 'blocks': blocks, 'sweep': swept, 'inventory': held, 'sender': balance, 'receiver': receiver, 'restart': True, 'shared_pin_release': True}
                    if state == 'none' and bodies == 'all':
                        # A reward-only execution does not touch the old nonce
                        # leaf. None may omit it; no peers exist to refill it.
                        extra = mine(node, helper, directory, owner, 4)
                        sweep(node)
                        code, unavailable = node.rpc('/accounts/' + owner + '?block=' + extra['cid'])
                        assert code == 404, ('None must not fabricate omitted nonce', code, unavailable)
                        node.stop()
                        node.start()
                        assert node.state()['tipCID'] == extra['cid']
                        assert node.rpc('/accounts/' + owner + '?block=' + extra['cid'])[0] == 404
                        case['untouched_nonce_negative'] = {'block': extra, 'status': code, 'response': unavailable, 'restart_preserves_unavailability': True}
                    if state == bodies == 'none':
                        case['mode_switches'] = []
                        for height, mode in [(4, 'tip'), (5, 'all'), (6, 'none'), (7, 'tip')]:
                            node.stop()
                            node.config['state'] = mode
                            node.start()
                            transaction = json.loads(subprocess.check_output([str(helper), str(height - 2), '2', shared.GENESIS], text=True))
                            code, submitted = node.rpc('/transactions', transaction)
                            assert code == 200, (code, submitted)
                            block = mine(node, helper, directory, owner, height)
                            sweep(node)
                            actual = node.balance(block['cid'], address=owner)
                            assert actual['balance'] == str(height * 1048576 - (2 * height - 1)) and actual['nonce'] == str(height - 1), actual
                            assert blocks[0]['post_state'] in inventory(node)['roots']
                            case['mode_switches'].append({'mode': mode, 'block': block, 'account': actual})
                    node.stop()
                    report['cases'].append(case)
                    save()
            assert snapshot() == sources, 'source changed during run'
            report.update(passed=True, completed_unix=time.time(), rustc_version=subprocess.check_output(['rustc', '--version'], text=True).strip())
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        for node in list(processes):
            node.process.kill()
            node.process.wait()
            node.log.close()
        save()


if __name__ == '__main__':
    main()
