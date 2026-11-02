#!/usr/bin/env python3
"""Actual two-daemon hosted-child mining, native sync and durable recovery.

Requires the hosted-child actor and scoped RPC implementation. Unsupported
endpoints/configurations fail explicitly; they are never replaced by fixture
admission facts or mocked responses.
"""
import argparse
import base64
import copy
import hashlib
import json
import pathlib
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parent / 'lattice-rs'
GENESIS = 'bafyreiggtg4ezifboyekbf4gxst2jr3mpjqcxsbmopy7w6fp46g4ngpgxa'
CHILD = ['Nexus', 'Alpha']
CHILD_KEY = '/'.join(CHILD)


def request(base, cookie, route, body=None):
    headers = {'Content-Type': 'application/json', 'Authorization': 'Basic ' + base64.b64encode(cookie.read_bytes().strip()).decode()}
    req = urllib.request.Request(base + route, data=None if body is None else json.dumps(body).encode(), headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def wait_for(check, timeout=90):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = check()
            if last:
                return last
        except (OSError, ValueError) as error:
            last = str(error)
        time.sleep(0.1)
    raise RuntimeError(('condition did not complete', last))


def snapshot():
    paths = [pathlib.Path(__file__).resolve(), ROOT / 'tests/child_adversarial_peer.rs', WORKSPACE / 'crates/lattice-validation/examples/nexus_genesis_volumes.rs', WORKSPACE / 'crates/lattice-validation/examples/mixed_node_signer.rs']
    for name in ['lattice-node-rs', 'lattice-rs', 'ivy-rs', 'cashew-rs', 'array-trie-rs', 'trie-dictionary-rs', 'volume-broker-rs', 'tally-rs']:
        repo = ROOT.parent / name
        paths += list(repo.glob('Cargo.*')) + list(repo.glob('src/**/*.rs')) + list(repo.glob('crates/*/Cargo.toml')) + list(repo.glob('crates/*/src/**/*.rs'))
    return {str(path.relative_to(ROOT.parent)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(set(paths))}


class Node:
    def __init__(self, name, directory, binary, out, processes, seeds=()):
        self.name, self.directory, self.binary, self.out = name, directory, binary, out
        self.processes = processes
        self.config = {'directory': name, 'identityKey': name + '.key', 'genesisCid': GENESIS, 'bootstrapFiles': ['bootstrap.json'], 'listen': '127.0.0.1:0', 'rpc': '127.0.0.1:0', 'seeds': list(seeds)}
        self.config_path = directory / (name + '.json')
        self.cookie = directory / name / '.cookie'
        self.round = 0

    def start(self):
        self.config_path.write_text(json.dumps(self.config))
        self.round += 1
        path = self.out / f'{self.name}-{self.round}.log'
        self.log = path.open('w')
        self.process = subprocess.Popen([str(self.binary), '--config', str(self.config_path)], stdout=self.log, stderr=subprocess.STDOUT)
        self.processes.append(self)

        def ready():
            assert self.process.poll() is None, path.read_text()
            for line in path.read_text().splitlines():
                if line.startswith('{'):
                    value = json.loads(line)
                    if value.get('event') == 'ready':
                        return value
        self.ready = wait_for(ready, 20)
        self.base = 'http://' + self.ready['rpc']
        self.config.update(listen=self.ready['overlay'], rpc=self.ready['rpc'], bootstrapFiles=[])

    def stop(self):
        self.process.terminate()
        try:
            result = self.process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
            raise RuntimeError('daemon did not shut down')
        self.log.close()
        self.processes.remove(self)
        assert result == 0, result

    def rpc(self, route, body=None):
        return request(self.base, self.cookie, route, body)

    def host(self, spec=None, recipient=GENESIS):
        body = {'hostChain': [CHILD_KEY + ('=' + str(spec) if spec else '')]}
        if spec:
            body['premineTo'] = [CHILD_KEY + '=' + recipient]
        code, value = self.rpc('/hosted-chains', body)
        assert code == 200, ('hosted configuration unavailable or refused', self.name, code, value)
        code, value = self.rpc('/hosted-chains')
        assert code == 200 and CHILD_KEY in value['chains'], (code, value)

    def state(self, child=False):
        route = '/status' + ('?chainPath=' + urllib.parse.quote(CHILD_KEY, safe='') if child else '')
        code, value = self.rpc(route)
        if child and code == 404:
            return {}  # Hosted level exists but has no executed child yet.
        assert code == 200, ('scoped status unavailable', self.name, code, value)
        assert value['chainPath'] == (CHILD if child else ['Nexus']), value
        return value

    def balance(self, tip, child=False, address=GENESIS):
        route = '/accounts/' + address + '?block=' + tip
        if child:
            route += '&chainPath=' + urllib.parse.quote(CHILD_KEY, safe='')
        code, value = self.rpc(route)
        assert code == 200, (self.name, code, value)
        return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=pathlib.Path, default=ROOT / 'target/child-node-integration')
    parser.add_argument('--state', choices=['all', 'tip', 'none'], default='all')
    parser.add_argument('--block-bodies', choices=['all', 'none'], default='all')
    parser.add_argument('--sweep', action='store_true', help='Sweep both hosted daemons before and after restart')
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    report = {'passed': False, 'started_unix': time.time(), 'scope': 'two actual Rust daemons with hosted child, localhost only', 'forged_proof_control': {'executed': False, 'reason': 'Pinned HTTP API accepts child block/index bytes but exposes no securing-proof submission endpoint.'}}

    def save():
        temp = out / 'report.tmp'
        temp.write_text(json.dumps(report, indent=2) + '\n')
        temp.replace(out / 'report.json')
    save()
    sources = snapshot()
    report['retention_policy'] = {'state': args.state, 'blockBodies': args.block_bodies, 'sweep': args.sweep}
    report['inputs_sha256'] = sources
    report['rustc_version'] = subprocess.check_output(['rustc', '--version'], text=True).strip()
    save()
    processes = []
    try:
        with (out / 'build.log').open('w') as log:
            subprocess.run(['cargo', 'test', '--locked', '--test', 'child_adversarial_peer'], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
            report['authenticated_ivy_proof_controls'] = {'passed': True, 'positive': 'canonical securing proof executes child', 'negative': 'wrong directory proof disconnects authenticated sender without execution or persisted proof'}
            save()
            subprocess.run(['cargo', 'build', '--locked', '--bin', 'lattice-node'], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
            subprocess.run(['cargo', 'build', '--locked', '-p', 'lattice-validation', '--example', 'mixed_node_signer', '--example', 'nexus_genesis_volumes'], cwd=WORKSPACE, stdout=log, stderr=subprocess.STDOUT, check=True)
        with tempfile.TemporaryDirectory(prefix='lattice-hosted-child-') as temporary:
            directory = pathlib.Path(temporary)
            node_bin = pathlib.Path(shutil.copy2(ROOT / 'target/debug/lattice-node', directory / 'lattice-node'))
            mine_bin = pathlib.Path(shutil.copy2(WORKSPACE / 'target/debug/examples/mixed_node_signer', directory / 'mine-fixture'))
            bootstrap_bin = pathlib.Path(shutil.copy2(WORKSPACE / 'target/debug/examples/nexus_genesis_volumes', directory / 'bootstrap-fixture'))
            report['binary_sha256'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in [node_bin, mine_bin, bootstrap_bin]}
            with (directory / 'bootstrap.json').open('w') as output:
                subprocess.run([str(bootstrap_bin)], stdout=output, check=True)
            identity = json.loads(subprocess.check_output([str(mine_bin)], text=True))
            recipient = identity['address']
            spec = {'maxNumberOfTransactionsPerBlock': 1000, 'maxStateGrowth': 100000, 'maxBlockSize': 1000000, 'premine': 1, 'targetBlockTime': 1000, 'initialReward': 10, 'halvingInterval': 1000, 'halfLife': 120, 'wasmPolicies': []}
            spec_path = directory / 'alpha.json'
            spec_path.write_text(json.dumps(spec))
            first = Node('producer', directory, node_bin, out, processes)
            first.config.update(state=args.state, blockBodies=args.block_bodies)
            first.start()
            first.host(spec_path, recipient)
            host, port = first.ready['overlay'].rsplit(':', 1)
            second = Node('follower', directory, node_bin, out, processes, [{'publicKey': first.ready['publicKey'], 'host': host, 'port': int(port)}])
            second.config.update(state=args.state, blockBodies=args.block_bodies)
            second.start()
            second.host()
            report['rounds'] = []
            for root_height, child_height in [(1, 0), (2, 1)]:
                if child_height == 1:
                    transaction = json.loads(subprocess.check_output([str(mine_bin), '0', '3', GENESIS, CHILD_KEY], text=True))
                    bad = copy.deepcopy(transaction)
                    bad['transaction']['signatures'][identity['publicKey']] = '00' * 64
                    rejected = []
                    for node in [first, second]:
                        code, value = node.rpc('/transactions', bad)
                        assert 400 <= code < 500 and 'invalid' in str(value).lower(), (code, value)
                        rejected.append({'status': code, 'body': value})
                    code, submitted_tx = second.rpc('/transactions', transaction)
                    assert code == 200, (code, submitted_tx)
                    wait_for(lambda: first.state(True).get('mempoolCount') == 1)
                    report['child_payment'] = {'transaction': transaction, 'submitted': submitted_tx, 'invalid_signature_controls': rejected, 'producer_pool': first.state(True)}
                    save()
                code, template = first.rpc('/mining/templates', {'recipients': [{'chainPath': ['Nexus'], 'address': GENESIS}, {'chainPath': CHILD, 'address': recipient}]})
                assert code == 200, (code, template)
                declared = {tuple(c['chainPath']): c for c in template['chains']}
                assert int(declared[tuple(CHILD)]['height']) == child_height, declared
                # Search the hardest advertised target, so this scenario advances
                # both root and child even when merged thresholds differ.
                work_template = dict(template, searchTarget=min(template['targets'], key=lambda x: int(x, 16)))
                template_path = directory / 'template.json'
                template_path.write_text(json.dumps(work_template))
                work = json.loads(subprocess.check_output([str(mine_bin), '--mine', str(template_path)], text=True))
                digest = hashlib.sha256(bytes(work['prefix']) + work['nonce'].to_bytes(8, 'big')).digest()
                assert digest == bytes(work['hash']) and int.from_bytes(digest, 'big') <= int(work_template['searchTarget'], 16)
                code, submitted = first.rpc('/mining/work', {'workID': template['workID'], 'nonce': work['nonce']})
                assert code == 200 and submitted['accepted'], (code, submitted)
                proofs = {p['directory']: p['childCID'] for p in submitted['durableChildProofs']}
                assert 'Alpha' in proofs, ('missing durable proof acknowledgement', submitted)

                def synced():
                    a, b = first.state(), second.state()
                    ca, cb = first.state(True), second.state(True)
                    if a.get('height') == b.get('height') == str(root_height) and a.get('tipCID') == b.get('tipCID') and ca.get('height') == cb.get('height') == str(child_height) and ca.get('tipCID') == cb.get('tipCID') == proofs['Alpha']:
                        return {'producer': a, 'follower': b, 'producer_child': ca, 'follower_child': cb}
                states = wait_for(synced)
                child_tip = states['producer_child']['tipCID']
                balances = [n.balance(child_tip, True, recipient) for n in [first, second]]
                assert balances[0] == balances[1] and balances[0]['balance'] == ('10' if child_height == 0 else '17') and balances[0]['nonce'] == str(child_height), balances
                if child_height == 1:
                    receivers = [n.balance(child_tip, True) for n in [first, second]]
                    assert receivers[0] == receivers[1] and receivers[0]['balance'] == '3' and receivers[0]['nonce'] == '0', receivers
                    assert all(n.state(True)['mempoolCount'] == 0 for n in [first, second])
                    report['child_payment']['receiver_account'] = receivers[0]
                    tx_cid = report['child_payment']['submitted']['transactionCID']
                    reads = []
                    scope = '?chainPath=' + urllib.parse.quote(CHILD_KEY, safe='')
                    for node in [first, second]:
                        code, dto = node.rpc('/api/transaction/' + tx_cid + scope)
                        assert code == 200 and dto['chainPath'] == CHILD, (code, dto)
                        if args.state == 'all' or 'blockHash' in dto:
                            assert dto.get('blockHash') == child_tip and dto.get('blockHeight') == '1', (code, dto)
                        else:
                            assert 'blockHeight' not in dto, dto  # Historical nonce evidence may be unavailable.
                        code, raw = node.rpc('/transactions/' + tx_cid + scope)
                        assert code == 200 and raw['cid'] == tx_cid and raw['transaction']['body']['rawCID'], (code, raw)
                        reads.append({'dto': dto, 'raw': raw})
                    report['child_payment']['scoped_reads'] = reads
                report['rounds'].append({'template': template, 'work': work, 'submitted': submitted, 'states': states, 'child_account': balances[0]})
                save()
            expected = report['rounds'][-1]
            if args.sweep:
                report['sweeps_before_restart'] = []
                for node in [first, second]:
                    code, result = node.rpc('/sweep', {})
                    assert code == 200, (code, result)
                    assert node.balance(proofs['Alpha'], True, recipient) == expected['child_account']
                    assert node.balance(proofs['Alpha'], True) == report['child_payment']['receiver_account']
                    report['sweeps_before_restart'].append(result)
            first.stop()
            second.stop()
            first.start()
            first.host(spec_path, recipient)
            second.start()
            second.host()
            restored = wait_for(synced)
            if args.sweep:
                report['sweeps_after_restart'] = []
                for node in [first, second]:
                    code, result = node.rpc('/sweep', {})
                    assert code == 200, (code, result)
                    report['sweeps_after_restart'].append(result)
            assert restored['producer']['tipCID'] == expected['states']['producer']['tipCID']
            balances = [n.balance(proofs['Alpha'], True, recipient) for n in [first, second]]
            assert balances == [expected['child_account']] * 2, balances
            assert [n.balance(proofs['Alpha'], True) for n in [first, second]] == [report['child_payment']['receiver_account']] * 2
            report['restarted'] = restored
            report['restarted_child_account'] = balances[0]
            first.stop()
            second.stop()
            current = snapshot()
            assert current == sources, ('source changed during run', [p for p in sources.keys() | current.keys() if sources.get(p) != current.get(p)])
            report.update(passed=True, completed_unix=time.time(), inputs_sha256=sources, rustc_version=subprocess.check_output(['rustc', '--version'], text=True).strip())
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
