#!/usr/bin/env python3
"""Actual daemon JSON configuration, paired browser listeners and shutdown."""
import base64, hashlib, json, pathlib, runpy, shutil, socket, struct, subprocess, tempfile
ROOT = pathlib.Path(__file__).resolve().parents[1]
h = runpy.run_path(str(ROOT / 'tools/child-node-integration.py'))

def snapshot():
    values = h['snapshot']()
    values['lattice-node-rs/tools/overlay-node-integration.py'] = hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()
    return values

def main():
    out = ROOT / 'target/overlay-node-integration'
    out.mkdir(parents=True, exist_ok=True)
    before = snapshot()
    report = {'passed': False, 'inputs_sha256': before, 'scope': 'actual localhost daemon startup, HTTP upgrade, UDP STUN, disabled listener, restart and shutdown; authenticated overlay tested separately in Rust'}
    processes = []
    try:
        with (out / 'build.log').open('w') as log:
            subprocess.run(['cargo','build','--locked','--bin','lattice-node'], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
            subprocess.run(['cargo','build','--locked','-p','lattice-validation','--example','nexus_genesis_volumes'], cwd=h['WORKSPACE'], stdout=log, stderr=subprocess.STDOUT, check=True)
        assert snapshot() == before, 'sources changed during build'
        with tempfile.TemporaryDirectory(prefix='lattice-overlay-') as tmp:
            directory = pathlib.Path(tmp)
            binary = pathlib.Path(shutil.copy2(ROOT / 'target/debug/lattice-node', directory / 'node'))
            bootstrap = pathlib.Path(shutil.copy2(h['WORKSPACE'] / 'target/debug/examples/nexus_genesis_volumes', directory / 'bootstrap'))
            report['binary_sha256'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in [binary, bootstrap]}
            with (directory / 'bootstrap.json').open('w') as stream:
                subprocess.run([str(bootstrap)], stdout=stream, check=True)
            # Reserve a port known to be available for both protocols, then hand it to the daemon.
            with socket.socket() as tcp, socket.socket(type=socket.SOCK_DGRAM) as udp:
                tcp.bind(('127.0.0.1', 0)); port = tcp.getsockname()[1]
                udp.bind(('127.0.0.1', port))
            node = h['Node']('browser', directory, binary, out, processes)
            node.config.update(webSocketPort=port, externalAddress='203.0.113.7')
            node.start()
            assert node.ready['webSocket'] == f'0.0.0.0:{port}'
            assert node.rpc('/status')[0] == 200
            with socket.create_connection(('127.0.0.1', port), timeout=3) as connection:
                key = base64.b64encode(bytes(range(16))).decode()
                connection.sendall(f'GET /ivy HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n'.encode())
                answer = connection.recv(4096)
                assert answer.startswith(b'HTTP/1.1 101 '), answer
            with socket.socket(type=socket.SOCK_DGRAM) as udp:
                udp.settimeout(3)
                transaction = b'local-stun00'
                udp.sendto(struct.pack('>HHI', 1, 0, 0x2112A442) + transaction, ('127.0.0.1', port))
                answer, _ = udp.recvfrom(512)
                assert answer[:2] == b'\x01\x01' and answer[4:8] == bytes.fromhex('2112a442') and answer[8:20] == transaction, answer
            node.stop()
            with socket.socket() as tcp, socket.socket(type=socket.SOCK_DGRAM) as udp:
                tcp.bind(('0.0.0.0', port)); udp.bind(('0.0.0.0', port))
                node.config['webSocketPort'] = 0
                node.start()
                assert node.ready['webSocket'] is None
                assert node.rpc('/status')[0] == 200
                node.stop()
            assert all(hashlib.sha256(p.read_bytes()).hexdigest() == report['binary_sha256'][p.name] for p in [binary, bootstrap])
        assert snapshot() == before, 'sources changed during verification'
        report.update(passed=True, checks=['configured paired listener', 'HTTP upgrade', 'STUN response', 'socket release', 'disabled restart despite occupied browser port'])
    finally:
        for node in list(processes):
            if node.process.poll() is None: node.process.kill(); node.process.wait()
            node.log.close()
        (out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': report['passed'], 'checks': report['checks']}))

if __name__ == '__main__': main()
