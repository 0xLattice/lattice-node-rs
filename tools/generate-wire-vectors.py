#!/usr/bin/env python3
"""Run pinned production Swift codec against generated Rust/adversarial bytes.
Requires the full dependency-resolved Lattice oracle package (see PORTING.md).
Only shell/config adapters are synthesized; protocol code is copied unchanged.
"""
import pathlib,subprocess,sys,tempfile,json,hashlib
repo=pathlib.Path(__file__).resolve().parents[1]
cache=pathlib.Path(sys.argv[1] if len(sys.argv)>1 else '/tmp/lattice-genesis-oracle')
reference=repo.parent/'lattice-node'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=reference,text=True).strip()=='3df9e0a786c7b103a3da88d1c3881efc81706255'
lattice=repo.parent/'Lattice'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=lattice,text=True).strip()=='dda2d852429a27be6043599b63bde81530cd9514'
for original in (lattice/'Sources').rglob('*.swift'):
 cached=cache/original.relative_to(lattice)
 assert cached.read_bytes()==original.read_bytes(),f'Oracle protocol source differs: {original}'
assert hashlib.sha256((cache/'Package.resolved').read_bytes()).hexdigest()=='958f69bd3ec734a372e8cd94e3d09d593674a1025979a2abe2b45a6f08cfc9a6'
source=reference/'Sources';target=cache/'Sources/NodeWireOracle';target.mkdir(exist_ok=True)
files={'ChainSyncWire':'LatticeNode/Networking/ChainSyncWire.swift','SyncProtocol':'LatticeNodeCore/Sync/SyncProtocol.swift','WireValidation':'LatticeNode/Networking/WireValidation.swift','OverlayWire':'LatticeNode/Networking/OverlayWire.swift'}
for name,path in files.items():
 raw=(source/path).read_text();(target/(name+'.swift')).write_text(raw.replace('import LatticeNodeCore\n','').replace('import Ivy\n',''))
raw=(source/'LatticeNode/Configuration/NodeConfiguration.swift').read_text();address=raw[raw.index('public struct ChainAddress'):raw.index('public enum NodeConfigurationError')]
(target/'Adapters.swift').write_text('import Foundation\nimport Lattice\nenum IvyConfig { static let defaultProtocolMaxFrameSize = 4 * 1024 * 1024 }\nenum NexusGenesis { static let expectedBlockHash = "bafyreieq7pwonjmp3azvp3mkgqmaknqhtzvt3aw634zgulbvhhvfcvu3ia" }\n'+address)
(target/'Oracle.swift').write_text((repo/'tools/NodeWireOracle.swift').read_text())
p=cache/'Package.swift';s=p.read_text()
if 'name: "NodeWireOracle"' not in s:
 s=s.replace('    targets: [\n','    targets: [\n        .executableTarget(name: "NodeWireOracle", dependencies: ["Lattice", .product(name: "cashew", package: "cashew"), .product(name: "UInt256", package: "UInt256")]),\n',1);p.write_text(s)
with tempfile.TemporaryDirectory() as directory:
 d=pathlib.Path(directory);initial=d/'initial.json';mutated=d/'mutated.json'
 with initial.open('w') as out:subprocess.run(['cargo','run','--quiet','--example','wire_samples'],cwd=repo,stdout=out,check=True)
 subprocess.run([sys.executable,str(repo/'tools/wire-mutations.py'),str(initial),str(mutated)],check=True)
 output=subprocess.check_output(['swift','run','--package-path',str(cache),'--force-resolved-versions','NodeWireOracle',str(mutated)])
 rows=json.loads(output);assert all(not row['outcome'].startswith('unexpected') for row in rows)
 (repo/'tests/fixtures/swift-wire.json').write_text(json.dumps(rows,sort_keys=True,indent=2)+'\n')
 print(f'{len(rows)} real Swift wire verdicts; Package.resolved SHA256 {hashlib.sha256((cache/"Package.resolved").read_bytes()).hexdigest()}')
