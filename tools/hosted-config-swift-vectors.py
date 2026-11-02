#!/usr/bin/env python3
"""Run exact pinned Swift premine/checkPremine against shared serialized fixtures."""
import hashlib,json,pathlib,subprocess,tempfile
repo=pathlib.Path(__file__).resolve().parents[1];reference=repo.parent/'lattice-node';lattice=repo.parent/'Lattice';cache=pathlib.Path('/tmp/lattice-genesis-oracle')
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=reference,text=True).strip()=='3df9e0a786c7b103a3da88d1c3881efc81706255'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=lattice,text=True).strip()=='dda2d852429a27be6043599b63bde81530cd9514'
for original in (lattice/'Sources').rglob('*.swift'):assert original.read_bytes()==(cache/original.relative_to(lattice)).read_bytes()
assert hashlib.sha256((cache/'Package.resolved').read_bytes()).hexdigest()=='958f69bd3ec734a372e8cd94e3d09d593674a1025979a2abe2b45a6f08cfc9a6'
def extract(text,marker):
 start=text.index(marker);brace=text.index('{',start);depth=1;i=brace+1
 while depth:
  depth+=(text[i]=='{')-(text[i]=='}');i+=1
 return text[start:i]
source_path=reference/'Sources/LatticeNode/Runtime/NodeRuntime+API.swift';source=source_path.read_text()
target=cache/'Sources/HostedConfigSwiftOracle';target.mkdir(exist_ok=True)
s='import Foundation\nimport Lattice\nimport cashew\ntypealias ChainPath = [String]\nenum NodeRuntime {\n'
for marker in ['    public static func premine(', '    public static func checkPremine(', '    public struct PremineError:']:s+=extract(source,marker)+'\n'
s+='}\n';(target/'Precheck.swift').write_text(s)
(target/'Oracle.swift').write_bytes((repo/'tests/hosted-config-swift-oracle.swift').read_bytes())
p=cache/'Package.swift';s=p.read_text()
if 'name: "HostedConfigSwiftOracle"' not in s:s=s.replace('    targets: [\n','    targets: [\n        .executableTarget(name: "HostedConfigSwiftOracle", dependencies: ["Lattice", .product(name: "cashew", package: "cashew")]),\n',1);p.write_text(s)
with tempfile.TemporaryDirectory() as d:
 path=pathlib.Path(d)/'input.json'
 with path.open('w') as out:subprocess.run(['cargo','run','--quiet','--locked','--example','hosted-config-swift-samples'],cwd=repo,stdout=out,check=True)
 rows=json.loads(path.read_text());results=json.loads(subprocess.check_output(['swift','run','--package-path',str(cache),'--force-resolved-versions','HostedConfigSwiftOracle',str(path)]))
 for row in rows:row['swift']=results[row['name']]
 data=json.dumps(rows,sort_keys=True,indent=2)+'\n';(repo/'tests/hosted-config-swift.json').write_text(data)
 evidence={'node_revision':'3df9e0a786c7b103a3da88d1c3881efc81706255','lattice_revision':'dda2d852429a27be6043599b63bde81530cd9514','cases':len(rows),'source_sha256':hashlib.sha256(source_path.read_bytes()).hexdigest(),'dependency_lock_sha256':hashlib.sha256((cache/'Package.resolved').read_bytes()).hexdigest(),'harness_sha256':{p:hashlib.sha256((repo/p).read_bytes()).hexdigest() for p in ['tools/hosted-config-swift-vectors.py','examples/hosted-config-swift-samples.rs','tests/hosted-config-swift-support/mod.rs','tests/hosted-config-swift-oracle.swift']},'corpus_sha256':hashlib.sha256(data.encode()).hexdigest(),'method':'Exact extracted premine, checkPremine and PremineError methods, unchanged pinned full Lattice libraries. Only ChainPath typealias supplied. Policies in this corpus do not depend on timestamp: Swift reads Date and Rust receives timestamp 1. Compares accept/refuse and exact premine transaction CIDs, not error prose.'}
 (repo/'tests/hosted-config-swift-evidence.json').write_text(json.dumps(evidence,indent=2)+'\n')
 mismatches=[(r['name'],r['expected'],r['swift']) for r in rows if r['expected']!=r['swift']]
 print(json.dumps(mismatches,indent=2));assert not mismatches
 print(f'{len(rows)} actual Swift premine precheck comparisons pass')
