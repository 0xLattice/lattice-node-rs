#!/usr/bin/env python3
"""Actual pinned assembly and extracted policy method, against full pinned Lattice."""
import hashlib,json,pathlib,subprocess,tempfile
repo=pathlib.Path(__file__).resolve().parents[1];reference=repo.parent/'lattice-node';lattice=repo.parent/'Lattice';cache=pathlib.Path('/tmp/lattice-genesis-oracle')
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=reference,text=True).strip()=='3df9e0a786c7b103a3da88d1c3881efc81706255'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=lattice,text=True).strip()=='dda2d852429a27be6043599b63bde81530cd9514'
for original in (lattice/'Sources').rglob('*.swift'):assert original.read_bytes()==(cache/original.relative_to(lattice)).read_bytes()
assert hashlib.sha256((cache/'Package.resolved').read_bytes()).hexdigest()=='958f69bd3ec734a372e8cd94e3d09d593674a1025979a2abe2b45a6f08cfc9a6'
sources={}
def source(path):
 p=reference/path;sources[path]=hashlib.sha256(p.read_bytes()).hexdigest();return p.read_text()
def extract(text,marker):
 start=text.index(marker);brace=text.index('{',start);depth=1;i=brace+1
 while depth:
  depth+=(text[i]=='{')-(text[i]=='}');i+=1
 return text[start:i]
target=cache/'Sources/MergedTemplateSwiftOracle';target.mkdir(exist_ok=True)
s=source('Sources/LatticeNode/Mining/MiningTemplateAssembly.swift')
for name in ['Ivy','LatticeNodeCore','LatticeNodeAPI']:s=s.replace(f'import {name}\n','')
(target/'Assembly.swift').write_text(s)
imports='import Foundation\nimport Lattice\nimport cashew\nimport Crypto\nimport UInt256\n'
s=imports+'enum MiningPlan {\n'+extract(source('Sources/LatticeNode/Mining/MiningPlan.swift'),'    static func policyAcceptedTransactions(')+'\n}\n'
s+='enum NodeRuntime {\n'+extract(source('Sources/LatticeNode/Runtime/NodeRuntime+API.swift'),'    static func templateDigest(tip: String, transactions: [String])')+'\n}\n'
s+='enum Mempool {\n'+extract(source('Sources/LatticeNodeCore/Mempool/Mempool.swift'),'    public static func cid(of transaction: Transaction)')+'\n}\n'
s+=extract(source('Sources/LatticeNodeCore/Mining/MiningState.swift'),'public struct TemplateChain:')+'\n'
s+=source('Sources/LatticeNodeAPI/MiningWork.swift')+'\n'
s+='enum NodeAPIError:Error {case templateTooLarge,templateContextChanged,unresolvedTransactionContent,timestampOverflow}\nlet _wireAtomCapacity = Int(UInt16.max)\n'
w=source('Sources/LatticeNode/Networking/WireValidation.swift')
s+=extract(w,'func _isBoundedWireAtom(')+'\n'+extract(w,'func _isBoundedDirectoryAtom(')+'\n'
(target/'Support.swift').write_text(s)
(target/'Oracle.swift').write_bytes((repo/'tests/merged-template-swift-oracle.swift').read_bytes())
p=cache/'Package.swift';s=p.read_text()
if 'name: "MergedTemplateSwiftOracle"' not in s:s=s.replace('    targets: [\n','    targets: [\n        .executableTarget(name: "MergedTemplateSwiftOracle", dependencies: ["Lattice", .product(name: "cashew", package: "cashew"), .product(name: "UInt256", package: "UInt256"), .product(name: "Crypto", package: "swift-crypto")]),\n',1);p.write_text(s)
with tempfile.TemporaryDirectory() as d:
 path=pathlib.Path(d)/'input.json'
 with path.open('w') as out:subprocess.run(['cargo','run','--quiet','--locked','--example','merged-template-swift-samples'],cwd=repo,stdout=out,check=True)
 rows=json.loads(path.read_text());results=json.loads(subprocess.check_output(['swift','run','--package-path',str(cache),'--force-resolved-versions','MergedTemplateSwiftOracle',str(path)]))
 for row in rows:
  row['swift']=results[row['name']]
 data=json.dumps(rows,sort_keys=True,indent=2)+'\n';(repo/'tests/merged-template-swift.json').write_text(data)
 (repo/'tests/merged-template-swift-evidence.json').write_text(json.dumps({'node_revision':'3df9e0a786c7b103a3da88d1c3881efc81706255','lattice_revision':'dda2d852429a27be6043599b63bde81530cd9514','dependency_lock_sha256':hashlib.sha256((cache/'Package.resolved').read_bytes()).hexdigest(),'harness_sha256':{p:hashlib.sha256((repo/p).read_bytes()).hexdigest() for p in ['tools/merged-template-swift-vectors.py','examples/merged-template-swift-samples.rs','tests/merged-template-swift-support/mod.rs']},'cases':len(rows),'source_sha256':sources,'corpus_sha256':hashlib.sha256(data.encode()).hexdigest(),'oracle_sha256':hashlib.sha256((repo/'tests/merged-template-swift-oracle.swift').read_bytes()).hexdigest(),'method':'Original full assembly (three unused imports removed); exact extracted policy, digest, CID, minimumWork and atom methods; actual pinned full Lattice libraries. Support-only NodeAPIError declaration. Local direct child candidates have no scheduling witness. Timestamp is previous.timestamp+1; the supplied anchor is the previous header. Observations compare exact block CIDs, selected transaction CIDs, carried child directories and threshold vectors. This verifies assembly and scheduling, not admission or securing proofs.'},indent=2)+'\n')
 mismatches=[(r['name'],r['expected'],r['swift']) for r in rows if r['expected']!=r['swift']]
 print(json.dumps(mismatches,indent=2));assert not mismatches
 print(f'{len(rows)} actual Swift assembly comparisons pass')
