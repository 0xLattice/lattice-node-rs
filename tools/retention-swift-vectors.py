#!/usr/bin/env python3
"""Actual pinned private Swift TipStateWalk + children traversal; local adapters."""
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
sources={}
def source(path):
 p=reference/path;sources[path]=hashlib.sha256(p.read_bytes()).hexdigest();return p.read_text()
retention=source('Sources/LatticeNode/Runtime/NodeStorage+Retention.swift');effects=source('Sources/LatticeNode/Runtime/NodeStorage+RuntimeEffects.swift')
target=cache/'Sources/RetentionSwiftOracle';target.mkdir(exist_ok=True)
s='import Foundation\nimport Lattice\nimport cashew\ntypealias ChainPath = [String]\nstruct NodeStorage:Sendable {let store:CountStore;let localFetcher:any Fetcher\n'
s+=extract(effects,'    static func children(of header: any Header)')+'\n'+extract(effects,'    private static func loadedNode<H: Header>')+'\n}\n'
s+=extract(retention,'private actor TipStateWalk {')+'\n'+(repo/'tests/retention-swift-oracle.swift').read_text()
(target/'Oracle.swift').write_text(s)
p=cache/'Package.swift';s=p.read_text()
if 'name: "RetentionSwiftOracle"' not in s:s=s.replace('    targets: [\n','    targets: [\n        .executableTarget(name: "RetentionSwiftOracle", dependencies: ["Lattice", .product(name: "cashew", package: "cashew")]),\n',1);p.write_text(s)
with tempfile.TemporaryDirectory() as d:
 path=pathlib.Path(d)/'input.json'
 with path.open('w') as out:subprocess.run(['cargo','run','--quiet','--locked','--example','retention-swift-samples'],cwd=repo,stdout=out,check=True)
 rows=json.loads(path.read_text());results=json.loads(subprocess.check_output(['swift','run','--package-path',str(cache),'--force-resolved-versions','RetentionSwiftOracle',str(path)]))
 for row in rows:row['swift']=results[row['name']]
 data=json.dumps(rows,sort_keys=True,indent=2)+'\n';(repo/'tests/retention-swift.json').write_text(data)
 evidence={'node_revision':'3df9e0a786c7b103a3da88d1c3881efc81706255','lattice_revision':'dda2d852429a27be6043599b63bde81530cd9514','cases':len(rows),'source_sha256':sources,'dependency_lock_sha256':hashlib.sha256((cache/'Package.resolved').read_bytes()).hexdigest(),'harness_sha256':{p:hashlib.sha256((repo/p).read_bytes()).hexdigest() for p in ['tools/retention-swift-vectors.py','examples/retention-swift-samples.rs','tests/retention-swift-support/mod.rs','tests/retention-swift-oracle.swift']},'corpus_sha256':hashlib.sha256(data.encode()).hexdigest(),'method':'Exact private TipStateWalk actor and NodeStorage.children/loadedNode methods from pinned node, unchanged pinned Lattice/cashew. NodeStorage adapter supplies an in-memory CountStore.stateCount and InMemoryContentSource only. Inputs use raw unresolved root headers. Compares complete signed changes dictionary including zero deltas and sorted entering CIDs. No broker pins, transactions or physical eviction exercised.'}
 (repo/'tests/retention-swift-evidence.json').write_text(json.dumps(evidence,indent=2)+'\n')
 mismatches=[(r['name'],r['expected'],r['swift']) for r in rows if r['expected']!=r['swift']]
 print(json.dumps(mismatches,indent=2));assert not mismatches
 print(f'{len(rows)} actual Swift retention comparisons pass')
