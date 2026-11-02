#!/usr/bin/env python3
"""Compare actual pinned Swift storeBlock selected roots with production Rust."""
import hashlib,json,pathlib,subprocess
repo=pathlib.Path(__file__).resolve().parents[1]
reference=repo.parent/'Lattice'
cache=pathlib.Path('/tmp/lattice-genesis-oracle')
pin='dda2d852429a27be6043599b63bde81530cd9514'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=reference,text=True).strip()==pin
assert not subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],cwd=reference)
for original in (reference/'Sources').rglob('*.swift'):
 assert original.read_bytes()==(cache/original.relative_to(reference)).read_bytes()
assert hashlib.sha256((cache/'Package.resolved').read_bytes()).hexdigest()=='958f69bd3ec734a372e8cd94e3d09d593674a1025979a2abe2b45a6f08cfc9a6'
target=cache/'Sources/ServingBundleOracle';target.mkdir(exist_ok=True)
(target/'Oracle.swift').write_bytes((repo/'tests/serving-bundle-swift-oracle.swift').read_bytes())
p=cache/'Package.swift';s=p.read_text()
if 'name: "ServingBundleOracle"' not in s:
 s=s.replace('    targets: [\n','    targets: [\n        .executableTarget(name: "ServingBundleOracle", dependencies: ["Lattice", .product(name: "cashew", package: "cashew")]),\n',1);p.write_text(s)
input=repo/'target/serving-bundle-input.json'
with input.open('w') as out:subprocess.run(['cargo','run','--offline','--locked','--quiet','--example','serving-bundle-samples'],cwd=repo,stdout=out,check=True)
rows=json.loads(input.read_text())
actual=json.loads(subprocess.check_output(['swift','run','--package-path',str(cache),'--force-resolved-versions','ServingBundleOracle',str(input)]))
for row in rows:row['swift']=actual[row['name']]
corpus=[{k:v for k,v in row.items() if k not in ['entries','expected']} for row in rows]
(repo/'tests/serving-bundle-swift.json').write_text(json.dumps(corpus,sort_keys=True,separators=(',',':'))+'\n')
paths=['Sources/LatticePrimitives/Block/Block+Content.swift']
report={'latticeRevision':pin,'cases':len(rows),'scope':'Original full pinned BlockHeader.storeBlock selected root sets; local source only, scheduling and cache excluded','sourceSha256':{p:hashlib.sha256((reference/p).read_bytes()).hexdigest() for p in paths},'harnessSha256':{p:hashlib.sha256((repo/p).read_bytes()).hexdigest() for p in ['examples/serving-bundle-samples.rs','tests/serving-bundle-swift-oracle.swift','tools/serving-bundle-swift-check.py']},'corpusSha256':hashlib.sha256((repo/'tests/serving-bundle-swift.json').read_bytes()).hexdigest()}
(repo/'tests/serving-bundle-swift-evidence.json').write_text(json.dumps(report,indent=2)+'\n')
mismatches=[r['name'] for r in rows if r['expected']!=r['swift']]
print(json.dumps({'cases':len(rows),'mismatches':mismatches}));assert not mismatches
