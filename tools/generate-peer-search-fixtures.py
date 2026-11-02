#!/usr/bin/env python3
"""Compile unchanged PeerSearch.swift against the pinned full node build modules."""
import hashlib,json,pathlib,random,subprocess,sys,tempfile
root=pathlib.Path(__file__).resolve().parents[1]
source=root.parent/'lattice-node/Sources/LatticeNode/Runtime/PeerSearch.swift'
build=pathlib.Path(sys.argv[1] if len(sys.argv)>1 else '/tmp/lattice-node-mixed-build')
main='''import Foundation
import LatticeNodeCore
let rows = try JSONSerialization.jsonObject(with: FileHandle.standardInput.readDataToEndOfFile()) as! [[String: Any]]
var search = PeerSearch()
var output: [[String: Any]] = []
for row in rows {
 let heights = (row["heights"] as! [[String: Any]]).map { (path: $0["path"] as! [String], height: ($0["height"] as! NSNumber).uint64Value) }
 let now = (row["now"] as! NSNumber).int64Value
 let interval = (row["interval"] as! NSNumber).int64Value
 let connected = row["connected"] as! Bool
 let due = search.due(heights: heights, now: now, interval: interval, connected: connected)
 let top = search.topUps(paths: row["paths"] as! [[String]], now: now, interval: interval, connected: connected).sorted { $0.lexicographicallyPrecedes($1) }
 output.append(["due": due, "topUps": top])
}
FileHandle.standardOutput.write(try JSONSerialization.data(withJSONObject: output, options: [.sortedKeys]))
'''
rng=random.Random(55137);paths=[['Nexus'],['Nexus','Alpha'],['Nexus','Beta'],['Nexus','Alpha','Leaf']];rows=[];now=0;heights=[0]*4
for index in range(512):
 now+=rng.choice([-5,0,1,10,99,100,101,600000])
 selected=rng.sample(range(4),rng.randrange(1,5))
 for i in selected:heights[i]=max(0,heights[i]+rng.choice([-2,-1,0,0,1,3]))
 rows.append(dict(heights=[dict(path=paths[i],height=heights[i])for i in selected],paths=rng.sample(paths,rng.randrange(5)),now=now,interval=rng.choice([-1,0,1,100,600000]),connected=rng.choice([True,True,False])))
with tempfile.TemporaryDirectory(prefix='lattice-peer-search-') as tmp:
 tmp=pathlib.Path(tmp);(tmp/'PeerSearch.swift').write_bytes(source.read_bytes());(tmp/'main.swift').write_text(main)
 subprocess.run(['swiftc','-I',str(build/'debug/Modules'),'-Xcc','-fmodule-map-file='+str(build/'checkouts/WasmKit/Sources/_CWasmKit/include/module.modulemap'),str(tmp/'PeerSearch.swift'),str(tmp/'main.swift'),'-o',str(tmp/'oracle')],check=True)
 output=json.loads(subprocess.check_output([str(tmp/'oracle')],input=json.dumps(rows).encode()))
fixture=dict(sourceCommit='3df9e0a786c7b103a3da88d1c3881efc81706255',sourceSHA256=hashlib.sha256(source.read_bytes()).hexdigest(),rows=rows,expected=output)
(root/'tests/fixtures/swift-peer-search.json').write_text(json.dumps(fixture,indent=2)+'\n')
print(f'{len(rows)} actual unchanged Swift scheduler observations')
