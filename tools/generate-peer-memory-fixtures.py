#!/usr/bin/env python3
"""Execute unchanged pinned PeerBook and PeerMemory methods against real IvyCore."""
import hashlib,json,pathlib,random,subprocess,tempfile
root=pathlib.Path(__file__).resolve().parents[1]
source=root.parent/'lattice-node/Sources/LatticeNode/Networking/PeerMemory.swift'
build=pathlib.Path('/tmp/lattice-node-mixed-build')
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=root.parent/'lattice-node',text=True).strip()=='3df9e0a786c7b103a3da88d1c3881efc81706255'
main=r'''import Foundation
import IvyCore
let rows = try JSONSerialization.jsonObject(with: FileHandle.standardInput.readDataToEndOfFile()) as! [[String: Any]]
var book=PeerBook()
var output:[[String:Any]]=[]
for row in rows {
 let now=(row["now"] as! NSNumber).int64Value
 let keys=Set(row["keys"] as! [String])
 switch row["op"] as! String {
 case "load": book=PeerBook(entries: try JSONDecoder().decode([PeerBook.Entry].self, from: JSONSerialization.data(withJSONObject:row["entries"]!)))
 case "reached":
 let entries=try JSONDecoder().decode([PeerBook.Entry].self, from: JSONSerialization.data(withJSONObject:row["entries"]!))
 book.reached(entries.map(\.endpoint),excluding:keys,now:now)
 case "open":book.stillOpen(keys,now:now)
 case "fail":for key in keys{book.failed(key)}
 default:book.prune(now:now)
 }
 output.append(["entries":try JSONSerialization.jsonObject(with: JSONEncoder().encode(book.entries.values.sorted{$0.publicKey<$1.publicKey})),"groups":(row["hosts"] as! [String]).map(PeerBook.group)])
}
FileHandle.standardOutput.write(try JSONSerialization.data(withJSONObject:output,options:[.sortedKeys]))
'''
rng=random.Random(7133)
def key(n):return (n.to_bytes(4,'big')*8).hex()
def entry(n,host=None,now=0,fail=0):return dict(publicKey=key(n),host=host or f'{n%200+1}.{n//200}.{n%251}.7',port=4001,lastSeen=now,failures=fail)
rows=[]
def add(op,entries=[],keys=[],now=0,hosts=[]):rows.append(dict(op=op,entries=entries,keys=keys,now=now,hosts=hosts))
add('load',[entry(i,now=i) for i in range(1300)])
add('prune',now=1300,hosts=['127.0.0.1','127.001.3.4','+1.2.3.4','-0.2.3.4','[::1]','EXAMPLE.COM','1.2.3.256'])
for i in range(320):
 n=rng.randrange(350);now=i*10000000
 if i%6==0:add('reached',[entry(n,now=now),entry(n+1,'same.example',now)],keys=[key(n+1)] if i%12==0 else [],now=now)
 elif i%6==1:add('open',keys=[key(n),key(n+1)],now=now)
 elif i%6 in (2,3):add('fail',keys=[key(n)],now=now)
 else:add('prune',now=now)
add('load',[entry(1,now=0),entry(2,now=-1)])
add('prune',now=2592000000)
add('prune',now=2592000001)
add('load',[entry(i,'192.168.1.1',now=1)for i in range(15)])
add('prune',now=2)
add('reached',[entry(99,'unknown'),entry(98,'host with space'),entry(97,'x'*254),entry(96,'é.example')])
with tempfile.TemporaryDirectory(prefix='peer-memory-oracle-') as temporary:
 tmp=pathlib.Path(temporary)
 # Only the imported re-export module changes; actual declarations unchanged.
 (tmp/'PeerMemory.swift').write_text(source.read_text().replace('import Ivy\n','import IvyCore\n'))
 (tmp/'main.swift').write_text(main)
 objects=[]
 for name in ['IvyCore','Tally','Crypto']:
  objects.extend(str(p) for p in (build/'debug'/f'{name}.build').glob('*.o'))
 subprocess.run(['swiftc','-I',str(build/'debug/Modules'),'-Xcc','-fmodule-map-file='+str(build/'checkouts/WasmKit/Sources/_CWasmKit/include/module.modulemap'),str(tmp/'PeerMemory.swift'),str(tmp/'main.swift'),*objects,'-o',str(tmp/'oracle')],check=True)
 result=json.loads(subprocess.check_output([str(tmp/'oracle')],input=json.dumps(rows).encode()))
fixture=dict(sourceCommit='3df9e0a786c7b103a3da88d1c3881efc81706255',sourceSHA256=hashlib.sha256(source.read_bytes()).hexdigest(),rows=rows,expected=result)
(root/'tests/fixtures/swift-peer-memory.json').write_text(json.dumps(fixture,indent=2)+'\n')
print(f'{len(rows)} unchanged Swift peer book observations')
