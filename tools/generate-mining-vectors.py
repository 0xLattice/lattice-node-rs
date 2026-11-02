#!/usr/bin/env python3
import pathlib,json,random,subprocess,base64,tempfile
repo=pathlib.Path(__file__).resolve().parents[1];cache=pathlib.Path('/tmp/lattice-genesis-oracle');target=cache/'Sources/MiningOracle';target.mkdir(exist_ok=True)
import hashlib
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo.parent/'lattice-node',text=True).strip()=='3df9e0a786c7b103a3da88d1c3881efc81706255'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo.parent/'Lattice',text=True).strip()=='dda2d852429a27be6043599b63bde81530cd9514'
assert hashlib.sha256((cache/'Package.resolved').read_bytes()).hexdigest()=='958f69bd3ec734a372e8cd94e3d09d593674a1025979a2abe2b45a6f08cfc9a6'
for original in (repo.parent/'Lattice/Sources').rglob('*.swift'):
 assert (cache/original.relative_to(repo.parent/'Lattice')).read_bytes()==original.read_bytes()
for name,path in {'MiningState':'Mining/MiningState.swift','Mempool':'Mempool/Mempool.swift','WorkTemplateBook':'Mining/WorkTemplateBook.swift','SyncProtocol':'Sync/SyncProtocol.swift'}.items():(target/(name+'.swift')).write_bytes((repo.parent/'lattice-node/Sources/LatticeNodeCore'/path).read_bytes())
(target/'Oracle.swift').write_bytes((repo/'tools/MiningOracle.swift').read_bytes());p=cache/'Package.swift';s=p.read_text()
if 'name: "MiningOracle"' not in s:s=s.replace('    targets: [\n','    targets: [\n        .executableTarget(name: "MiningOracle", dependencies: ["Lattice", .product(name: "cashew", package: "cashew"), .product(name: "UInt256", package: "UInt256")]),\n',1);p.write_text(s)
case=json.load(open(repo/'tests/fixtures/swift-mempool.json'))[0]['scenario'];bodies=[op['body'] for op in case['operations'] if op['op']=='submit'][:6]
wire=json.load(open(repo/'tests/fixtures/swift-wire.json'));row=next(r for r in wire if r['name']=='valid-5');block=json.loads(base64.b64decode(row['payload']))['entries'][0]['block']
commands=[]
def add(op,index=0,id=0,value=None):commands.append(dict(op=op,index=index,id=id,value=value,now=2000+len(commands)*100))
for args in [('receive',0,1,'peer'),('receive',0,2,'peer'),('receive',1,3,'peer'),('receive',0,4,'local'),('receive',1,5,'submitted'),('receive',2,6,'submitted'),('verdict',0,0,'ready'),('tip',0,0,'B'),('tip',0,0,'A'),('verdict',0,0,'invalid'),('template',0,7,'miner'),('template',0,8,'miner'),('built',0,0,'ready'),('submit',0,9,None)]:add(*args)
rng=random.Random(1701)
for i in range(300):
 op=rng.choice(['receive']*4+['verdict']*3+['tip','confirm','template','built','submit','refuse'])
 value={'receive':rng.choice(['local','peer','submitted','restored','returned']),'verdict':rng.choice(['ready','future','invalid','unavailable']),'tip':rng.choice(['A','B']),'confirm':rng.choice(['A','B']),'template':rng.choice(['miner','other']),'built':rng.choice(['ready','fail']),'refuse':rng.choice(['A','B'])}.get(op)
 add(op,rng.randrange(6 if op in ['receive','confirm','refuse'] else 50),i+10,value)
data=dict(spec=case['spec'],bodies=bodies,commands=commands,block=block)
with tempfile.TemporaryDirectory() as directory:
 path=pathlib.Path(directory)/'input.json';path.write_text(json.dumps(data));observed=json.loads(subprocess.check_output(['swift','run','--package-path',str(cache),'--force-resolved-versions','MiningOracle',str(path)]));(repo/'tests/fixtures/swift-mining.json').write_text(json.dumps(dict(input=data,observations=observed),sort_keys=True,indent=2)+'\n');print(len(observed),'actual Swift mining reducer turns')
