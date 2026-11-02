#!/usr/bin/env python3
import pathlib,json,random,subprocess,sys,tempfile
repo=pathlib.Path(__file__).resolve().parents[1];cache=pathlib.Path('/tmp/lattice-genesis-oracle');target=cache/'Sources/MempoolOracle';target.mkdir(exist_ok=True)
(target/'Mempool.swift').write_bytes((repo.parent/'lattice-node/Sources/LatticeNodeCore/Mempool/Mempool.swift').read_bytes())
(target/'Oracle.swift').write_bytes((repo/'tools/MempoolOracle.swift').read_bytes())
p=cache/'Package.swift';s=p.read_text()
if 'name: "MempoolOracle"' not in s:s=s.replace('    targets: [\n','    targets: [\n        .executableTarget(name: "MempoolOracle", dependencies: ["Lattice", .product(name: "cashew", package: "cashew"), .product(name: "UInt256", package: "UInt256")]),\n',1);p.write_text(s)
def body(owner,nonce,amount,kind=0):
 b=dict(accountActions=[dict(owner=owner,delta=-amount)] if amount else [],actions=[],depositActions=[],receiptActions=[],withdrawalActions=[],signers=[owner],nonce=nonce,chainPath=['Nexus'])
 if kind==1:b['actions']=[dict(key='shared',newValue='v'+str(nonce))]
 if kind==2:b['depositActions']=[dict(demander=owner,amountDemanded=7,amountDeposited=amount//2,nonce=0)]
 if kind==3:b['receiptActions']=[dict(withdrawer=owner,demander='payee',directory='child',amountDemanded=4,nonce=0)]
 if kind==5:b['withdrawalActions']=[dict(withdrawer=owner,demander='payee',amountDemanded=7,amountWithdrawn=30,nonce=0)]
 if kind==4:b['actions']=[dict(key='large'+str(nonce),newValue='v'*800)]
 return b
cases=[]
# Refuse a cache with altered consensus sources or resolved dependencies.
import hashlib
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo.parent/'lattice-node',text=True).strip()=='3df9e0a786c7b103a3da88d1c3881efc81706255'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo.parent/'Lattice',text=True).strip()=='dda2d852429a27be6043599b63bde81530cd9514'
assert hashlib.sha256((cache/'Package.resolved').read_bytes()).hexdigest()=='958f69bd3ec734a372e8cd94e3d09d593674a1025979a2abe2b45a6f08cfc9a6'
for original in (repo.parent/'Lattice/Sources').rglob('*.swift'):
 assert (cache/original.relative_to(repo.parent/'Lattice')).read_bytes()==original.read_bytes()

for mode in range(8):
 rng=random.Random(900+mode);ops=[];spec=dict(maxNumberOfTransactionsPerBlock=[1,2,5,100][mode%4],maxStateGrowth=3000,maxBlockSize=6000,premine=0,targetBlockTime=1000,initialReward=1024,halvingInterval=210000,halfLife=120,wasmPolicies=[])
 for i in range(90):
  if i%23==22:ops.append(dict(op='rebalance',balances={'a':rng.randrange(250),'b':rng.randrange(250),'c':rng.randrange(250)}));continue
  if i%19==18:ops.append(dict(op='remove',remove=[rng.randrange(i)]));continue
  if i==70:ops.append(dict(op='clear'));continue
  owner=rng.choice(['a','b','c']);b=body(owner,rng.randrange(8),rng.randrange(1,130),rng.randrange(6));ops.append(dict(op='submit',body=b,at=rng.randrange(12),balances={'a':200,'b':220,'c':240}))
 cases.append(dict(name=f'pool-{mode}',spec=spec,limits=dict(count=[2,5,20,100][mode%4],bytes=15000,signatures=64,floor=mode%3),operations=ops))
with tempfile.TemporaryDirectory() as directory:
 input=pathlib.Path(directory)/'input.json';input.write_text(json.dumps(cases));result=subprocess.check_output(['swift','run','--package-path',str(cache),'--force-resolved-versions','MempoolOracle',str(input)])
 (repo/'tests/fixtures/swift-mempool.json').write_text(json.dumps(json.loads(result),sort_keys=True,indent=2)+'\n');print('720 actual Swift pool operations')
