#!/usr/bin/env python3
"""Build and run actual pinned Swift child-only peer memory on loopback."""
import argparse,hashlib,json,pathlib,socket,subprocess,tempfile,time
root=pathlib.Path(__file__).resolve().parents[1]
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--swift-build',type=pathlib.Path,default=pathlib.Path('/tmp/lattice-node-mixed-build'))
args=parser.parse_args();build=args.swift_build
reference=root.parent/'lattice-node'; pin='3df9e0a786c7b103a3da88d1c3881efc81706255'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=reference,text=True).strip()==pin
assert not subprocess.check_output(['git','diff','--name-only','--','Sources'],cwd=reference,text=True).strip()
out=root/'target/child-peer-memory-swift';out.mkdir(parents=True,exist_ok=True)
with (out/'build.log').open('w') as log:
 subprocess.run(['swift','build','--package-path',str(reference),'--scratch-path',str(build),'--product','lattice-node','--disable-automatic-resolution'],stdout=log,stderr=subprocess.STDOUT,check=True)
objects=[p for p in (build/'debug/lattice-node.product/Objects.LinkFileList').read_text().splitlines() if '/LatticeNodeDaemon.build/' not in p]
modulemaps=[p for p in (build/'debug').glob('*.build/module.modulemap')]
modulemaps += [p for p in (build/'checkouts').glob('*/Sources/*/include/module.modulemap') if p.parent.parent.name not in {q.parent.name.removesuffix('.build') for q in modulemaps}]
source=root/'tools/ChildPeerMemoryOracle.swift'
report={'passed':False,'node_revision':pin,'started_unix':time.time(),'source_sha256':{str(p.relative_to(reference)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [reference/'Sources/LatticeNode/Networking/PeerMemory.swift',reference/'Sources/LatticeNode/Runtime/NodeRuntime.swift']},'harness_sha256':hashlib.sha256(source.read_bytes()).hexdigest()}
try:
 with tempfile.TemporaryDirectory(prefix='child-peer-memory-') as temporary:
  temporary=pathlib.Path(temporary);binary=temporary/'oracle';response=temporary/'objects.txt';response.write_text('\n'.join(objects))
  command=['swiftc','-parse-as-library','-I',str(build/'debug/Modules')]
  for path in modulemaps:command+=['-Xcc','-fmodule-map-file='+str(path)]
  command += [str(source),'@'+str(response),'-lsqlite3','-lz','-o',str(binary)]
  with (out/'compile.log').open('w') as log:subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,check=True)
  report['binary_sha256']=hashlib.sha256(binary.read_bytes()).hexdigest()
  leases=[]
  for _ in range(4):
   lease=socket.socket();lease.bind(('127.0.0.1',0));leases.append(lease)
  ports=[str(lease.getsockname()[1]) for lease in leases]
  for lease in leases:lease.close()
  with (out/'run.log').open('w') as log:subprocess.run([str(binary),str(temporary/'data'),*ports],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=90)
  rows=[json.loads(line) for line in (out/'run.log').read_text().splitlines() if line.startswith('{')]
  report['observations']=rows[-1];assert rows[-1]['passed']
  report.update(passed=True,completed_unix=time.time())
finally:
 (out/'report.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report['observations']))
