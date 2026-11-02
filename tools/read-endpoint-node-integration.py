#!/usr/bin/env python3
"""Actual localhost overlay read-endpoint declarations across hosted levels."""
import hashlib,json,pathlib,runpy,shutil,subprocess,tempfile,time
ROOT=pathlib.Path(__file__).resolve().parents[1]
shared=runpy.run_path(str(ROOT/'tools/child-node-integration.py'))
Node=shared['Node'];wait_for=shared['wait_for'];WORKSPACE=shared['WORKSPACE']

def snapshot():
    result=shared['snapshot']()
    p=pathlib.Path(__file__)
    result[str(p.relative_to(ROOT.parent))]=hashlib.sha256(p.read_bytes()).hexdigest()
    return result

def main():
    out=ROOT/'target/read-endpoint-node-integration';out.mkdir(parents=True,exist_ok=True)
    report={'passed':False,'started_unix':time.time()};sources=snapshot();report['inputs_sha256']=sources
    processes=[]
    def save():
        path=out/'report.tmp';path.write_text(json.dumps(report,indent=2)+'\n');path.replace(out/'report.json')
    save()
    try:
        with (out/'build.log').open('w') as log:
            subprocess.run(['cargo','build','--locked','--bin','lattice-node'],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,check=True)
            subprocess.run(['cargo','build','--locked','-p','lattice-validation','--example','nexus_genesis_volumes'],cwd=WORKSPACE,stdout=log,stderr=subprocess.STDOUT,check=True)
        with tempfile.TemporaryDirectory(prefix='lattice-read-endpoint-') as d:
            tmp=pathlib.Path(d);node=pathlib.Path(shutil.copy2(ROOT/'target/debug/lattice-node',tmp/'node'));bootstrap=pathlib.Path(shutil.copy2(WORKSPACE/'target/debug/examples/nexus_genesis_volumes',tmp/'bootstrap'))
            report['binary_sha256']={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [node,bootstrap]}
            with (tmp/'bootstrap.json').open('w') as f:subprocess.run([bootstrap],stdout=f,check=True)
            url='https://tree-host.example/base'
            tree=Node('tree',tmp,node,out,processes);tree.config.update(publicReadUrl=url,publicSubmit=True,publicRead="127.0.0.1:0",peerSearchInterval=1,announceLifetime=2);tree.start()
            code,value=tree.rpc('/hosted-chains',{'hostChain':['Nexus/Alpha','Nexus/Alpha/Beta']});assert code==200,(code,value)
            host,port=tree.ready['overlay'].rsplit(':',1);seeds=[{'publicKey':tree.ready['publicKey'],'host':host,'port':int(port)}]
            root=Node('root',tmp,node,out,processes,seeds);root.start()
            follower=Node('follower',tmp,node,out,processes,seeds);follower.start();follower.host()
            def lookup(n,path):return n.rpc('/api/chain/endpoints?chainPath='+path)
            def found(n,path):
                code,value=lookup(n,path)
                report.setdefault('last_observations',{})[n.name]={'code':code,'value':value,'peers':n.rpc('/api/peers')}
                report['tree_peers']=tree.rpc('/api/peers')
                save()
                return value if code==200 and value['endpoints']==[url] else None
            own=wait_for(lambda:found(tree,'Nexus/Alpha/Beta'),20)
            through_root=wait_for(lambda:found(root,'Nexus/Alpha'),30)
            through_parent=wait_for(lambda:found(follower,'Nexus/Alpha/Beta'),30)
            for value in [own,through_root,through_parent]:
                assert value['submitEndpoints']==[url] and 'committedBlock' not in value,value
            # Declarations exist without any child block. A root-only host
            # cannot answer a grandchild discovery route.
            code,value=lookup(root,'Nexus/Alpha/Beta');assert code==404,(code,value)
            code,unknown=lookup(follower,'Nexus/Alpha/Unknown');assert code==404,(code,unknown)
            code,invalid=lookup(root,'Nexus');assert code==400,(code,invalid)
            start=time.monotonic();code,cached=lookup(root,'Nexus/Alpha');assert code==200 and cached==through_root
            report['cached_seconds']=time.monotonic()-start
            assert report['cached_seconds']<1
            # Ivy's default netgroup limit permits two peers from this loopback
            # group. Rotate the root client instead of overriding that policy.
            root.stop()
            decoy=Node('decoy',tmp,node,out,processes,seeds);decoy.config['publicReadUrl']='https://not-a-host.example';decoy.start()
            through_decoy=wait_for(lambda:found(decoy,'Nexus/Alpha'),30)
            assert through_decoy['submitEndpoints']==[url]
            report.update(own=own,through_root=through_root,through_parent=through_parent,through_decoy=through_decoy,unknown=unknown,invalid=invalid)
            decoy.stop()
            root.start()
            restored=wait_for(lambda:found(root,'Nexus/Alpha'),30)
            assert restored==through_root;report['restart']=restored
            for n in list(processes):n.stop()
            current=snapshot();assert current==sources,('source changed during run',[p for p in sources.keys()|current.keys() if sources.get(p)!=current.get(p)])
            report.update(passed=True,completed_unix=time.time())
    except Exception as error:
        report['error']=str(error)
        for n in processes:
            try:report.setdefault('peers_on_failure',{})[n.name]=n.rpc('/api/peers')
            except Exception:pass
        raise
    finally:
        try:
            for n in list(processes):
                if n.process.poll() is None:n.stop()
                else:n.log.close();processes.remove(n)
        finally:save()
if __name__=='__main__':main()
