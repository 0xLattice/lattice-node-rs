#!/usr/bin/env python3
"""Actual public HTTP submission, mining, read isolation and durable restart."""
import base64, copy, hashlib, importlib.util, json, pathlib, shutil, subprocess, tempfile, time, urllib.request, urllib.error
ROOT=pathlib.Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('retention',ROOT/'tools/retention-node-integration.py')
retention=importlib.util.module_from_spec(spec);spec.loader.exec_module(retention)
shared=retention.shared

def request(node,path,body=None,cache=None):
    req=urllib.request.Request('http://'+node.ready['publicRead']+path,data=None if body is None else json.dumps(body).encode(),headers={'Content-Type':'application/json','Origin':'https://public-client.example'})
    try:
        with urllib.request.urlopen(req,timeout=10) as response:
            assert response.headers['access-control-allow-origin']=='*'
            if cache is not None: assert response.headers['cache-control']==cache
            return response.status,json.load(response)
    except urllib.error.HTTPError as error:
        assert error.headers.get('cache-control') is None
        return error.code,error.read().decode()

def snapshot():
    result=retention.snapshot()
    path=pathlib.Path(__file__).resolve()
    result[str(path.relative_to(ROOT.parent))]=hashlib.sha256(path.read_bytes()).hexdigest()
    return result

def main():
    out=ROOT/'target/public-node-integration';out.mkdir(parents=True,exist_ok=True)
    sources=snapshot();report={'passed':False,'inputs_sha256':sources,'started_unix':time.time()};processes=[]
    try:
        with (out/'build.log').open('w') as log:
            subprocess.run(['cargo','build','--locked','--bin','lattice-node'],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,check=True)
            subprocess.run(['cargo','build','--locked','-p','lattice-validation','--example','mixed_node_signer','--example','nexus_genesis_volumes'],cwd=shared.WORKSPACE,stdout=log,stderr=subprocess.STDOUT,check=True)
        with tempfile.TemporaryDirectory(prefix='lattice-public-') as temp:
            directory=pathlib.Path(temp)
            node_bin=pathlib.Path(shutil.copy2(ROOT/'target/debug/lattice-node',directory/'node'))
            helper=pathlib.Path(shutil.copy2(shared.WORKSPACE/'target/debug/examples/mixed_node_signer',directory/'signer'))
            bootstrap=pathlib.Path(shutil.copy2(shared.WORKSPACE/'target/debug/examples/nexus_genesis_volumes',directory/'bootstrap'))
            report['binary_sha256']={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [node_bin,helper,bootstrap]}
            with (directory/'bootstrap.json').open('w') as stream:subprocess.run([str(bootstrap)],stdout=stream,check=True)
            identity=json.loads(subprocess.check_output([str(helper)],text=True));owner=identity['address']
            node=shared.Node('public',directory,node_bin,out,processes)
            node.config.update(publicRead='127.0.0.1:0',publicSubmit=True)
            node.start()
            report['funding']=retention.mine(node,helper,directory,owner,1)
            transaction=json.loads(subprocess.check_output([str(helper),'0','3',shared.GENESIS],text=True))
            bad=copy.deepcopy(transaction);bad['transaction']['signatures'][identity['publicKey']]='00'*64
            code,result=request(node,'/transactions',bad);assert code==400,(code,result)
            report['invalid_signature']={'code':code,'body':result}
            code,result=request(node,'/transactions',transaction);assert code==200,(code,result)
            report['submission']=result
            report['inclusion']=retention.mine(node,helper,directory,owner,2)
            cid=report['inclusion']['cid']
            def accounts():
                result={}
                for address in [owner,shared.GENESIS]:
                    code,value=request(node,'/accounts/'+address+'?block='+cid);assert code==200,(code,value);result[address]=value
                return result
            balances=accounts();assert balances[owner]['balance']==str(2*1048576-3) and balances[owner]['nonce']=='1',balances
            assert balances[shared.GENESIS]['balance']=='3',balances
            report['balances']=balances
            immutable='public, max-age=31536000, immutable'
            changing='public, max-age=3'
            report['cache_headers']={}
            for route,cache in [('/api/block/'+cid,immutable),('/api/block/2',changing),('/api/block/latest',changing),('/transactions/'+report['submission']['transactionCID'],immutable),('/api/transaction/'+report['submission']['transactionCID'],changing),('/accounts/'+owner+'?block='+cid,immutable),('/api/state/account/'+owner,changing),('/api/chain/genesis',immutable),('/api/chain/info',changing)]:
                code,value=request(node,route,cache=cache);assert code==200,(route,code,value)
                report['cache_headers'][route]=cache
                if route=='/api/chain/info': assert value['acceptsSubmit'] is True
            for route,body in [('/pins',None),('/status',None),('/mining/templates',{}),('/sweep',{}),('/child-blocks',{})]:
                code,value=request(node,route,body);assert code==404,(route,code,value)
            node.stop();node.config['publicSubmit']=False;node.start();assert accounts()==balances
            report['restart_balances']=accounts()
            code,info=request(node,'/api/chain/info',cache=changing)
            assert code==200 and info['acceptsSubmit'] is False,(code,info)
            code,value=request(node,'/transactions',transaction);assert code==404,(code,value)
            report['public_submit_disabled_after_restart']=True
            host,port=node.ready['overlay'].rsplit(':',1)
            provider=shared.Node('provider',directory,node_bin,out,processes,[{'publicKey':node.ready['publicKey'],'host':host,'port':int(port)}])
            provider.start()
            payload=b'\x67fixture'
            volume_cid='b'+base64.b32encode(bytes([1,0x71,0x12,0x20])+hashlib.sha256(payload).digest()).decode().rstrip('=').lower()
            code,pinned=provider.rpc('/pins',{'cid':volume_cid,'entries':{volume_cid:base64.b64encode(payload).decode()}})
            assert code==200,(code,pinned)
            def fetched():
                req=urllib.request.Request('http://'+node.ready['publicRead']+'/volumes/'+volume_cid)
                try:
                    with urllib.request.urlopen(req,timeout=35) as response:
                        assert response.headers['content-type']=='application/vnd.lattice.volume'
                        assert response.headers['access-control-allow-origin']=='*'
                        return response.read()
                except urllib.error.HTTPError as error:
                    assert error.code in [404,503],error.code
            archive=shared.wait_for(fetched,60)
            for suffix in ['?chainPath=Nexus/Unknown','?chainPath=Nexus//Unknown']:
                try:
                    urllib.request.urlopen('http://'+node.ready['publicRead']+'/volumes/'+volume_cid+suffix,timeout=10)
                    raise AssertionError('unhosted scope unexpectedly served')
                except urllib.error.HTTPError as error:
                    assert error.code==404,error.code

            expected=(1).to_bytes(2,'big')+len(volume_cid).to_bytes(2,'big')+volume_cid.encode()+len(payload).to_bytes(4,'big')+payload
            assert archive==expected
            # Fetch is transient: requester gains no persistent volume/pin.
            import sqlite3
            with sqlite3.connect('file:'+str(directory/'public/volumes.db')+'?mode=ro',uri=True) as db:
                assert db.execute('SELECT COUNT(*) FROM volume_metadata WHERE root=?',(volume_cid,)).fetchone()[0]==0
            report['remote_volume']={'cid':volume_cid,'archive_sha256':hashlib.sha256(archive).hexdigest(),'not_persisted':True}
            provider.stop();node.stop()
            assert snapshot()==sources,'source changed during run'
            report.update(passed=True,completed_unix=time.time())
    except Exception as error:
        report['error']=str(error);raise
    finally:
        for node in processes:
            node.process.kill();node.process.wait();node.log.close()
        (out/'report.json').write_text(json.dumps(report,indent=2)+'\n')
if __name__=='__main__':main()
