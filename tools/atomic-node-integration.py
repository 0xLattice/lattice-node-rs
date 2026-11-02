#!/usr/bin/env python3
"""Actual merged mining: second-level SQL failure emits no success or peer facts."""
import hashlib, json, pathlib, runpy, shutil, sqlite3, subprocess, tempfile, time
ROOT=pathlib.Path(__file__).resolve().parents[1]
h=runpy.run_path(str(ROOT/'tools/child-node-integration.py'))
Node,wait_for,GENESIS,CHILD=h['Node'],h['wait_for'],h['GENESIS'],h['CHILD']
def snapshot():
    values=h['snapshot']();values[str(pathlib.Path(__file__).relative_to(ROOT.parent))]=hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest();return values

def main():
    out=ROOT/'target/atomic-node-integration';out.mkdir(parents=True,exist_ok=True)
    before=snapshot();report={'passed':False,'inputs_sha256':before};processes=[]
    try:
        with (out/'build.log').open('w') as log:
            subprocess.run(['cargo','build','--locked','--bin','lattice-node'],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,check=True)
            subprocess.run(['cargo','build','--locked','-p','lattice-validation','--example','mixed_node_signer','--example','nexus_genesis_volumes'],cwd=ROOT.parent/'lattice-rs',stdout=log,stderr=subprocess.STDOUT,check=True)
        with tempfile.TemporaryDirectory(prefix='lattice-atomic-') as temp:
            directory=pathlib.Path(temp)
            node_bin=pathlib.Path(shutil.copy2(ROOT/'target/debug/lattice-node',directory/'node'))
            miner=pathlib.Path(shutil.copy2(ROOT.parent/'lattice-rs/target/debug/examples/mixed_node_signer',directory/'miner'))
            bootstrap=pathlib.Path(shutil.copy2(ROOT.parent/'lattice-rs/target/debug/examples/nexus_genesis_volumes',directory/'bootstrap'))
            report['binary_sha256']={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [node_bin,miner,bootstrap]}
            with (directory/'bootstrap.json').open('w') as stream:subprocess.run([str(bootstrap)],stdout=stream,check=True)
            owner=json.loads(subprocess.check_output([str(miner)],text=True))['address']
            spec=directory/'child.json';spec.write_text(json.dumps({'maxNumberOfTransactionsPerBlock':1000,'maxStateGrowth':100000,'maxBlockSize':1000000,'premine':1,'targetBlockTime':1000,'initialReward':10,'halvingInterval':1000,'halfLife':120,'wasmPolicies':[]}))
            producer=Node('producer',directory,node_bin,out,processes);producer.start();producer.host(spec,owner)
            host,port=producer.ready['overlay'].rsplit(':',1)
            follower=Node('follower',directory,node_bin,out,processes,[{'publicKey':producer.ready['publicKey'],'host':host,'port':int(port)}]);follower.start();follower.host()
            def prepare():
                code,template=producer.rpc('/mining/templates',{'recipients':[{'chainPath':['Nexus'],'address':GENESIS},{'chainPath':CHILD,'address':owner}]});assert code==200,(code,template)
                template['searchTarget']=min(template['targets'],key=lambda s:int(s,16))
                path=directory/'template.json';path.write_text(json.dumps(template))
                work=json.loads(subprocess.check_output([str(miner),'--mine',str(path)],text=True))
                return {'workID':template['workID'],'nonce':work['nonce']}
            work=prepare()
            db_path=directory/'producer/state.db'
            with sqlite3.connect(db_path) as db:
                baseline=db.execute('SELECT COUNT(*) FROM admission_batches').fetchone()[0]
                db.execute("CREATE TRIGGER fail_child_turn BEFORE INSERT ON admission_batches WHEN NEW.chain_path='Nexus/Alpha' BEGIN SELECT RAISE(ABORT,'injected second-level failure'); END")
            try:
                code,result=producer.rpc('/mining/work',work)
                assert code!=200,(code,result)
                report['failure_response']=code
            except (OSError,ValueError) as error:
                report['failure_response']=type(error).__name__
            result=producer.process.wait(timeout=15);assert result!=0,result
            producer.log.close();processes.remove(producer)
            with sqlite3.connect(db_path) as db:
                assert db.execute('SELECT COUNT(*) FROM admission_batches').fetchone()[0]==baseline
                assert db.execute("SELECT COUNT(*) FROM child_proofs").fetchone()[0]==0
                db.execute('DROP TRIGGER fail_child_turn')
            assert follower.state()['height']=='0'
            assert follower.state(True)['phase']=='awaitingGenesis'
            report['failed_turn_escaped_neither_reply_nor_peer_facts']=True
            producer.start();producer.host(spec,owner)
            assert producer.state()['height']=='0' and producer.state(True)['phase']=='awaitingGenesis'
            code,accepted=producer.rpc('/mining/work',prepare());assert code==200 and accepted['accepted'],(code,accepted)
            assert accepted['durableChildProofs'],accepted
            wait_for(lambda:producer.state(True).get('height')=='0')
            report['successful_turn']=accepted
            producer.stop();producer.start();producer.host()
            assert producer.state()['height']=='1' and producer.state(True)['height']=='0'
            report['recovered_both_levels']=True
            producer.stop();follower.stop()
            assert snapshot()==before,'source changed during guarded run'
            report['passed']=True
    except Exception as error:
        report['error']=str(error);raise
    finally:
        for node in processes:
            node.process.kill();node.process.wait();node.log.close()
        (out/'report.json').write_text(json.dumps(report,indent=2)+'\n')
if __name__=='__main__':main()
