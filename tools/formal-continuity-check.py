#!/usr/bin/env python3
"""Finite production anchor-decision refinement against unchanged Hierarchy.tla.
The model's state==block abstraction is used only for distinct changing states;
no second continuity oracle is implemented in Python.
"""
import copy,json,os,pathlib,shutil,subprocess,sys,tempfile
ROOT=pathlib.Path(__file__).resolve().parent.parent
FORMAL=pathlib.Path(sys.argv[1] if len(sys.argv)>1 else ROOT.parent/'lattice-formal').resolve()
PIN='a9f4c2879b6565ae11c17d35efe14475c5830978'
if subprocess.check_output(['git','rev-parse','HEAD'],cwd=FORMAL,text=True).strip()!=PIN:sys.exit('formal revision mismatch')
if subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],cwd=FORMAL):sys.exit('formal tracked sources modified')
JAVA=os.environ.get('JAVA') or ('/opt/homebrew/opt/openjdk/bin/java' if pathlib.Path('/opt/homebrew/opt/openjdk/bin/java').exists() else 'java')
def atom(x):return json.dumps(x) if isinstance(x,str) else str(x)
def aset(xs):return '{'+','.join(atom(x) for x in xs)+'}'
def fn(xs):return '('+' @@ '.join(atom(k)+' :> '+atom(v) for k,v in xs.items())+')'
def check(trace,negative=False):
    parents=trace['parents'].copy();anchors={k:'none' for k in parents};children=[];rows=[]
    for i,row in enumerate(trace['rows']):
        block=('genesis' if row['genesis'] else row['old'])+'_'+row['new'];pred='old_'+row['old'];children.extend([block,pred]);parents[pred]='none';parents[block]='none' if row['genesis'] else pred
        anchors[pred]=row['old'];anchors[block]=row['new']
        rows.append('[block |-> '+atom(block)+', executed |-> '+aset(row['validated'])+', outcome |-> '+atom(row['outcome'])+']')
    model=r'''---- MODULE MCProductionContinuity ----
EXTENDS Hierarchy, TLC, Sequences
VARIABLE i
TP == @P@
TC == @C@
TAll == TP \cup TC
TParent == @PARENT@
TAnchor == @ANCHOR@
TRank == @RANK@
TValid == [b \in TAll |-> TRUE]
TEmpty == {}
Rows == <<@ROWS@>>
ObservedExecuted(n) == [ch \in Chains |-> IF ch = "P" THEN Rows[n].executed ELSE {}]
ProductionInit == /\ i = 1
                  /\ delivered = {}
                  /\ loc = [ch \in Chains |-> [g \in {} |-> None]]
                  /\ qty = [ch \in Chains |-> [g \in {} |-> 0]]
                  /\ weighed = [ch \in Chains |-> ChainBlocks(ch)]
                  /\ validated = ObservedExecuted(1)
                  /\ excluded = [ch \in Chains |-> {}]
ProductionNext == \/ /\ i < Len(Rows) /\ i' = i + 1
                     /\ validated' = ObservedExecuted(i + 1)
                     /\ UNCHANGED <<delivered,loc,qty,weighed,excluded>>
                  \/ /\ i = Len(Rows) /\ UNCHANGED <<vars,i>>
ContinuityMatches == Continuity(Rows[i].block) = Rows[i].outcome
====
'''
    values={'@P@':aset(trace['parents']),'@C@':aset(sorted(set(children))),'@PARENT@':fn(parents),'@ANCHOR@':fn(anchors),'@RANK@':fn({k:i for i,k in enumerate(parents)}),'@ROWS@':',\n'.join(rows)}
    for key,value in values.items():model=model.replace(key,value)
    cfg='''INIT ProductionInit
NEXT ProductionNext
CONSTANTS
 None = "none"
 PBlocks <- TP
 CBlocks <- TC
 GBlocks <- TEmpty
 Parent <- TParent
 CidRank <- TRank
 Grinds <- TEmpty
 Obs <- TEmpty
 Commits <- TEmpty
 Anchor <- TAnchor
 Valid <- TValid
 Available <- TAll
 HeaderBad <- TEmpty
 PrevState <- TParent
 ExpectedTips = 0
 ExpectedChildWork = 0
INVARIANT ContinuityMatches
'''
    with tempfile.TemporaryDirectory(prefix='lattice-continuity-tlc-') as directory:
        work=pathlib.Path(directory);shutil.copy(FORMAL/'specs/Hierarchy.tla',work)
        (work/'MCProductionContinuity.tla').write_text(model);(work/'MCProductionContinuity_Run.cfg').write_text(cfg)
        result=subprocess.run([str(FORMAL/'scripts/tlc.sh'),'MCProductionContinuity_Run.cfg',str(work)],env={**os.environ,'JAVA':JAVA},capture_output=True,text=True)
        output=result.stdout+result.stderr
        if negative:
            if 'Invariant ContinuityMatches is violated' not in output:sys.exit('negative control did not reject:\n'+output)
        elif 'No error has been found' not in output:sys.exit(output)
trace=json.loads(subprocess.check_output(['cargo','run','--quiet','--example','continuity_trace'],cwd=ROOT,text=True))
check(trace);print(f"Hierarchy.tla: {len(trace['rows'])} production continuity decisions conform",flush=True)
controls=[('unproduced equality',lambda r:r['old']==r['new']=='u' and not r['genesis'],'proven'),
 ('sideways anchor',lambda r:r['old']=='a' and r['new']=='b' and len(r['validated'])==5,'proven'),
 ('backward anchor',lambda r:r['old']=='aa' and r['new']=='a' and len(r['validated'])==5,'proven'),
 ('empty genesis',lambda r:r['genesis'] and r['new']=='none','proven'),
 ('noncanonical exclusion',lambda r:r['old']=='a' and r['new']=='aa' and len(r['validated'])==5,'held')]
for label,predicate,bad in controls:
    mutated=copy.deepcopy(trace);row=next(r for r in mutated['rows'] if predicate(r));assert row['outcome']!=bad;row['outcome']=bad
    check(mutated,True);print('negative control rejected: '+label,flush=True)
