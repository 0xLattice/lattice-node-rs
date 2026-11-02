#!/usr/bin/env python3
"""Check production journal/reducer observations using pinned Recovery.tla.
No second Python/Rust graph oracle: TLC evaluates the original Fold, Apply,
CanonicalTip and TrueCumWork operators on actual staged production traces.
"""
import copy,json,os,pathlib,shutil,subprocess,sys,tempfile
ROOT=pathlib.Path(__file__).resolve().parent.parent
FORMAL=pathlib.Path(sys.argv[1] if len(sys.argv)>1 else ROOT.parent/'lattice-formal').resolve()
PIN='a9f4c2879b6565ae11c17d35efe14475c5830978'
if subprocess.check_output(['git','rev-parse','HEAD'],cwd=FORMAL,text=True).strip()!=PIN:
    sys.exit('formal revision mismatch')
if subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],cwd=FORMAL):
    sys.exit('formal sources have tracked modifications')
JAVA=os.environ.get('JAVA') or ('/opt/homebrew/opt/openjdk/bin/java' if pathlib.Path('/opt/homebrew/opt/openjdk/bin/java').exists() else 'java')
def tla(x):
    if isinstance(x,bool):return 'TRUE' if x else 'FALSE'
    if isinstance(x,int):return str(x)
    if isinstance(x,str):return json.dumps(x)
    if isinstance(x,list):return '<<'+','.join(tla(v) for v in x)+'>>'
    if isinstance(x,dict):return '['+', '.join(f'{k} |-> {tla(v)}' for k,v in x.items())+']'
    raise TypeError(x)
def aset(xs):return '{'+','.join(tla(v) for v in xs)+'}'
def fn(d):return '('+' @@ '.join(f'{tla(k)} :> {tla(v)}' for k,v in d.items())+')' if d else '[x \\in {} |-> 0]'
def graph(g):return '[held |-> '+aset(g['held'])+', meta |-> '+fn(g['meta'])+', loc |-> '+fn(g['loc'])+', qty |-> '+fn(g['qty'])+', val |-> '+aset(g['val'])+', exc |-> '+aset(g['exc'])+']'
def check(trace,rows,expected=None):
    batch_values={json.dumps(x,sort_keys=True):x for r in rows for x in r['log']}
    batches=list(batch_values.values());gen=next(x for x in batches if x['b']=='r')
    records=[]
    for r in rows:
        records.append('[log |-> '+tla(r['log'])+', graph |-> '+graph(r['graph'])+', tip |-> '+tla(r['tip'])+', weights |-> '+fn(r['weights'])+', cumulative |-> '+fn(r['cumulative'])+', revision |-> '+str(r['revision'])+', seen |-> '+str(r['seen'])+']')
    model='''---- MODULE MCProduction ----
EXTENDS Recovery, TLC
VARIABLE i
TBlocks == {"r","a","b"}
TParent == [b \\in TBlocks |-> IF b = "r" THEN "none" ELSE "r"]
TGrinds == {"g_r","g_a","g_b","g_x"}
TRank == '''+fn(trace['cidRank'])+'''
TBatches == '''+aset(batches)+'''
TGenesis == '''+tla(gen)+'''
TRebuild == [x \\in TBatches |-> x]
Rows == <<'''+',\n'.join(records)+'''>>
ProductionInit == Init /\\ i = 1
ProductionNext == \\/ /\\ i < Len(Rows) /\\ i' = i + 1 /\\ UNCHANGED vars
                  \\/ /\\ i = Len(Rows) /\\ UNCHANGED <<vars,i>>
GraphMatches == Rows[i].graph = Fold(EmptyG, Rows[i].log)
TipMatches == Rows[i].tip = CanonicalTip(Fold(EmptyG, Rows[i].log))
WeightsMatch == \\A b \\in Rows[i].graph.held :
    Rows[i].weights[b] = SumQty(Fold(EmptyG, Rows[i].log), {g \\in Grinds : Fold(EmptyG, Rows[i].log).loc[g] = b})
CumulativeMatches == \\A b \\in Rows[i].graph.held :
    Rows[i].cumulative[b] = TrueCumWork(Fold(EmptyG, Rows[i].log), b)
ObservedRevisionMonotone == Rows[i].revision >= Rows[i].seen
====
'''
    cfg='''INIT ProductionInit
NEXT ProductionNext
CONSTANTS
 None = "none"
 Blocks <- TBlocks
 Parent <- TParent
 CidRank <- TRank
 Grinds <- TGrinds
 Batches <- TBatches
 Genesis <- TGenesis
 Rebuild <- TRebuild
 MaxLog = 100
 MaxCrashes = 2
 FloorPersisted = TRUE
 SortedReplay = TRUE
INVARIANTS GraphMatches TipMatches WeightsMatch CumulativeMatches ObservedRevisionMonotone
'''
    with tempfile.TemporaryDirectory(prefix='lattice-recovery-tlc-') as directory:
        work=pathlib.Path(directory);shutil.copy(FORMAL/'specs/Recovery.tla',work)
        (work/'MCProduction.tla').write_text(model);(work/'MCProduction_Run.cfg').write_text(cfg)
        result=subprocess.run([str(FORMAL/'scripts/tlc.sh'),'MCProduction_Run.cfg',str(work)],env={**os.environ,'JAVA':JAVA},capture_output=True,text=True)
        output=result.stdout+result.stderr
        if expected:
            if f'Invariant {expected} is violated' not in output:sys.exit('negative control did not fail as expected:\n'+output)
            print(f'negative control rejected: {expected}')
        elif 'No error has been found' not in output:sys.exit(output)
        else:print(f'Recovery.tla: {len(rows)} production observations conform')
with tempfile.TemporaryDirectory(prefix='lattice-recovery-export-') as directory:
    output=pathlib.Path(directory)/'trace.json'
    with output.open('w') as stream:subprocess.run(['cargo','run','--locked','--quiet','--example','recovery_trace'],cwd=ROOT,stdout=stream,check=True)
    trace=json.loads(output.read_text())
check(trace,trace['rows'])
for field,invariant in [('weights','WeightsMatch'),('cumulative','CumulativeMatches')]:
    rows=copy.deepcopy(trace['rows']);r=next(r for r in rows if r[field]);key=next(iter(r[field]));r[field][key]+=1;check(trace,rows,invariant)
rows=copy.deepcopy(trace['rows']);rows[0]['tip']='none';check(trace,rows,'TipMatches')
rows=copy.deepcopy(trace['rows']);rows[-1]['graph']['qty']['g_r']+=1;check(trace,rows,'GraphMatches')
check(trace,[trace['negativeFloor']],'ObservedRevisionMonotone')
print('Production trace refinement passed. Scope: observed durable prefixes, replay permutations, conflicts, exact graph/weights/tip, and revision floor. Not a proof of full node admission or liveness.')
