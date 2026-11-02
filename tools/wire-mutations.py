#!/usr/bin/env python3
"""Generate canonical and adversarial JSON envelopes; verdicts come from Swift."""
import base64,copy,json,sys
rows=json.load(open(sys.argv[1])); bases=copy.deepcopy(rows)
def add(name,topic,payload): rows.append(dict(name=name,topic=topic,payload=base64.b64encode(payload).decode()))
def canon(obj):return json.dumps(obj,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
for base in bases:
 raw=base64.b64decode(base['payload']); obj=json.loads(raw); name=base['name']; topic=base['topic']
 for label,payload in [('space',b' '+raw),('newline',raw+b'\n'),('bom',b'\xef\xbb\xbf'+raw),('utf16le',b'\xff\xfe'+raw.decode().encode('utf-16-le')),('utf32be',b'\x00\x00\xfe\xff'+raw.decode().encode('utf-32-be')),('duplicate-invalid-first',b'{"requestID":false,'+raw[1:]),('duplicate-invalid-last',raw[:-1]+b',"requestID":false}'),('order',json.dumps(obj,separators=(',',':')).encode()),('unknown',canon(dict(obj,unknown=0))),('duplicate',b'{"requestID":7,'+raw[1:]),('numeric-float',raw.replace(b'"requestID":1,',b'"requestID":1.0,')),('numeric-exponent',raw.replace(b'"requestID":1,',b'"requestID":1e0,'))]:add(name+'-'+label,topic,payload)
 for path in [[],['Other'],['Nexus',''],['Nexus','a/b'],['Nexus',' '],['Nexus','é'],['Nexus','child'],['Nexus','a"b'],['Nexus','a\\b']]:
  v=copy.deepcopy(obj);v['chainPath']=path;add(name+'-path-'+repr(path),topic,canon(v))
 for val in [-1,1.5,'1',True,None,18446744073709551616]:
  v=copy.deepcopy(obj);v['requestID']=val;add(name+'-request-'+repr(val),topic,canon(v))
 for key in ['requestID','chainPath']:
  v=copy.deepcopy(obj);del v[key];add(name+'-missing-'+key,topic,canon(v))
 if 'maximum' in obj:
  for val in [0,-1,2048,2049,10.0,True,'10']:
   v=copy.deepcopy(obj);v['maximum']=val;add(name+'-maximum-'+repr(val),topic,canon(v))
 if 'own' in obj:
  for val in ['',None,' ', 'a'*129,'a"b','a\\b']:
   v=copy.deepcopy(obj);v['own']=val;add(name+'-own-'+repr(val),topic,canon(v))
  v=copy.deepcopy(obj);v['logID']=None;add(name+'-null-log',topic,canon(v))
 if 'headers.response' in topic and obj['entries']:
  for key in ['block','children','spec']:
   for val in [None,'','AA==','!!!!',obj['entries'][0][key].rstrip('=')]:
    v=copy.deepcopy(obj);v['entries'][0][key]=val;add(name+'-'+key+'-'+repr(val[:12] if isinstance(val,str) else val),topic,canon(v))
# Actual Swift binary proof vectors are inserted into a canonical header envelope.
from pathlib import Path
proofs=json.load(open(Path(__file__).resolve().parents[2]/'lattice-rs/crates/lattice-block-tree/tests/fixtures/swift-proof-wire.json'))
base=next(row for row in bases if 'headers.response' in row['topic'] and json.loads(base64.b64decode(row['payload']))['entries'])
for i,proof in enumerate(proofs):
 obj=json.loads(base64.b64decode(base['payload']));blob=bytes.fromhex(proof['wire']);obj['entries'][0]['proofs']=[base64.b64encode(blob).decode()];add('proof-'+str(i),base['topic'],canon(obj))
 if i<5:
  obj['entries'][0]['proofs']=[base64.b64encode(blob+b'\x00').decode()];add('proof-trailing-'+str(i),base['topic'],canon(obj))
add('unknown-topic' ,'unrelated',b'invalid')
json.dump(rows,open(sys.argv[2],'w'),separators=(',',':'))
