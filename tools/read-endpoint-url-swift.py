#!/usr/bin/env python3
"""Run the unchanged pinned NodeConfiguration URL validator with Foundation."""
import hashlib,json,pathlib,subprocess,tempfile
root=pathlib.Path(__file__).resolve().parents[1]
source=root.parent/'lattice-node/Sources/LatticeNode/Configuration/NodeConfiguration.swift'
text=source.read_text();start=text.index('    public static func isValidPublicReadURL');end=text.index('\n    public var chainPath:',start)
function=text[start:end]
cases=['https://reads.example','http://127.0.0.1:8080/base','HTTP://example.com','https://[::1]:80','http://user@example.com','http://:password@example.com','http://example.com?','http://example.com#','http://','ftp://example.com','/relative','https://例子.test','https://example.com/space here','https://example.com/%zz','https://example.com/%00','https://example.com/\\x','https://a:abc','https://a:99999','https://a:','https://[xyz]','https://[fe80::1%25en0]']
for char in ['a','_','-','.','~','!','$','&',"'",'(',')','*','+',',',';',':','=','%','%20','%zz','[',']','\\','|','^','`','{','}','<','>','"','@']:
    cases.extend(['https://'+char+'test','https://example.com/'+char])
cases+=['https://x/'+ 'a'*n for n in [501,502,503,504]]
swift='import Foundation\nstruct ReadEndpointResponseMessage { static let maximumURLBytes=512 }\nstruct NodeConfiguration {\n'+function+'\n}\nlet input=try! JSONDecoder().decode([String].self,from:FileHandle.standardInput.readDataToEndOfFile())\nlet result=input.map { ["url":$0,"valid":NodeConfiguration.isValidPublicReadURL($0)] as [String:Any] }\nFileHandle.standardOutput.write(try! JSONSerialization.data(withJSONObject:result,options:[.sortedKeys]))\n'
with tempfile.TemporaryDirectory() as directory:
    path=pathlib.Path(directory);(path/'main.swift').write_text(swift)
    subprocess.run(['swiftc',str(path/'main.swift'),'-o',str(path/'oracle')],check=True)
    rows=json.loads(subprocess.check_output([path/'oracle'],input=json.dumps(cases).encode()))
(root/'tests/read-endpoint-url-swift.json').write_text(json.dumps({'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'function_sha256':hashlib.sha256(function.encode()).hexdigest(),'rows':rows},indent=2)+'\n')
print(len(rows),'actual Swift URL verdicts')
