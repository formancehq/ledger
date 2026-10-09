import argparse, fcntl, json, os, pathlib, pty, select, signal, struct, subprocess, termios, time, urllib.request

parser = argparse.ArgumentParser(description="Run the three-operation prototype against an empty local Ledger")
parser.add_argument('--runtime', required=True, help="JSON with http, grpc, version of a loopback-only Ledger")
parser.add_argument('--work-dir', required=True, help="A new directory for the isolated fctl config and report")
parser.add_argument('--fctl-binary', required=True)
parser.add_argument('--ledgerctl-binary', required=True)
parser.add_argument('--plugin-binary', required=True)
args = parser.parse_args()
runtime = json.loads(pathlib.Path(args.runtime).read_text())
assert runtime['http'].startswith('http://127.0.0.1:'), 'refusing a non-loopback HTTP target'
assert runtime['grpc'].startswith('127.0.0.1:'), 'refusing a non-loopback gRPC target'
directory = pathlib.Path(args.work_dir).resolve()
directory.mkdir(parents=True, exist_ok=False)
config = directory / 'config'
config.mkdir()
fctl = [str(pathlib.Path(args.fctl_binary).resolve()), '--config-dir', str(config), '--auth-mode', 'none', '--ledger-url', runtime['http']]
ledgerctl = [str(pathlib.Path(args.ledgerctl_binary).resolve()), '--shared-command-poc', '--server', runtime['grpc'], '--insecure', '--auth-token', 'fixture']
plugin_binary = str(pathlib.Path(args.plugin_binary).resolve())
env = {k:v for k,v in os.environ.items() if not k.startswith(('FCTL_', 'LEDGERCTL_', 'OTEL_'))}
env.update(NO_COLOR='1', TERM='xterm-256color')
results = []
owned = set()
prefix = 'sharedpoc' + str(os.getpid())
passed = False

def run(name, command, body=None, fail=False, structured=True):
    result = subprocess.run(command, input=body, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, timeout=25)
    record = {'case':name, 'code':result.returncode, 'stdout':result.stdout.decode(), 'stderr':result.stderr.decode()}
    results.append(record)
    assert (result.returncode != 0) == fail, record
    if structured and not fail:
        assert b'\x1b' not in result.stdout, record
        return json.loads(result.stdout)
    if fail:
        assert result.stdout == b'', record
    return record

def read(path):
    with urllib.request.urlopen(runtime['http'] + path, timeout=10) as response:
        return json.load(response)

def name_list():
    return [item['name'] for item in read('/v3/')['data']]

def count(name):
    return len(read('/v3/' + name + '/transactions')['data'])

def terminal(name, command, actions, cancel=False):
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack('HHHH',24,100,0,0))
    def setup():
        os.setsid()
        fcntl.ioctl(slave, termios.TIOCSCTTY, 0)
    process = subprocess.Popen(command, stdin=slave, stderr=slave, stdout=subprocess.PIPE, env=env, preexec_fn=setup)
    os.close(slave)
    terminal_output, stdout = bytearray(), bytearray()
    step, current_start = 0, 0
    deadline = time.monotonic()+25
    active = {master: 'terminal', process.stdout.fileno(): 'stdout'}
    try:
        while active:
            assert time.monotonic()<deadline, {'case':name, 'timeout':terminal_output.decode(errors='replace')[-6000:]}
            ready, _, _ = select.select(list(active), [], [], min(0.25, max(0, deadline-time.monotonic())))
            for descriptor in ready:
                try: chunk = os.read(descriptor, 65536)
                except OSError: chunk = b''
                if not chunk:
                    active.pop(descriptor, None)
                    continue
                if descriptor == master: terminal_output.extend(chunk)
                else: stdout.extend(chunk)
            if step < len(actions):
                expected, keys = actions[step]
                if expected.encode() in terminal_output[current_start:]:
                    os.write(master, keys)
                    current_start = len(terminal_output)
                    step += 1
        code = process.wait(timeout=5)
        record = {'case':name, 'code':code, 'stdout':stdout.decode(), 'terminal':terminal_output.decode(errors='replace'), 'steps':step}
        results.append(record)
        assert step == len(actions), record
        assert (code != 0) == cancel, record
        assert b'\x1b' not in stdout, record
        if cancel:
            assert not stdout, record
            return None
        return json.loads(stdout)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        os.close(master)

try:
    run('install external plugin', fctl + ['plugins','install','--binary',plugin_binary,'-o','json'])
    assert run('HTTP initial list', fctl+['ledger','list','-o','json','--no-input'])['data'] == []
    assert run('gRPC initial list', ledgerctl+['ledgers','list','--json']) == {}
    for host, base in [('http', fctl), ('grpc',ledgerctl)]:
        ledger = prefix+host
        owned.add(ledger)
        body = json.dumps({'metadata':{'counter':9007199254740993},'defaultEnforcementMode':'STRICT'})
        create = base+(['ledger','create',ledger,'--data',body,'-o','json','--no-input'] if host=='http' else ['ledgers','create','--name',ledger,'--data',body,'--json'])
        run(host+' create JSON', create)
        assert read('/v3/'+ledger)['data']['metadata']['counter'] == 9007199254740993
        postings={'postings':[{'source':'world','destination':'users:001','asset':'USD/2','amount':9007199254740993}]}
        payload=json.dumps(postings, separators=(',',':'))
        body_file=directory/(host+'-transaction.json')
        body_file.write_text(payload)
        for mode, value, stdin in [('inline',payload,None),('file','@'+str(body_file),None),('stdin','-',payload.encode())]:
            key=prefix+host+mode
            create=base+(['ledger','--ledger',ledger,'transactions','create','-o','json','--no-input'] if host=='http' else ['transactions','create','--ledger',ledger,'--json'])
            run(host+' transaction '+mode, create+['--data',value,'--idempotency-key',key], stdin)
            latest=read('/v3/'+ledger+'/transactions')['data'][0]
            tx=latest.get('transaction',latest)
            assert tx['postings'][0]['amount']==9007199254740993, latest
        assert count(ledger)==3
        command=base+(['ledger','--ledger',ledger,'transactions','create','-o','json','--no-input'] if host=='http' else ['transactions','create','--ledger',ledger,'--json'])
        for value in ['{bad','@'+str(directory/'missing.json')]:
            run(host+' invalid input '+value[:8],command+['--data',value],fail=True)
            assert count(ledger)==3
    native=[item for item in ledgerctl if item!='--shared-command-poc']
    assert run('shared gRPC list', ledgerctl+['ledgers','list','--json']) == run('legacy gRPC list', native+['ledgers','list','--json'])
    for host, base in [('http',fctl),('grpc',ledgerctl)]:
        run(host+' human list',base+(['ledger','list','--no-input'] if host=='http' else ['ledgers','list']),structured=False)
        run(host+' missing name nonTTY',base+(['ledger','create','--no-input','-o','json'] if host=='http' else ['ledgers','create','--json']),fail=True)
    form_ledger=prefix+'httpform'; owned.add(form_ledger)
    terminal('HTTP real form',fctl+['ledger','create','-o','json'],[
        ('Step 1 of 5',form_ledger.encode()+b'\r'),
        ('Account type enforcement',b'\r'),('Ledger metadata',b'\r'),('Initial metadata schema',b'\r'),('Account types',b'\r')])
    assert form_ledger in name_list()
    form_ledger=prefix+'grpcform'; owned.add(form_ledger)
    terminal('gRPC real native prompt',ledgerctl+['ledgers','create','--json'],[('Ledger name: ',form_ledger.encode()+b'\r')])
    assert form_ledger in name_list()
    before=name_list()
    terminal('HTTP form cancellation',fctl+['ledger','create','-o','json'],[('Step 1 of 5',b'\x03')],cancel=True)
    terminal('gRPC prompt cancellation',ledgerctl+['ledgers','create','--json'],[('Ledger name: ',b'\x03')],cancel=True)
    assert name_list()==before, 'cancelled prompts created a ledger'
    passed = True
finally:
    for ledger in sorted(owned):
        if ledger in name_list():
            run('cleanup '+ledger,fctl+['ledger','delete',ledger,'--confirm','-o','json','--no-input'])
    leftovers=[name for name in name_list() if name in owned]
    assert not leftovers, leftovers
    (directory/'report.json').write_text(json.dumps({'status':'PASS' if passed else 'FAIL','runtime':{'version':runtime['version'],'http':runtime['http'],'grpc':runtime['grpc']},'cases':results,'cleanup':{'leftovers':leftovers}},indent=2))

print(json.dumps({'status':'PASS', 'commandCases':len(results)-len(owned), 'cleanupCalls':len(owned), 'report':str(directory/'report.json')}))
