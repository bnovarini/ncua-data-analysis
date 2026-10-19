"""Minimal MCP call log. No arguments, results, addresses or User-Agent stored."""
import os,json,sqlite3,time,threading,re
_lock=threading.Lock()
class UsageLog:
 def __init__(self,app,tool_names):self.app=app;self.tools=set(tool_names);self.sessions={}
 async def __call__(self,scope,receive,send):
  if scope['type']!='http' or scope.get('method')!='POST':return await self.app(scope,receive,send)
  chunks=[]
  while True:
   m=await receive()
   if m['type']=='http.disconnect':return
   chunks.append(m.get('body',b''))
   if not m.get('more_body'):break
  body=b''.join(chunks);now=time.time();rows=[]
  # Protocol session token is used only for a short-lived in-memory association.
  headers=dict(scope.get('headers',[]));session=headers.get(b'mcp-session-id');cn=cv=None
  if session in self.sessions:
   cn,cv,stamp=self.sessions[session]
   if now-stamp>3600:cn=cv=None
  try:
   msgs=json.loads(body);msgs=msgs if isinstance(msgs,list) else [msgs]
   for m in msgs:
    if not isinstance(m,dict):continue
    method=m.get('method');p=m.get('params') or {}
    if method=='initialize':
     ci=p.get('clientInfo') or {};clean=lambda x,n:re.sub(r'[^A-Za-z0-9 ._\-/+]','',str(x or ''))[:n]
     cn,cv=clean(ci.get('name'),64),clean(ci.get('version'),32);rows.append((time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime(now)),'initialize',None,cn,cv))
    elif method=='tools/call':rows.append((time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime(now)),'tool_call',p.get('name') if p.get('name') in self.tools else '(unknown)',cn,cv))
  except Exception:pass
  sent=False
  async def replay():
   nonlocal sent
   if not sent:sent=True;return {'type':'http.request','body':body,'more_body':False}
   return await receive()
  async def capture(message):
   if message['type']=='http.response.start' and cn is not None:
    sid=dict(message.get('headers',[])).get(b'mcp-session-id')
    if sid:self.sessions[sid]=(cn,cv,now)
    if len(self.sessions)>5000:self.sessions={k:v for k,v in self.sessions.items() if now-v[2]<3600}
   await send(message)
  info,buf={'code':0},[]
  async def cap2(message):
   if message['type']=='http.response.start':info['code']=message['status']
   elif message['type']=='http.response.body' and sum(map(len,buf))<262144:buf.append(message.get('body',b''))
   await capture(message)
  t0=time.time();outcome=None
  try:await self.app(scope,replay,cap2)
  except BaseException:
   outcome='exception';raise
  finally:
   try:
    dur=int((time.time()-t0)*1000)
    if outcome is None:
     code=info['code'];b=b''.join(buf)
     if code==0:outcome='aborted'
     elif code>=500:outcome='http_5xx'
     elif code>=400:outcome='http_4xx'
     elif b'"isError":true' in b or b'"isError": true' in b:outcome='tool_error'
     elif b'-32601' in b[:2000]:outcome='method_not_found'
     elif b'"error":{' in b[:2000] or b'"error": {' in b[:2000]:outcome='rpc_error'
     else:outcome='ok'
    if rows:
     with _lock:
      c=sqlite3.connect(os.environ['MCP_USAGE_DB'],timeout=5);c.execute('CREATE TABLE IF NOT EXISTS usage(ts TEXT,event TEXT,tool TEXT,client_name TEXT,client_version TEXT)')
      have={r[1] for r in c.execute('PRAGMA table_info(usage)')}
      for col,typ in (('duration_ms','INTEGER'),('status','TEXT')):
       if col not in have:
        try:c.execute('ALTER TABLE usage ADD COLUMN %s %s'%(col,typ))
        except sqlite3.OperationalError:pass
      c.executemany('INSERT INTO usage VALUES(?,?,?,?,?,?,?)',[tuple(r)+(dur,outcome) for r in rows]);c.commit();c.close()
   except Exception:pass
