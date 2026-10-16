// خادم غرف لعبة الخمسات — يعمل على Render
const http = require('http');
const { WebSocketServer } = require('ws');

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('khamsat rooms server is running');
});

const wss = new WebSocketServer({ server, path: '/ws' });

const rooms = new Map(); // code -> Map(id -> {ws, presence})
let nextId = 1;

function roomOf(code) {
  if (!rooms.has(code)) rooms.set(code, new Map());
  return rooms.get(code);
}

function broadcastPeers(code, leftIds) {
  const room = rooms.get(code);
  if (!room) return;
  const peers = [...room.entries()].map(([id, p]) => ({ id, p: p.presence || {} }));
  const left = (leftIds || []).map((id) => ({ id, p: {} }));
  const msg = JSON.stringify({ t: 'peers', peers, left });
  for (const p of room.values()) {
    if (p.ws.readyState === 1) p.ws.send(msg);
  }
}

wss.on('connection', (ws) => {
  let id = null;
  let code = null;

  ws.on('message', (raw) => {
    let x;
    try {
      x = JSON.parse(raw.toString());
    } catch (e) {
      return;
    }

    if (x.t === 'join') {
      code = String(x.room || 'lobby');
      id = 'p' + nextId++;
      roomOf(code).set(id, { ws, presence: {} });
      ws.send(JSON.stringify({ t: 'hi', id, n: roomOf(code).size }));
      broadcastPeers(code);
      return;
    }

    if (!id || !code) return;
    const room = rooms.get(code);
    if (!room) return;

    if (x.t === 'pres') {
      const entry = room.get(id);
      if (!entry) return;
      const p = entry.presence || {};
      Object.assign(p, x.p || {});
      for (const k of Object.keys(x.p || {})) {
        if (x.p[k] === null) delete p[k];
      }
      entry.presence = p;
      broadcastPeers(code);
      return;
    }

    if (x.t === 'emit') {
      const d = x.d || {};
      const target = d.to;
      if (target && room.has(target)) {
        room.get(target).ws.send(JSON.stringify({ t: 'emit', e: x.e, d, from: id }));
      }
      return;
    }
  });

  ws.on('close', () => {
    if (!id || !code) return;
    const room = rooms.get(code);
    if (!room) return;
    room.delete(id);
    broadcastPeers(code, [id]);
    if (room.size === 0) rooms.delete(code);
  });
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
  console.log('listening on ' + PORT);
});
