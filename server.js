const http = require('http');
const { WebSocketServer } = require('ws');
const url = require('url');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const server = http.createServer((req, res) => {
  const parsedUrl = url.parse(req.url, true);
  const pathname = parsedUrl.pathname;

  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  if (pathname === '/') {
    const filePath = path.join(__dirname, 'index.html');
    fs.readFile(filePath, (err, data) => {
      if (err) {
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('خادم ألعاب خمسات يعمل الآن! (الرجاء التأكد من وجود ملف index.html في المجلد)');
      } else {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(data);
      }
    });
    return;
  }

  // Simple in-memory DB mocks for users, top wins, etc.
  let body = '';
  req.on('data', chunk => { body += chunk; });
  req.on('end', () => {
    let data = {};
    try { data = body ? JSON.parse(body) : {}; } catch (e) {}

    if (pathname === '/api/top') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ top: globalTopWinners }));
      return;
    }

    if (pathname === '/api/login' || pathname === '/api/register') {
      const name = (data.name || '').trim();
      if (!name || name.length < 2) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ e: 'bad' }));
        return;
      }
      let user = dbUsers.get(name);
      if (pathname === '/api/register') {
        if (user) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ e: 'exists' }));
          return;
        }
        user = { n: name, w: 0, g: 0, av: data.av || '', created: Date.now() };
        dbUsers.set(name, user);
      } else {
        if (!user) {
          user = { n: name, w: 0, g: 0, av: data.av || '', created: Date.now() };
          dbUsers.set(name, user);
        }
      }
      const token = 'tok_' + Math.random().toString(36).slice(2);
      activeTokens.set(token, name);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ token, u: user }));
      return;
    }

    if (pathname === '/api/me') {
      const token = parsedUrl.query.token;
      const name = activeTokens.get(token);
      const user = name ? dbUsers.get(name) : null;
      if (!user) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ e: 'auth' }));
        return;
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ u: user }));
      return;
    }

    if (pathname === '/api/profile') {
      const name = activeTokens.get(data.token);
      const user = name ? dbUsers.get(name) : null;
      if (user && data.av) {
        user.av = data.av;
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ u: user }));
      return;
    }

    if (pathname === '/api/user') {
      const name = parsedUrl.query.name;
      const user = dbUsers.get(name);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ u: user || null }));
      return;
    }

    if (pathname === '/api/solo') {
      const name = activeTokens.get(data.token);
      const user = name ? dbUsers.get(name) : null;
      if (user) {
        user.g = (user.g || 0) + 1;
        if (data.win) user.w = (user.w || 0) + 1;
        updateTopWinners();
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ u: user }));
      return;
    }

    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ e: 'not_found' }));
  });
});

const wss = new WebSocketServer({ server });

const rooms = new Map(); // code -> { code, hostId, meta, peers: Map(ws -> { id, user, presence }) }
const dbUsers = new Map();
const activeTokens = new Map();
let globalTopWinners = [];

function updateTopWinners() {
  const all = [...dbUsers.values()].sort((a, b) => (b.w || 0) - (a.w || 0));
  globalTopWinners = all.slice(0, 5).map(u => ({ n: u.n, w: u.w || 0 }));
}

function broadcastRoomsList() {
  const list = [];
  for (const [code, r] of rooms.entries()) {
    if (r.meta && r.meta.pub === 1) {
      let hostName = 'مضيف';
      let hostWins = 0;
      for (const [ws, info] of r.peers.entries()) {
        if (ws === r.hostWs) {
          hostName = info.presence.n || 'مضيف';
          if (info.u) hostWins = info.u.w || 0;
        }
      }
      list.push({
        c: code,
        nm: r.meta.name || 'غرفة',
        h: hostName,
        hw: hostWins,
        n: r.peers.size,
        x: 8,
        tg: r.meta.tg || 150,
        tw: r.meta.tw || 100,
        st: r.meta.st || 'l'
      });
    }
  }
  const msg = JSON.stringify({ t: 'rooms', rooms: list });
  wss.clients.forEach(client => {
    if (client.subRooms && client.readyState === 1) {
      client.send(msg);
    }
  });
}

function broadcastPeers(code) {
  const r = rooms.get(code);
  if (!r) return;
  const peersArr = [];
  for (const [ws, info] of r.peers.entries()) {
    peersArr.push({ id: info.id, p: info.presence, u: info.u || null });
  }
  const payload = JSON.stringify({ t: 'peers', peers: peersArr, left: [] });
  r.peers.forEach((info, ws) => {
    if (ws.readyState === 1) ws.send(payload);
  });
}

wss.on('connection', (ws) => {
  ws.id = 'usr_' + Math.random().toString(36).slice(2, 9);
  ws.currentRoom = null;
  ws.subRooms = false;
  ws.user = null;
  ws.presence = {};

  ws.on('message', (message) => {
    let data;
    try { data = JSON.parse(message); } catch (e) { return; }

    if (data.t === 'ping') return;

    if (data.t === 'auth') {
      const name = activeTokens.get(data.token);
      ws.user = name ? dbUsers.get(name) : null;
      ws.send(JSON.stringify({ t: 'authed', u: ws.user }));
      return;
    }

    if (data.t === 'sub') {
      ws.subRooms = true;
      const list = [];
      for (const [code, r] of rooms.entries()) {
        if (r.meta && r.meta.pub === 1) {
          let hostName = 'مضيف';
          let hostWins = 0;
          for (const [w, info] of r.peers.entries()) {
            if (w === r.hostWs) {
              hostName = info.presence.n || 'مضيف';
              if (info.u) hostWins = info.u.w || 0;
            }
          }
          list.push({
            c: code,
            nm: r.meta.name || 'غرفة',
            h: hostName,
            hw: hostWins,
            n: r.peers.size,
            x: 8,
            tg: r.meta.tg || 150,
            tw: r.meta.tw || 100,
            st: r.meta.st || 'l'
          });
        }
      }
      ws.send(JSON.stringify({ t: 'rooms', rooms: list }));
      return;
    }

    if (data.t === 'unsub') {
      ws.subRooms = false;
      return;
    }

    if (data.t === 'join') {
      const code = data.room;
      if (!code) return;

      if (!rooms.has(code)) {
        rooms.set(code, {
          code,
          hostWs: ws,
          meta: { pub: 0, name: 'غرفة', tg: 150, tw: 100, st: 'l' },
          peers: new Map()
        });
      }

      const r = rooms.get(code);
      if (r.peers.size >= 8 && !r.peers.has(ws)) {
        ws.send(JSON.stringify({ t: 'err', e: 'full' }));
        return;
      }

      ws.currentRoom = code;
      r.peers.set(ws, { id: ws.id, user: ws.user, presence: ws.presence, u: ws.user });

      ws.send(JSON.stringify({ t: 'hi', id: ws.id, n: r.peers.size }));
      broadcastPeers(code);
      broadcastRoomsList();
      return;
    }

    if (data.t === 'pres') {
      const code = ws.currentRoom;
      if (!code || !rooms.has(code)) return;
      const r = rooms.get(code);
      const info = r.peers.get(ws);
      if (info) {
        info.presence = Object.assign({}, info.presence, data.p);
      }
      broadcastPeers(code);
      return;
    }

    if (data.t === 'meta') {
      const code = ws.currentRoom;
      if (!code || !rooms.has(code)) return;
      const r = rooms.get(code);
      if (ws === r.hostWs) {
        r.meta = Object.assign({}, r.meta, data.m);
        broadcastRoomsList();
      }
      return;
    }

    if (data.t === 'win') {
      const gid = data.gid;
      const winners = data.hs || [];
      winners.forEach(peerId => {
        for (const [w, info] of rooms.get(ws.currentRoom || '')?.peers.entries() || []) {
          if (info.id === peerId && info.u) {
            info.u.g = (info.u.g || 0) + 1;
            if (data.w === peerId) info.u.w = (info.u.w || 0) + 1;
          }
        }
      });
      updateTopWinners();
      broadcastRoomsList();
      return;
    }

    if (data.t === 'emit') {
      const code = ws.currentRoom;
      if (!code || !rooms.has(code)) return;
      const r = rooms.get(code);
      const targetTo = data.d && data.d.to;
      
      r.peers.forEach((info, peerWs) => {
        if (targetTo) {
          if (info.id === targetTo && peerWs.readyState === 1) {
            peerWs.send(JSON.stringify({ t: 'emit', e: data.e, d: data.d, from: ws.id }));
          }
        } else {
          if (peerWs !== ws && peerWs.readyState === 1) {
            peerWs.send(JSON.stringify({ t: 'emit', e: data.e, d: data.d, from: ws.id }));
          }
        }
      });
      return;
    }

    if (data.t === 'leave') {
      handleLeave(ws);
      return;
    }
  });

  ws.on('close', () => {
    handleLeave(ws);
  });
});

function handleLeave(ws) {
  const code = ws.currentRoom;
  if (!code || !rooms.has(code)) return;
  const r = rooms.get(code);
  r.peers.delete(ws);
  ws.currentRoom = null;

  if (r.peers.size === 0) {
    rooms.delete(code);
  } else {
    if (ws === r.hostWs) {
      const nextWs = r.peers.keys().next().value;
      r.hostWs = nextWs;
    }
    broadcastPeers(code);
  }
  broadcastRoomsList();
}

server.listen(PORT, () => {
  console.log(`Server is listening on port ${PORT}`);
});
