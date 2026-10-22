// خادم لعبة الخمسات والجاسوس — غرف أونلاين (WebSocket) + حسابات (HTTP)
// التشغيل: npm install && npm start
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 10000;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');
const MAX_ROOM = 12;                 // أقصى عدد في الغرفة
const TOKEN_TTL = 90 * 864e5;        // مدة بقاء تسجيل الدخول: 90 يوماً
const MAX_USERS = 20000;

/* ================= التخزين (ملف JSON) ================= */
let DB = { users: {}, tokens: {} };
try { DB = Object.assign(DB, JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'))); } catch (e) { /* أول تشغيل */ }
let saveTimer = null;
function save() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    try {
      const tmp = DATA_FILE + '.tmp';
      fs.writeFileSync(tmp, JSON.stringify(DB));
      fs.renameSync(tmp, DATA_FILE);
    } catch (e) { console.error('تعذّر حفظ البيانات:', e.message); }
  }, 1500);
}

/* ================= أدوات ================= */
const keyOf = (n) => String(n).normalize('NFKC').trim().toLowerCase();
const okName = (n) => typeof n === 'string' && n.trim().length >= 2 && n.trim().length <= 14 && !/[\u0000-\u001f<>&"'`\\]/.test(n);
const okAv = (a) => typeof a === 'string' && a.length <= 8000 && /^data:image\/jpeg;base64,[A-Za-z0-9+/=]+$/.test(a);
const pubU = (u) => (u ? { n: u.n, av: u.av || '', w: u.w | 0, g: u.g | 0, t: u.t } : null);
const hashPw = (pw, salt) => crypto.scryptSync(pw, salt, 32).toString('hex');
const sameHex = (a, b) => { try { return crypto.timingSafeEqual(Buffer.from(a, 'hex'), Buffer.from(b, 'hex')); } catch (e) { return false; } };

const hits = new Map(); // تحديد محاولات الدخول لكل IP
function limited(ip) {
  const now = Date.now();
  let h = hits.get(ip);
  if (!h || now - h.t > 60000) h = { n: 0, t: now };
  h.n++;
  hits.set(ip, h);
  return h.n > 15;
}
setInterval(() => { const now = Date.now(); for (const [k, v] of hits) if (now - v.t > 60000) hits.delete(k); }, 60000);

function accByToken(token) {
  const t = typeof token === 'string' ? DB.tokens[token] : null;
  if (!t) return null;
  if (t.exp < Date.now()) { delete DB.tokens[token]; save(); return null; }
  return DB.users[t.u] || null;
}
function newToken(k) {
  const token = crypto.randomBytes(24).toString('hex');
  DB.tokens[token] = { u: k, exp: Date.now() + TOKEN_TTL };
  return token;
}

/* ================= HTTP ================= */
const json = (res, code, obj) => { res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' }); res.end(JSON.stringify(obj)); };
function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on('data', (c) => { size += c.length; if (size > 65536) { reject(new Error('big')); req.destroy(); } else chunks.push(c); });
    req.on('end', () => { try { resolve(chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : {}); } catch (e) { reject(e); } });
    req.on('error', reject);
  });
}
const clientIp = (req) => String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();

async function apiRoute(req, res, url) {
  const p = url.pathname;
  const ip = clientIp(req);
  let b = {};
  if (req.method === 'POST') {
    try { b = await readBody(req); } catch (e) { return json(res, 400, { e: 'bad' }); }
  }

  if (p === '/api/register' && req.method === 'POST') {
    if (limited(ip)) return json(res, 429, { e: 'many' });
    const name = String(b.name || '').trim(), pass = String(b.pass || '');
    if (!okName(name) || pass.length < 4 || pass.length > 64) return json(res, 400, { e: 'bad' });
    const k = keyOf(name);
    if (DB.users[k]) return json(res, 409, { e: 'exists' });
    if (Object.keys(DB.users).length >= MAX_USERS) return json(res, 503, { e: 'full' });
    const salt = crypto.randomBytes(16).toString('hex');
    DB.users[k] = { n: name, salt, h: hashPw(pass, salt), av: okAv(b.av) ? b.av : '', w: 0, g: 0, t: Date.now(), gids: [], ls: 0 };
    const token = newToken(k);
    save();
    return json(res, 200, { u: pubU(DB.users[k]), token });
  }

  if (p === '/api/login' && req.method === 'POST') {
    if (limited(ip)) return json(res, 429, { e: 'many' });
    const k = keyOf(b.name || ''), u = DB.users[k];
    if (!u || !sameHex(u.h, hashPw(String(b.pass || ''), u.salt))) return json(res, 401, { e: 'bad' });
    if (!u.av && okAv(b.av)) u.av = b.av;
    const token = newToken(k);
    save();
    return json(res, 200, { u: pubU(u), token });
  }

  if (p === '/api/logout' && req.method === 'POST') {
    if (typeof b.token === 'string' && DB.tokens[b.token]) { delete DB.tokens[b.token]; save(); }
    return json(res, 200, { ok: 1 });
  }

  if (p === '/api/me' && req.method === 'GET') {
    const u = accByToken(url.searchParams.get('token'));
    return u ? json(res, 200, { u: pubU(u) }) : json(res, 401, { e: 'auth' });
  }

  if (p === '/api/profile' && req.method === 'POST') {
    const u = accByToken(b.token);
    if (!u) return json(res, 401, { e: 'auth' });
    if (okAv(b.av)) { u.av = b.av; save(); }
    return json(res, 200, { u: pubU(u) });
  }

  if (p === '/api/solo' && req.method === 'POST') {
    const u = accByToken(b.token);
    if (!u) return json(res, 401, { e: 'auth' });
    const gid = String(b.gameId || '').slice(0, 64), now = Date.now();
    if (gid && !u.gids.includes(gid) && now - (u.ls || 0) > 10000) {
      u.gids.push(gid); if (u.gids.length > 100) u.gids.shift();
      u.ls = now; u.g++; if (b.win === true) u.w++;
      save();
    }
    return json(res, 200, { u: pubU(u) });
  }

  if (p === '/api/top' && req.method === 'GET') {
    const top = Object.values(DB.users).filter((u) => u.w > 0).sort((a, b2) => b2.w - a.w || a.g - b2.g).slice(0, 10).map((u) => ({ n: u.n, w: u.w }));
    return json(res, 200, { top });
  }

  if (p === '/api/user' && req.method === 'GET') {
    const u = DB.users[keyOf(url.searchParams.get('name') || '')];
    return u ? json(res, 200, { u: pubU(u) }) : json(res, 404, { e: 'none' });
  }

  return json(res, 404, { e: 'none' });
}

// الملفات التي يقدّمها الخادم للمتصفح (قائمة بيضاء: لا يُكشف server.js ولا data.json)
const STATIC = {
  'index.html': 'text/html; charset=utf-8',
  'style.css': 'text/css; charset=utf-8',
  'app.js': 'text/javascript; charset=utf-8',
  'spy.js': 'text/javascript; charset=utf-8',
};
function findStatic(pathname) {
  const name = pathname === '/' ? 'index.html' : pathname.replace(/^\//, '');
  if (!Object.prototype.hasOwnProperty.call(STATIC, name)) return null;
  for (const dir of [__dirname, path.join(__dirname, 'public')]) {
    const f = path.join(dir, name);
    if (fs.existsSync(f)) return { file: f, type: STATIC[name] };
  }
  return null;
}

const server = http.createServer(async (req, res) => {
  // CORS: يسمح بفتح اللعبة من Netlify أو أي موقع آخر
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  let url;
  try { url = new URL(req.url, 'http://localhost'); } catch (e) { res.writeHead(400); return res.end(); }
  try {
    if (url.pathname.startsWith('/api/')) return await apiRoute(req, res, url);
    if (url.pathname === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
    if (req.method === 'GET') {
      const st = findStatic(url.pathname);
      if (st) {
        res.writeHead(200, { 'Content-Type': st.type, 'Cache-Control': 'no-cache', 'X-Content-Type-Options': 'nosniff' });
        return fs.createReadStream(st.file).pipe(res);
      }
    }
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('khamsat rooms server is running');
  } catch (e) {
    console.error(e);
    if (!res.headersSent) res.writeHead(500);
    res.end();
  }
});

/* ================= الغرف (WebSocket) ================= */
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 262144 });
const rooms = new Map();   // code -> {code, peers:Map(id -> {ws,p,acct}), host, meta, gids:Set}
const subs = new Set();    // المشتركون في قائمة الغرف العامة
let nextId = 1;

const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
function peersMsg(room, left) {
  return {
    t: 'peers',
    peers: [...room.peers].map(([id, q]) => ({ id, p: q.p, u: q.acct ? pubU(DB.users[q.acct]) : null })),
    left: left || [],
  };
}
function bcast(room, o, except) {
  const s = JSON.stringify(o);
  for (const [id, q] of room.peers) if (id !== except && q.ws.readyState === 1) q.ws.send(s);
}
function roomsList() {
  const out = [];
  for (const r of rooms.values()) {
    if (!r.meta || r.meta.pub !== 1 || !r.host) continue;
    const h = r.peers.get(r.host), hu = h && h.acct && DB.users[h.acct];
    out.push({ c: r.code, nm: r.meta.name, h: hu ? hu.n : ((h && h.p && h.p.n) || ''), hw: hu ? hu.w | 0 : undefined, n: r.meta.n, x: 8, tg: r.meta.tg, tw: r.meta.tw, st: r.meta.st });
  }
  return out;
}
let roomsTimer = null;
function pushRooms() {
  if (roomsTimer) return;
  roomsTimer = setTimeout(() => {
    roomsTimer = null;
    const m = JSON.stringify({ t: 'rooms', rooms: roomsList() });
    subs.forEach((ws) => { if (ws.readyState === 1) ws.send(m); });
  }, 250);
}

function leave(ws) {
  const room = ws.room;
  if (!room) return;
  const id = ws.id, q = room.peers.get(id);
  ws.room = null; ws.id = null;
  if (!q) return;
  room.peers.delete(id);
  if (room.peers.size === 0) { rooms.delete(room.code); pushRooms(); return; }
  if (room.host === id) { room.host = null; room.meta = null; }
  bcast(room, peersMsg(room, [{ id, p: q.p }]));
  pushRooms();
}

const cleanMeta = (m) => {
  if (!m || typeof m !== 'object') return null;
  const num = (v, max) => Math.max(0, Math.min(max, v | 0));
  return {
    pub: m.pub === 1 ? 1 : 0,
    name: String(m.name || 'غرفة').slice(0, 30),
    n: num(m.n, 12), tg: num(m.tg, 9999), tw: num(m.tw, 9999),
    st: m.st === 'p' ? 'p' : 'l',
  };
};

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.id = null; ws.room = null; ws.acct = null;
  const bucket = { n: 120, t: Date.now() };

  ws.on('message', (raw) => {
    const now = Date.now();
    bucket.n = Math.min(120, bucket.n + (now - bucket.t) * 0.06); bucket.t = now;
    if (bucket.n < 1) return;
    bucket.n--;
    let x;
    try { x = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!x || typeof x !== 'object') return;

    switch (x.t) {
      case 'auth': {
        const u = accByToken(x.token);
        ws.acct = u ? keyOf(u.n) : null;
        send(ws, { t: 'authed', u: pubU(u) });
        if (ws.room && ws.room.peers.has(ws.id)) {
          ws.room.peers.get(ws.id).acct = ws.acct;
          bcast(ws.room, peersMsg(ws.room));
        }
        break;
      }
      case 'join': {
        const code = String(x.room || '');
        if (!/^\d{4}$/.test(code)) return send(ws, { t: 'err', e: 'bad' });
        leave(ws);
        let room = rooms.get(code);
        const others = room ? room.peers.size : 0;      // عدد من هم موجودون قبلي
        if (others >= MAX_ROOM) return send(ws, { t: 'err', e: 'full' });
        if (!room) { room = { code, peers: new Map(), host: null, meta: null, gids: new Set() }; rooms.set(code, room); }
        const id = 'p' + nextId++;
        room.peers.set(id, { ws, p: {}, acct: ws.acct });
        ws.id = id; ws.room = room;
        if (!room.host) room.host = id;
        send(ws, { t: 'hi', id, n: others });
        bcast(room, peersMsg(room));
        pushRooms();
        break;
      }
      case 'pres': {
        const room = ws.room, q = room && room.peers.get(ws.id);
        if (!q || !x.p || typeof x.p !== 'object') return;
        for (const k of Object.keys(x.p)) {
          if (x.p[k] === null) delete q.p[k]; else q.p[k] = x.p[k];
        }
        bcast(room, { t: 'pres', id: ws.id, p: x.p });
        break;
      }
      case 'emit': {
        const room = ws.room;
        if (!room || typeof x.e !== 'string' || x.e.length > 16) return;
        const d = x.d && typeof x.d === 'object' ? x.d : {};
        const m = { t: 'emit', e: x.e, d, from: ws.id };
        if (d.to) { const q = room.peers.get(d.to); if (q) send(q.ws, m); }
        else bcast(room, m, ws.id);
        break;
      }
      case 'meta': {
        const room = ws.room;
        if (!room || room.host !== ws.id) return;
        room.meta = cleanMeta(x.m);
        pushRooms();
        break;
      }
      case 'win': {
        const room = ws.room;
        if (!room || room.host !== ws.id) return;
        const gid = String(x.gid || '').slice(0, 64);
        if (!gid || room.gids.has(gid)) return;
        room.gids.add(gid); if (room.gids.size > 50) room.gids.delete(room.gids.values().next().value);
        const ids = [...new Set(Array.isArray(x.hs) ? x.hs.map(String) : [])].filter((i) => room.peers.has(i));
        if (ids.length < 2) return;                       // لا تُحسب إلا ألعاب فيها لاعبان حقيقيان على الأقل
        const touched = new Set();
        ids.forEach((i) => {
          const q = room.peers.get(i), u = q.acct && DB.users[q.acct];
          if (!u) return;
          u.g++; if (x.w === i) u.w++;
          touched.add(i);
        });
        if (touched.size) {
          save();
          touched.forEach((i) => { const q = room.peers.get(i); send(q.ws, { t: 'authed', u: pubU(DB.users[q.acct]) }); });
          bcast(room, peersMsg(room));
        }
        break;
      }
      case 'leave': leave(ws); break;
      case 'sub': subs.add(ws); send(ws, { t: 'rooms', rooms: roomsList() }); break;
      case 'unsub': subs.delete(ws); break;
      default: break; // ping وغيرها
    }
  });

  ws.on('close', () => { subs.delete(ws); leave(ws); });
  ws.on('error', () => {});
});

// نبضات للحفاظ على الاتصال على Render وقطع الاتصالات الميتة
setInterval(() => {
  wss.clients.forEach((c) => {
    if (c.isAlive === false) return c.terminate();
    c.isAlive = false;
    try { c.ping(); } catch (e) { /* ignore */ }
  });
}, 25000);

server.listen(PORT, () => console.log('listening on ' + PORT));
