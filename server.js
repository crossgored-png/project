'use strict';
/**
 * ARChess Server — pencocokan lawan + penerus pesan (relay) lewat WebSocket.
 * Tanpa dependensi: cukup Node.js 14+.   Jalankan:  node server.js     (PORT=8787 bawaan)
 *
 * Alur klien (teks, satu baris per frame):
 *   -> QUICK            cari lawan acak          <- SRV WAIT ... SRV MATCHED <W|B> <token>
 *   -> CREATE           buat room pribadi        <- SRV CODE <kode>, SRV WAIT ... SRV MATCHED ...
 *   -> JOIN <kode>      gabung room              <- SRV MATCHED ...   |   SRV ERR nocode
 *   -> CANCEL           batalkan pencarian/room
 *   -> RESUME <token>   sambung ulang ke game    <- SRV RESUMED       |   SRV ERR gone
 * Setelah SRV MATCHED, pesan permainan (MOVE, REQ, ACC, ... NICK) diteruskan apa adanya ke lawan.
 */
const http = require('http');
const crypto = require('crypto');

const PORT = parseInt(process.env.PORT || '8787', 10);
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_PAYLOAD = 512 * 1024;          // STATE (seluruh posisi) bisa puluhan KB
const GRACE_MS = 90 * 1000;              // waktu menunggu pemain yang terputus untuk sambung ulang
const OPEN_ROOM_MS = 10 * 60 * 1000;     // room pribadi yang tak pernah dimasuki dihapus
const RELAY = new Set(['MOVE', 'REQ', 'ACC', 'DEC', 'RESIGN', 'SYNC', 'STATE', 'CLOCK', 'TIMEOUT', 'PING', 'BYE', 'NICK']);
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

// ------------------------------------------------------------ WebSocket (sisi server, minimal)

const allConns = new Set();

class Conn {
  constructor(sock) {
    this.sock = sock;
    this.buf = Buffer.alloc(0);
    this.alive = true;
    this.closed = false;
    this.fragOp = 0;
    this.frags = [];
    this.fragLen = 0;
    this.state = 'new';      // new | queue | waiting | game
    this.room = null;
    this.side = null;
    this.rateStart = Date.now();
    this.rateCount = 0;
    sock.setNoDelay(true);
    sock.on('data', (d) => this.onData(d));
    sock.on('end', () => this.destroy());    // server HTTP memakai allowHalfOpen: tanpa ini FIN dari klien tak terdeteksi
    sock.on('close', () => this.destroy());
    sock.on('error', () => this.destroy());
    allConns.add(this);
  }

  frame(op, payload) {
    const len = payload.length;
    let head;
    if (len < 126) {
      head = Buffer.from([0x80 | op, len]);
    } else if (len <= 0xFFFF) {
      head = Buffer.alloc(4);
      head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2);
    } else {
      head = Buffer.alloc(10);
      head[0] = 0x80 | op; head[1] = 127; head.writeUInt32BE(0, 2); head.writeUInt32BE(len, 6);
    }
    return Buffer.concat([head, payload]);
  }

  send(text) {
    if (this.closed) return;
    try { this.sock.write(this.frame(0x1, Buffer.from(text, 'utf8'))); } catch (e) { this.destroy(); }
  }

  ping() {
    if (this.closed) return;
    try { this.sock.write(this.frame(0x9, Buffer.alloc(0))); } catch (e) { this.destroy(); }
  }

  fail(code) {
    if (this.closed) return;
    try {
      const p = Buffer.alloc(2); p.writeUInt16BE(code, 0);
      this.sock.write(this.frame(0x8, p));
    } catch (e) { /* abaikan */ }
    this.destroy();
  }

  onData(d) {
    if (this.closed) return;
    this.buf = this.buf.length ? Buffer.concat([this.buf, d]) : d;
    for (;;) {
      const b = this.buf;
      if (b.length < 2) return;
      const fin = (b[0] & 0x80) !== 0;
      const op = b[0] & 0x0F;
      const masked = (b[1] & 0x80) !== 0;
      let len = b[1] & 0x7F;
      let off = 2;
      if (len === 126) {
        if (b.length < 4) return;
        len = b.readUInt16BE(2); off = 4;
      } else if (len === 127) {
        if (b.length < 10) return;
        if (b.readUInt32BE(2) !== 0) { this.fail(1009); return; }
        len = b.readUInt32BE(6); off = 10;
      }
      if (!masked) { this.fail(1002); return; }           // klien wajib memberi mask
      if (len > MAX_PAYLOAD) { this.fail(1009); return; }
      if (b.length < off + 4 + len) return;
      const mask = b.slice(off, off + 4);
      const data = Buffer.alloc(len);
      for (let i = 0; i < len; i++) data[i] = b[off + 4 + i] ^ mask[i & 3];
      this.buf = b.slice(off + 4 + len);
      this.onFrame(fin, op, data);
      if (this.closed) return;
    }
  }

  onFrame(fin, op, data) {
    if (op === 0x8) {                       // close
      try { this.sock.write(this.frame(0x8, data.slice(0, 2))); } catch (e) { /* abaikan */ }
      this.destroy();
    } else if (op === 0x9) {                // ping -> pong
      try { this.sock.write(this.frame(0xA, data)); } catch (e) { this.destroy(); }
    } else if (op === 0xA) {                // pong
      this.alive = true;
    } else if (op === 0x1 || op === 0x2 || op === 0x0) {
      if (op !== 0x0) { this.fragOp = op; this.frags = []; this.fragLen = 0; }
      this.frags.push(data);
      this.fragLen += data.length;
      if (this.fragLen > MAX_PAYLOAD) { this.fail(1009); return; }
      if (fin) {
        const all = Buffer.concat(this.frags);
        this.frags = []; this.fragLen = 0;
        if (this.fragOp === 0x1) this.onText(all.toString('utf8'));
      }
    }
  }

  onText(text) {
    const now = Date.now();                 // pembatas laju: maks 100 pesan per detik
    if (now - this.rateStart > 1000) { this.rateStart = now; this.rateCount = 0; }
    if (++this.rateCount > 100) return;
    const line = text.replace(/[\r\n]+$/, '');
    if (this.state === 'game') {
      if (line.startsWith('RESUME ')) return;
      relay(this, line);
      return;
    }
    handleCommand(this, line);
  }

  destroy() {
    if (this.closed) return;
    this.closed = true;
    allConns.delete(this);
    try { this.sock.destroy(); } catch (e) { /* abaikan */ }
    onDisconnect(this);
  }
}

// ------------------------------------------------------------ Room & pencocokan

const queue = [];                 // Conn yang menunggu lawan acak
const rooms = new Map();          // kode -> room
const tokens = new Map();         // token -> { room, side }

function newCode() {
  for (;;) {
    let c = '';
    for (let i = 0; i < 4; i++) c += CODE_CHARS[crypto.randomInt(CODE_CHARS.length)];
    if (!rooms.has(c)) return c;
  }
}

function newRoom() {
  const room = {
    code: newCode(), matched: false, over: false, openTimer: null,
    slots: { W: { token: null, conn: null, timer: null }, B: { token: null, conn: null, timer: null } },
  };
  rooms.set(room.code, room);
  return room;
}

function other(side) { return side === 'W' ? 'B' : 'W'; }

function pair(room, connW, connB) {
  room.matched = true;
  if (room.openTimer) { clearTimeout(room.openTimer); room.openTimer = null; }
  [['W', connW], ['B', connB]].forEach(([side, conn]) => {
    const token = crypto.randomBytes(12).toString('hex');
    const slot = room.slots[side];
    slot.token = token;
    slot.conn = conn;
    tokens.set(token, { room, side });
    conn.room = room;
    conn.side = side;
    conn.state = 'game';
    conn.send('SRV MATCHED ' + side + ' ' + token);
  });
}

function dropRoom(room, notify) {
  if (room.over) return;
  rooms.delete(room.code);
  ['W', 'B'].forEach((side) => {
    const slot = room.slots[side];
    if (slot.timer) { clearTimeout(slot.timer); slot.timer = null; }
    if (slot.token) tokens.delete(slot.token);
    if (notify && slot.conn) slot.conn.send('SRV GONE');
    if (slot.conn) { slot.conn.room = null; slot.conn.state = 'new'; }
  });
  room.over = true;
}

function leaveAll(c) {
  const qi = queue.indexOf(c);
  if (qi >= 0) queue.splice(qi, 1);
  if (c.state === 'waiting' && c.room && !c.room.matched) {
    if (c.room.openTimer) clearTimeout(c.room.openTimer);
    rooms.delete(c.room.code);
    c.room = null;
  }
  if (c.state === 'queue' || c.state === 'waiting') c.state = 'new';
}

function handleCommand(c, line) {
  const sp = line.indexOf(' ');
  const verb = sp < 0 ? line : line.slice(0, sp);
  const arg = sp < 0 ? '' : line.slice(sp + 1).trim();
  if (verb === 'QUICK') {
    leaveAll(c);
    while (queue.length && queue[0].closed) queue.shift();
    if (queue.length) {
      const o = queue.shift();
      const room = newRoom();
      if (Math.random() < 0.5) pair(room, o, c); else pair(room, c, o);
    } else {
      queue.push(c);
      c.state = 'queue';
      c.send('SRV WAIT');
    }
  } else if (verb === 'CREATE') {
    leaveAll(c);
    const room = newRoom();
    room.creator = c;
    room.openTimer = setTimeout(() => { if (!room.matched) { rooms.delete(room.code); c.send('SRV ERR expired'); c.state = 'new'; c.room = null; } }, OPEN_ROOM_MS);
    c.room = room;
    c.state = 'waiting';
    c.send('SRV CODE ' + room.code);
    c.send('SRV WAIT');
  } else if (verb === 'JOIN') {
    const room = rooms.get(arg.toUpperCase());
    if (!room || room.matched || !room.creator || room.creator.closed || room.creator === c) {
      c.send('SRV ERR nocode');
      return;
    }
    leaveAll(c);
    pair(room, room.creator, c);       // pembuat room = Putih, yang bergabung = Hitam
  } else if (verb === 'CANCEL') {
    leaveAll(c);
  } else if (verb === 'RESUME') {
    const t = tokens.get(arg);
    if (!t || t.room.over) { c.send('SRV ERR gone'); return; }
    const slot = t.room.slots[t.side];
    if (slot.conn && slot.conn !== c) {            // soket lama dianggap mati
      const old = slot.conn;
      slot.conn = null;
      old.room = null;
      old.state = 'new';
      old.destroy();
    }
    if (slot.timer) { clearTimeout(slot.timer); slot.timer = null; }
    slot.conn = c;
    c.room = t.room;
    c.side = t.side;
    c.state = 'game';
    c.send('SRV RESUMED');
    const peer = t.room.slots[other(t.side)].conn;
    if (peer) peer.send('SRV PEER_UP');
  } else {
    c.send('SRV ERR bad');
  }
}

function relay(c, line) {
  const room = c.room;
  if (!room || !room.matched || room.over) return;
  const sp = line.indexOf(' ');
  const verb = sp < 0 ? line : line.slice(0, sp);
  if (!RELAY.has(verb)) return;                    // hanya pesan permainan yang dikenal
  const peer = room.slots[other(c.side)].conn;
  if (peer) peer.send(line);
  if (verb === 'BYE') setTimeout(() => dropRoom(room, false), 5000);
}

function onDisconnect(c) {
  leaveAll(c);
  const room = c.room;
  if (!room || !room.matched || room.over) return;
  const slot = room.slots[c.side];
  if (slot.conn !== c) return;                     // sudah digantikan lewat RESUME
  slot.conn = null;
  const peer = room.slots[other(c.side)].conn;
  if (peer) peer.send('SRV PEER_DOWN');
  slot.timer = setTimeout(() => dropRoom(room, true), GRACE_MS);
}

// ------------------------------------------------------------ HTTP + upgrade

const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, name: 'ARChess', rooms: rooms.size, waiting: queue.length, connections: allConns.size }));
  } else {
    res.writeHead(404);
    res.end();
  }
});

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (String(req.headers.upgrade || '').toLowerCase() !== 'websocket' || !key) {
    socket.destroy();
    return;
  }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  new Conn(socket);
});

// ping berkala: sambungan yang tak menjawab dalam 2 siklus dianggap mati
setInterval(() => {
  for (const c of Array.from(allConns)) {
    if (!c.alive) { c.destroy(); continue; }
    c.alive = false;
    c.ping();
  }
}, 25000).unref();

server.listen(PORT, () => console.log('ARChess server mendengarkan di port ' + PORT));
