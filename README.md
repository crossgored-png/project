# ARChess Server

Server kecil untuk mode **vs Player Online** ARChess: mencocokkan dua pemain dan meneruskan
pesan permainan lewat **WebSocket**. Tanpa dependensi, cukup **Node.js 14.17+**.

## Menjalankan

```
node server.js            # mendengarkan di port 8787 (ubah dengan PORT=3000 node server.js)
```

Cek: buka `http://ALAMAT:8787/health` di browser, harus muncul `{"ok":true,...}`.

## Uji cepat di jaringan sendiri (tanpa TLS)

1. Jalankan server di komputer/HP yang satu Wi-Fi dengan dua HP pemain.
2. Di aplikasi: **Pilih Mode → vs Player Online**, isi alamat server `ws://IP-KOMPUTER:8787`.
3. Di HP pertama tekan **Buat Room** (muncul kode 4 huruf), di HP kedua **Gabung Room** lalu isi kodenya.
   Atau tekan **Cari Lawan** di kedua HP untuk pencocokan acak.

## Untuk dipakai lewat internet (wajib `wss://`)

Aplikasi akan memakai `wss://` (terenkripsi). Server ini sendiri berbicara `ws://` biasa, jadi taruh di
belakang penyedia/reverse-proxy yang menangani TLS:

- **Platform hosting** (Render, Railway, Fly.io, dsb.): buat layanan Node/Docker dari folder ini,
  perintah mulai `node server.js`. Platform akan memberi alamat `https://nama.layanan.app`; di aplikasi
  isi `wss://nama.layanan.app`. Platform gratis sering "tidur" saat sepi, jadi sambungan pertama bisa lambat.
- **VPS sendiri**: jalankan `node server.js` (mis. dengan `pm2` atau systemd), lalu pasang Caddy:

  ```
  archess.contoh.com {
      reverse_proxy localhost:8787
  }
  ```

  Caddy mengurus sertifikat Let's Encrypt otomatis; di aplikasi isi `wss://archess.contoh.com`.
- **Docker**: `docker build -t archess-server .` lalu `docker run -p 8787:8787 archess-server`.

Sebelum membangun aplikasi, Anda bisa mengubah `Prefs.DEFAULT_SERVER` agar kolom alamat server terisi otomatis.

## Protokol (teks, satu baris per frame WebSocket)

Klien -> server (sebelum bermain):

| Pesan | Arti | Jawaban |
|---|---|---|
| `QUICK` | cari lawan acak | `SRV WAIT`, lalu `SRV MATCHED <W|B> <token>` |
| `CREATE` | buat room pribadi | `SRV CODE <kode>`, `SRV WAIT`, lalu `SRV MATCHED ...` |
| `JOIN <kode>` | gabung room | `SRV MATCHED ...` atau `SRV ERR nocode` |
| `CANCEL` | batalkan | - |
| `RESUME <token>` | sambung ulang ke game | `SRV RESUMED` atau `SRV ERR gone` |

Setelah `SRV MATCHED`, server **hanya meneruskan** pesan permainan yang dikenal ke lawan:
`MOVE REQ ACC DEC RESIGN SYNC STATE CLOCK TIMEOUT PING BYE NICK`. Pesan lain dibuang.
Server mengirim `SRV PEER_DOWN` / `SRV PEER_UP` saat lawan terputus / tersambung lagi, dan
menyimpan room **90 detik** setelah salah satu pemain terputus agar bisa `RESUME`.

Pembuat room otomatis menjadi **Putih**; pencocokan acak mengundi warna.

## Batas dan keamanan dasar

- Pesan maksimal 512 KB (seluruh posisi permainan hanya puluhan KB).
- Maksimal 100 pesan per detik per koneksi; klien wajib memberi mask (RFC 6455).
- Server tidak menyimpan apa pun ke disk dan tidak mengenal akun; nickname hanya dikirim antar-pemain.
- Tidak ada batas jumlah koneksi atau otentikasi. Untuk publik yang ramai, tambahkan batas di
  reverse-proxy Anda.
