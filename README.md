# 🐱 YamzzBot

Bot WhatsApp berbasis **Go** ([whatsmeow](https://github.com/tulir/whatsmeow)) buat **nelpon + streaming musik/video** ke call, dengan **multi-sender**, **antrian**, dan fitur **prank**. Pakai [meowcaller](./meowcaller) (WhatsApp 1:1 calling stack, pure Go, di-fork & dipatch lokal) buat signaling/RTP/codec-nya.

> 🙏 **Tidak berafiliasi** dengan API theresav / nexray / spotify — bot ini cuma pakai API publiknya sebagai sumber lagu/video.

---

## ✨ Fitur

### 📞 Panggilan Audio
| Command | Fungsi |
|---|---|
| `m!playcall <judul>` | Reply chat target + judul, ATAU `m!playcall 628xxx, judul`. Otomatis pilih sender nganggur |
| `m!skip [sender]` | Lanjut ke lagu berikutnya di antrian |
| `m!stopcall [sender]` | Hangup + bersihin antrian |
| `m!antrian [sender]` | Lihat antrian lagu (publik, gak perlu owner) |
| `m!prank <sender>` | Reply audio → sisipin ke call yang lagi jalan, lagu lama lanjut abis itu 😹 |

### 🎬 Panggilan Video (eksperimental)
| Command | Fungsi |
|---|---|
| `m!playvideo <link YouTube>` | Video call + streaming video, **multi-sender** (otomatis pilih sender nganggur, gak nabrak `.playcall`) |
| `m!skipvideo [sender]` | Lanjut ke video berikutnya di antrian |
| `m!stopvideo [sender]` | Hangup + bersihin antrian video |
| `m!antrianvideo` | Lihat semua video call yang jalan + antrian |

Video call render beneran di HP target (bukan cuma ring+audio) — butuh bundling SPS/PPS jadi 1 paket STAP-A biar decoder WhatsApp nerima stream-nya. Detail teknis ada di komentar `meowcaller/engine_media.go`.

### 🤖 Manajemen Sender
| Command | Fungsi |
|---|---|
| `m!addsender 628xxx` | Tambah akun penelpon baru (pairing code, bukan QR) |
| `m!canceladd <sender>` | Batalin pairing yang belum kelar / salah nomor |
| `m!listsender` | Status semua sender: online/offline, lagi nelpon siapa |

Bisa daftarin **banyak akun** buat nelpon/video call. Kalau sender 1 lagi sibuk (audio ATAU video call), permintaan berikutnya otomatis pindah ke sender 2, dst. Kalau semua sibuk, masuk antrian — otomatis jalan begitu ada yang nganggur.

### ⚙️ Umum & Pengaturan
| Command | Fungsi |
|---|---|
| `m!allmenu` | Tampilin menu (isinya beda buat owner vs non-owner) |
| `m!ping` | Cek latency + uptime |
| `m!self` | Bot cuma respon owner |
| `m!public` | Command umum (allmenu, ping, antrian) kebuka buat semua orang |

---

## 📦 Requirement

- **Go 1.25+**
- **ffmpeg** (`sudo apt install -y ffmpeg`) — buat encode video & prank audio
- **apikey theresav** (buat `m!playvideo`) — daftar gratis di https://api.theresav.biz.id

---

## 🚀 Setup

```bash
git clone https://github.com/Yamzzdev/YamzzBot-Caller
cd YamzzBot-Caller

# 1. Isi apikey theresav (buat .playvideo) di config.go -> TheresavAPIKey
#    atau: export THERESAV_APIKEY="apikey_kamu" (env selalu menang)

# 2. Download dependency + build
go mod tidy
go build -o yamzzbot .

# 3. Jalanin — pertama kali keluar QR, scan di:
#    WhatsApp -> Perangkat Tertaut -> Tautkan Perangkat
./yamzzbot
```

Pakai **PM2** biar jalan permanen:
```bash
pm2 start ./yamzzbot --name yamzzbot
pm2 logs yamzzbot
```

---

## ⚙️ Config (`config.go`)

```go
OwnerNumber             = "628xxxxxxxx" // nomor owner (command owner-only)
Prefix                  = "m!"          // prefix command
PlaycallCooldownSeconds = 30            // anti-spam .playcall per pengirim
TheresavAPIKey          = ""            // apikey theresav (buat .playvideo)
TheresavResolution      = "720"         // resolusi SOURCE download (bukan resolusi final call)
```

---

## 📁 Struktur

```
yamzzbot_vid/
├── main.go          # entry point + multi-device loader
├── sender.go        # sender pool (multi akun), pairing, cek sibuk (audio+video)
├── commands.go      # router command, playcall, antrian, menu
├── features.go      # addsender, listsender, prank
├── playvideo.go     # video call (multi-sender, antrian, sync audio/video)
├── videoencode.go   # ffmpeg: download+encode H.264 + audio
├── spotify.go       # sumber lagu (audio)
├── theresav.go      # sumber video (apikey di config.go)
├── keepalive.go     # heartbeat call (pakai koneksi sender yang benar)
├── config.go        # SEMUA setting bot dalam satu file
└── meowcaller/       # library calling WhatsApp (pure Go, fork lokal + patch)
```

---

## ⚠️ Batasan & Catatan Teknis

- **Durasi video call max 10 menit** (`maxVideoSeconds` di `videoencode.go`). Video lebih panjang bakal di-loop dari titik itu, bukan diputer full — encode video lama juga makan waktu & RAM lebih banyak, jadi ada batesnya.
- **LID (privacy ID WhatsApp)**: kalau reply chat di grup dan orangnya pakai LID (`@lid`, bukan nomor asli), bot otomatis coba resolve ke nomor asli. Kalau gagal (belum ke-cache), bakal dikasih pesan error yang jelas — bukan asal nge-dial LID yang gak bisa ditelpon.
- **Video call cuma 1 aktif per sender** — kalau mau lebih banyak video call bareng, tambah sender lain (`.addsender`).
- Session tersimpan di `yamzzbot.db` — **JANGAN di-commit / sebarin** (itu kredensial login WA). Udah ke-cover di `.gitignore`.
- Butuh `ffmpeg` di PATH server.

---

Dibuat dengan ❤️
