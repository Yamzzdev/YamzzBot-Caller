package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	meowcaller "github.com/purpshell/meowcaller"
	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// Sender = satu akun WhatsApp yang bisa dipakai buat NELPON. Bot punya banyak sender
// (multi-device): kalau sender 1 lagi dipakai nelpon orang, .playcall berikutnya
// otomatis pindah ke sender 2, dst — jadi bisa banyak call barengan.
//
// Sender pertama (index 0) juga jadi "bot utama": dia yang nangkep semua command &
// balesin chat. Sender lain khusus buat nelpon aja.
type Sender struct {
	name   string // "sender1", "sender2", ...
	wa     *whatsmeow.Client
	call   *meowcaller.Client
	device *store.Device

	mu        sync.Mutex
	sess      *callSession  // audio call (.playcall) yang lagi jalan di sender ini (nil = nganggur)
	videoSess *videoSession // video call (.playvideo) yang lagi jalan (nil = nganggur)
}

// number balikin nomor telpon sender (buat ditampilin), atau "?" kalau belum login.
func (s *Sender) number() string {
	if s.wa != nil && s.wa.Store != nil && s.wa.Store.ID != nil {
		return s.wa.Store.ID.User
	}
	return "?"
}

func (s *Sender) connected() bool {
	return s.wa != nil && s.wa.IsConnected() && s.wa.IsLoggedIn()
}

// inCall balikin true kalau sender ini lagi sibuk — call audio (.playcall) ATAU
// video (.playvideo). Satu sumber kebenaran ini dipakai pool.acquireFree() buat
// dua-duanya, jadi otomatis gak saling nabrak: sender yang lagi playcall gak
// bakal kepilih buat playvideo, begitu juga sebaliknya.
func (s *Sender) inCall() bool {
	s.mu.Lock()
	sess, vsess := s.sess, s.videoSess
	s.mu.Unlock()
	if sess != nil && sess.active {
		return true
	}
	if vsess != nil {
		vsess.mu.Lock()
		active := vsess.active
		vsess.mu.Unlock()
		if active {
			return true
		}
	}
	return false
}

// videoSession balikin video session sender ini, bikin baru kalau belum ada.
func (s *Sender) videoSession() *videoSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.videoSess == nil {
		s.videoSess = &videoSession{sender: s}
	}
	return s.videoSess
}

// ─── Pool ─────────────────────────────────────────────────────────────────────

type senderPool struct {
	mu        sync.Mutex
	senders   []*Sender
	container *sqlstore.Container
	logger    zerolog.Logger
}

var pool = &senderPool{}

// initFromDevices bikin Sender buat tiap device yang udah login di DB. Device pertama
// jadi bot utama. Client-nya di-connect + event handler dipasang di sini.
func (p *senderPool) initFromDevices(ctx context.Context, devices []*store.Device) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, dev := range devices {
		s := p.buildSender(i, dev)
		p.senders = append(p.senders, s)
	}
}

// buildSender bikin satu Sender dari device (belum di-connect). Caller connect sendiri.
func (p *senderPool) buildSender(index int, dev *store.Device) *Sender {
	name := fmt.Sprintf("sender%d", index+1)
	wa := whatsmeow.NewClient(dev, waLog.Zerolog(p.logger).Sub(name))
	call := meowcaller.NewClient(wa, meowcaller.WithLogger(p.logger))
	s := &Sender{name: name, wa: wa, call: call, device: dev}

	// Cuma sender utama (index 0) yang nangkep command & event chat.
	if index == 0 {
		wa.AddEventHandler(func(evt any) { mainSenderEvents(s, evt) })
	}
	return s
}

// connectAll nyambungin semua sender ke WhatsApp.
func (p *senderPool) connectAll() {
	p.mu.Lock()
	senders := append([]*Sender(nil), p.senders...)
	p.mu.Unlock()
	for _, s := range senders {
		if err := s.wa.Connect(); err != nil {
			p.logger.Warn().Err(err).Str("sender", s.name).Msg("gagal connect sender")
		}
	}
}

// main balikin sender utama (index 0), atau nil kalau pool kosong.
func (p *senderPool) main() *Sender {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.senders) == 0 {
		return nil
	}
	return p.senders[0]
}

// list balikin salinan daftar sender (buat listsender).
func (p *senderPool) list() []*Sender {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*Sender(nil), p.senders...)
}

// acquireFree cari sender yang connected & lagi nganggur (gak lagi call). Balikin nil
// kalau semua sibuk / gak ada yang online.
func (p *senderPool) acquireFree() *Sender {
	p.mu.Lock()
	senders := append([]*Sender(nil), p.senders...)
	p.mu.Unlock()
	for _, s := range senders {
		if s.connected() && !s.inCall() {
			return s
		}
	}
	return nil
}

// byName cari sender by nama ("sender1", "sender2"), case-insensitive. nil kalau gak ada.
func (p *senderPool) byName(name string) *Sender {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, s := range p.senders {
		if strings.EqualFold(s.name, name) {
			return s
		}
	}
	return nil
}

// activeSessions balikin semua session yang lagi call (dari semua sender).
func (p *senderPool) activeSessions() []*callSession {
	p.mu.Lock()
	senders := append([]*Sender(nil), p.senders...)
	p.mu.Unlock()
	var out []*callSession
	for _, s := range senders {
		s.mu.Lock()
		if s.sess != nil && s.sess.active {
			out = append(out, s.sess)
		}
		s.mu.Unlock()
	}
	return out
}

// sessionFor cari session aktif buat sebuah command. Kalau senderName diisi, cari sender
// itu. Kalau kosong & cuma ADA 1 call aktif → pakai itu. Kalau banyak → nil + minta
// disebutin. Balikin (session, errorMsg).
func (p *senderPool) sessionFor(senderName string) (*callSession, string) {
	if senderName != "" {
		s := p.byName(senderName)
		if s == nil {
			return nil, "❌ Sender *" + senderName + "* gak ada. Cek *" + Prefix + "listsender*."
		}
		s.mu.Lock()
		sess := s.sess
		s.mu.Unlock()
		if sess == nil || !sess.active {
			return nil, "📭 *" + senderName + "* lagi gak ada call."
		}
		return sess, ""
	}
	act := p.activeSessions()
	switch len(act) {
	case 0:
		return nil, "📭 Gak ada call aktif."
	case 1:
		return act[0], ""
	default:
		return nil, fmt.Sprintf("❓ Ada %d call aktif. Sebutin sender-nya, contoh: *%sskip sender1*", len(act), Prefix)
	}
}

// videoSessionFor: sama kayak sessionFor tapi buat video call. Sender bisa punya
// call audio & video BARENGAN (dua slot beda), makanya lookup-nya terpisah.
func (p *senderPool) videoSessionFor(senderName string) (*videoSession, string) {
	if senderName != "" {
		s := p.byName(senderName)
		if s == nil {
			return nil, "❌ Sender *" + senderName + "* gak ada. Cek *" + Prefix + "listsender*."
		}
		s.mu.Lock()
		vs := s.videoSess
		s.mu.Unlock()
		if vs == nil || !vs.isActive() {
			return nil, "📭 *" + senderName + "* lagi gak ada video call."
		}
		return vs, ""
	}
	var active []*videoSession
	for _, s := range p.list() {
		s.mu.Lock()
		vs := s.videoSess
		s.mu.Unlock()
		if vs != nil && vs.isActive() {
			active = append(active, vs)
		}
	}
	switch len(active) {
	case 0:
		return nil, "📭 Gak ada video call aktif."
	case 1:
		return active[0], ""
	default:
		return nil, fmt.Sprintf("❓ Ada %d video call aktif. Sebutin sender-nya, contoh: *%sskipvideo sender1*", len(active), Prefix)
	}
}

// addSender bikin device baru, mulai pairing pakai nomor telpon, balikin pairing code.
// Sender-nya langsung dimasukin pool (status connecting sampai PairSuccess).
func (p *senderPool) addSender(ctx context.Context, phone string) (code string, name string, err error) {
	dev := p.container.NewDevice()
	p.mu.Lock()
	index := len(p.senders)
	s := p.buildSender(index, dev)
	p.senders = append(p.senders, s)
	p.mu.Unlock()

	if err := s.wa.Connect(); err != nil {
		return "", "", fmt.Errorf("connect: %w", err)
	}

	// Pasang handler buat tau pairing sukses (biar auto ke-save).
	s.wa.AddEventHandler(func(evt any) {
		switch evt.(type) {
		case *events.PairSuccess:
			p.logger.Info().Str("sender", s.name).Str("num", s.number()).Msg("✅ sender pairing sukses")
		case *events.Connected:
			_ = s.wa.SendPresence(context.Background(), types.PresenceAvailable)
		}
	})

	ctx2, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	code, err = s.wa.PairPhone(ctx2, phone, true, whatsmeow.PairClientChrome, "Chrome (Linux)")
	if err != nil {
		return "", "", fmt.Errorf("pair: %w", err)
	}
	return code, s.name, nil
}

// removeSender: disconnect & buang sender dari pool. Dipake tombol "Batalkan"
// di .addsender (pairing belum kelar / salah nomor).
func (p *senderPool) removeSender(name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, s := range p.senders {
		if s.name == name {
			if s.wa != nil {
				s.wa.Disconnect()
			}
			p.senders = append(p.senders[:i], p.senders[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("sender %s gak ketemu", name)
}

// mainSenderEvents nangkep event di sender utama: pesan masuk (→ router command) &
// lifecycle. Cuma sender utama yang punya handler ini.
func mainSenderEvents(s *Sender, evt any) {
	ctx := context.Background()
	switch v := evt.(type) {
	case *events.Message:
		handleMessage(ctx, v)
	case *events.Connected:
		pool.logger.Info().Str("sender", s.name).Msg("✅ bot utama connected")
		_ = s.wa.SendPresence(ctx, types.PresenceAvailable)
	case *events.LoggedOut:
		pool.logger.Warn().Msg("⚠️ bot utama logged out — hapus yamzzbot.db & login ulang")
	}
}
