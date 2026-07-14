package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	meowcaller "github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// ─── Queue & Session ─────────────────────────────────────────────────────────

type queueItem struct {
	track *spotifyTrack
	file  string
	evt   *events.Message
}

type callSession struct {
	mu        sync.Mutex
	sender    *Sender // sender yang naruh call ini
	active    bool
	target    string
	shortID   string
	requester string // user yang manggil .playcall — dipake buat validasi tap tombol
	queue     []queueItem
	player    *meowcaller.Player
	call      *meowcaller.Call
	stopHB    chan struct{}
	done      chan struct{}
	once      sync.Once
	playNext  func() // dipanggil manual saat skip

	// prank: kalau lagi diprank, lagu yang lagi diputer disimpen (path file) buat
	// dilanjutin lagi begitu audio prank habis.
	pranking   bool
	curFile    string // path mp3 lagu yang lagi diputer (buat resume abis prank)
	prankTitle string
}

func (s *callSession) reset() {
	s.active = false
	s.target = ""
	s.shortID = ""
	s.queue = nil
	s.player = nil
	s.call = nil
	s.playNext = nil
	s.pranking = false
	s.curFile = ""
	s.prankTitle = ""
}

func (s *callSession) stopAll() {
	s.once.Do(func() {
		select {
		case <-s.stopHB:
		default:
			close(s.stopHB)
		}
		select {
		case <-s.done:
		default:
			close(s.done)
		}
	})
}

// ─── Mode ─────────────────────────────────────────────────────────────────────

var (
	modeMu sync.RWMutex
	mode   = "self"
)

func getMode() string  { modeMu.RLock(); defer modeMu.RUnlock(); return mode }
func setMode(m string) { modeMu.Lock(); mode = m; modeMu.Unlock() }

// ─── Cooldown ───────────────────────────────────────────────────────────────
// Dipake bareng buat .playcall & .playvideo, per pengirim. Sebelumnya
// PlaycallCooldownSeconds cuma didefinisiin tapi gak pernah di-enforce — ini
// benerin itu.

var (
	cooldownMu sync.Mutex
	lastCallAt = map[string]time.Time{}
)

// checkCooldown balikin (true, 0) kalau boleh lanjut (dan langsung nyatet
// waktunya), atau (false, sisaWaktu) kalau masih kena cooldown.
func checkCooldown(user string) (bool, time.Duration) {
	cooldownMu.Lock()
	defer cooldownMu.Unlock()
	limit := time.Duration(PlaycallCooldownSeconds) * time.Second
	if t, ok := lastCallAt[user]; ok {
		if elapsed := time.Since(t); elapsed < limit {
			return false, limit - elapsed
		}
	}
	lastCallAt[user] = time.Now()
	return true, 0
}

// senderUser ngambil nomor/user id pengirim dari sebuah events.Message, konsisten
// sama logika resolusi sender di handleMessage (pake SenderAlt kalau ada).
func senderUser(evt *events.Message) string {
	senderStr := evt.Info.Sender.ToNonAD().String()
	if !evt.Info.MessageSource.SenderAlt.IsEmpty() {
		senderStr = evt.Info.MessageSource.SenderAlt.ToNonAD().String()
	}
	return strings.Split(senderStr, "@")[0]
}

// cleanJIDNumber motong suffix "@server" dan ":device" dari sebuah JID/nomor
// string, nyisain nomor polos doang. Dipake buat nampilin target di pesan.
func cleanJIDNumber(target string) string {
	return strings.Split(strings.Split(target, "@")[0], ":")[0]
}

// ─── Router ──────────────────────────────────────────────────────────────────

func handleMessage(ctx context.Context, evt *events.Message) {
	text := evt.Message.GetConversation()
	if text == "" {
		text = evt.Message.GetExtendedTextMessage().GetText()
	}
	text = strings.TrimSpace(text)

	if evt.Info.IsFromMe || text == "" || !strings.HasPrefix(text, Prefix) {
		return
	}

	body := strings.TrimSpace(strings.TrimPrefix(text, Prefix))
	if body == "" {
		return
	}

	fields := strings.Fields(body)
	cmd := strings.ToLower(fields[0])
	args := strings.TrimSpace(strings.TrimPrefix(body, fields[0]))

	user := senderUser(evt)
	ownerSender := isOwner(user)

	switch cmd {
	case "allmenu", "menu", "help":
		if !requirePublicOrOwner(ownerSender) {
			return
		}
		sendText(ctx, evt.Info.Chat, menuText(ownerSender))

	case "ping":
		if !requirePublicOrOwner(ownerSender) {
			return
		}
		handlePing(ctx, evt)

	case "self":
		if !requireOwner(ctx, evt, ownerSender) {
			return
		}
		setMode("self")
		sendText(ctx, evt.Info.Chat, "🔒 Mode *SELF* — bot cuma respon owner.")

	case "public":
		if !requireOwner(ctx, evt, ownerSender) {
			return
		}
		setMode("public")
		sendText(ctx, evt.Info.Chat, "🌐 Mode *PUBLIC* — command umum bisa dipake semua.")

	case "playcall":
		if !requireOwner(ctx, evt, ownerSender) {
			return
		}
		go handlePlaycall(ctx, evt, args)

	case "skip":
		if !requireOwner(ctx, evt, ownerSender) {
			return
		}
		handleSkip(ctx, evt, args)

	case "stopcall":
		if !requireOwner(ctx, evt, ownerSender) {
			return
		}
		handleStop(ctx, evt, args)

	case "addsender":
		if !requireOwner(ctx, evt, ownerSender) {
			return
		}
		go handleAddSender(ctx, evt, args)

	case "canceladd":
		if !requireOwner(ctx, evt, ownerSender) {
			return
		}
		if err := pool.removeSender(strings.TrimSpace(args)); err != nil {
			sendText(ctx, evt.Info.Chat, "❌ "+err.Error())
			return
		}
		reactMsg(ctx, evt, "🗑️")
		sendText(ctx, evt.Info.Chat, "🗑️ Pairing "+strings.TrimSpace(args)+" dibatalin.")

	case "listsender", "senders":
		if !requireOwner(ctx, evt, ownerSender) {
			return
		}
		handleListSender(ctx, evt)

	case "prank":
		if !requireOwner(ctx, evt, ownerSender) {
			return
		}
		go handlePrank(ctx, evt, args)

	case "playvideo":
		if !requireOwner(ctx, evt, ownerSender) {
			return
		}
		go handlePlayVideo(ctx, evt, args)

	case "stopvideo":
		if !requireOwner(ctx, evt, ownerSender) {
			return
		}
		handleStopVideo(ctx, evt, args)

	case "skipvideo":
		if !requireOwner(ctx, evt, ownerSender) {
			return
		}
		handleSkipVideo(ctx, evt, args)

	case "antrianvideo", "queuevideo":
		if !requirePublicOrOwner(ownerSender) {
			return
		}
		handleVideoQueue(ctx, evt)

	case "queue", "antrian":
		if !requirePublicOrOwner(ownerSender) {
			return
		}
		handleQueue(ctx, evt, args)
	}
}

func requirePublicOrOwner(owner bool) bool {
	return owner || getMode() == "public"
}

func requireOwner(ctx context.Context, evt *events.Message, owner bool) bool {
	if owner {
		return true
	}
	if getMode() == "public" {
		sendText(ctx, evt.Info.Chat, "🔒 Fitur ini cuma bisa dipakai *owner* bot.")
	}
	return false
}

// ─── Menu & Ping ─────────────────────────────────────────────────────────────

func menuText(ownerSender bool) string {
	modeLabel := "🌐 PUBLIC"
	if getMode() == "self" {
		modeLabel = "🔒 SELF"
	}
	nSender := len(pool.list())
	var b strings.Builder

	fmt.Fprintf(&b, "╭━━━━━━━━━━━━━━━╮\n")
	fmt.Fprintf(&b, "   🐱 *%s*\n", BotName)
	fmt.Fprintf(&b, "╰━━━━━━━━━━━━━━━╯\n")
	fmt.Fprintf(&b, "Mode   : %s\n", modeLabel)
	fmt.Fprintf(&b, "Sender : 🤖 %d akun\n\n", nSender)

	fmt.Fprintf(&b, "┌─ 『 *UMUM* 』\n")
	fmt.Fprintf(&b, "│ %sallmenu — menu ini\n", Prefix)
	fmt.Fprintf(&b, "│ %sping — cek latency & uptime\n", Prefix)
	fmt.Fprintf(&b, "│ %santrian [sender] — lihat antrian lagu\n", Prefix)
	fmt.Fprintf(&b, "└──────────────\n")

	if !ownerSender {
		return b.String()
	}

	fmt.Fprintf(&b, "\n┌─ 『 *PANGGILAN AUDIO* 』\n")
	fmt.Fprintf(&b, "│ %splaycall <judul> — reply chat target,\n", Prefix)
	fmt.Fprintf(&b, "│   atau: %splaycall 628xxx, judul\n", Prefix)
	fmt.Fprintf(&b, "│ %sskip [sender] — lanjut ke lagu berikutnya\n", Prefix)
	fmt.Fprintf(&b, "│ %sstopcall [sender] — akhiri call\n", Prefix)
	fmt.Fprintf(&b, "│ %sprank <sender> — reply audio, sisipin ke call\n", Prefix)
	fmt.Fprintf(&b, "└──────────────\n")

	fmt.Fprintf(&b, "\n┌─ 『 *PANGGILAN VIDEO* 』 (eksperimen)\n")
	fmt.Fprintf(&b, "│ %splayvideo <link YouTube>\n", Prefix)
	fmt.Fprintf(&b, "│ %sskipvideo [sender] — lanjut video berikutnya\n", Prefix)
	fmt.Fprintf(&b, "│ %sstopvideo [sender] — akhiri video call\n", Prefix)
	fmt.Fprintf(&b, "│ %santrianvideo — lihat antrian video\n", Prefix)
	fmt.Fprintf(&b, "└──────────────\n")

	fmt.Fprintf(&b, "\n┌─ 『 *SENDER* 』\n")
	fmt.Fprintf(&b, "│ %saddsender 628xxx — tambah akun penelpon\n", Prefix)
	fmt.Fprintf(&b, "│ %scanceladd <sender> — batalin pairing\n", Prefix)
	fmt.Fprintf(&b, "│ %slistsender — lihat status semua sender\n", Prefix)
	fmt.Fprintf(&b, "└──────────────\n")

	fmt.Fprintf(&b, "\n┌─ 『 *PENGATURAN* 』\n")
	fmt.Fprintf(&b, "│ %sself — bot cuma respon owner\n", Prefix)
	fmt.Fprintf(&b, "│ %spublic — command umum kebuka buat semua\n", Prefix)
	fmt.Fprintf(&b, "└──────────────\n")

	return b.String()
}

func handlePing(ctx context.Context, evt *events.Message) {
	delay := time.Since(evt.Info.Timestamp)
	if delay < 0 {
		delay = 0
	}
	sendText(ctx, evt.Info.Chat, fmt.Sprintf(
		"🏓 *Pong!*\n⏱️ Latency : %dms\n🕐 Uptime  : %s\n📡 Mode    : %s",
		delay.Milliseconds(), formatDuration(time.Since(startTime)), strings.ToUpper(getMode()),
	))
}

func formatDuration(d time.Duration) string {
	h, m, s := int(d.Hours()), int(d.Minutes())%60, int(d.Seconds())%60
	if h > 0 {
		return fmt.Sprintf("%dh %dm %ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

// ─── Queue commands ──────────────────────────────────────────────────────────

func handleQueue(ctx context.Context, evt *events.Message, args string) {
	sess, msg := pool.sessionFor(strings.TrimSpace(args))
	if sess == nil {
		sendText(ctx, evt.Info.Chat, msg)
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	var b strings.Builder
	fmt.Fprintf(&b, "📋 *Antrian Lagu*\n📞 +%s | 🤖 %s | 🔖 `%s`\n\n", sess.target, sess.sender.name, sess.shortID)
	if len(sess.queue) == 0 {
		fmt.Fprintf(&b, "_antrian kosong_")
	} else {
		for i, item := range sess.queue {
			fmt.Fprintf(&b, "%d. *%s* - %s (%s)\n", i+1, item.track.Title, item.track.Artist, item.track.Duration)
		}
	}
	sendText(ctx, evt.Info.Chat, b.String())
}

func handleSkip(ctx context.Context, evt *events.Message, args string) {
	sess, msg := pool.sessionFor(strings.TrimSpace(args))
	if sess == nil {
		sendText(ctx, evt.Info.Chat, msg)
		return
	}
	sess.mu.Lock()
	if sess.player == nil {
		sess.mu.Unlock()
		sendText(ctx, evt.Info.Chat, "📭 Belum ada yang diputar.")
		return
	}
	if len(sess.queue) == 0 {
		sess.mu.Unlock()
		sendText(ctx, evt.Info.Chat, "📭 Antrian kosong.")
		return
	}
	p := sess.player
	fn := sess.playNext
	sess.mu.Unlock()

	reactMsg(ctx, evt, "⏭️")
	p.Stop()
	if fn != nil {
		go fn()
	}
}

func handleStop(ctx context.Context, evt *events.Message, args string) {
	sess, msg := pool.sessionFor(strings.TrimSpace(args))
	if sess == nil {
		sendText(ctx, evt.Info.Chat, msg)
		return
	}
	sess.mu.Lock()
	call := sess.call
	for _, q := range sess.queue {
		os.Remove(q.file)
	}
	name := sess.sender.name
	sess.reset()
	sess.mu.Unlock()

	reactMsg(ctx, evt, "🛑")
	sendText(ctx, evt.Info.Chat, "🛑 Call *"+name+"* dihangup & antrian dibersihkan.")
	if call != nil {
		_ = call.Hangup()
	}
}

// ─── Playcall ────────────────────────────────────────────────────────────────

// normalizeJIDString parse sebuah JID string dan strip bagian device/AD-nya
// (misal "166692462301357:74@lid" -> "166692462301357@lid"). Ini PENTING:
// pesan yang di-reply di grup kadang ngasih Participant JID yang masih nempel
// device-suffix (AD) dari device tertentu si pengirim. Kalau suffix itu kebawa
// ke target call, WhatsApp kadang nyasar nyari device yang udah gak aktif --
// makanya call "kadang bisa kadang ga" tergantung device mana yang lagi
// online. Stripping ke NonAD (bare LID/JID) bikin resolusi konsisten.
func normalizeJIDString(raw string) string {
	if raw == "" {
		return raw
	}
	jid, err := types.ParseJID(raw)
	if err != nil {
		return raw
	}
	return jid.ToNonAD().String()
}

// resolveLIDToPhone: kalau JID-nya format @lid (WhatsApp LID/privacy ID, BUKAN nomor
// telepon), coba resolve ke nomor asli pakai LID store bawaan whatsmeow. Ini WAJIB
// buat .playcall/.playvideo — CallVideo/Call butuh NOMOR TELEPON asli, bukan LID,
// buat bisa benar-benar nelpon. Reply chat di grup SERING ngasih JID format @lid
// (fitur privasi WA yang nyembunyiin nomor asli di daftar partisipan grup), jadi ini
// resolusi WAJIB, bukan optional. Kalau gagal resolve (belum ada di cache whatsmeow),
// balikin JID aslinya + error biar caller bisa kasih pesan yang jelas ke user.
func resolveLIDToPhone(ctx context.Context, jid types.JID) (types.JID, error) {
	if jid.Server != types.HiddenUserServer { // bukan @lid, langsung balikin apa adanya
		return jid, nil
	}
	pn, err := waClient.Store.LIDs.GetPNForLID(ctx, jid)
	if err != nil {
		return jid, fmt.Errorf("gagal resolve LID ke nomor: %w", err)
	}
	if pn.IsEmpty() || pn.User == "" {
		return jid, fmt.Errorf("nomor asli belum ke-cache whatsmeow buat kontak ini (LID: %s) — coba reply chat lain dari orang yg sama, atau minta dia chat bot ini duluan sekali biar ke-cache", jid.User)
	}
	return pn, nil
}

func resolveTarget(ctx context.Context, evt *events.Message, args string) (target, title string, err error) {
	ctxInfo := evt.Message.GetExtendedTextMessage().GetContextInfo()
	quoted := ctxInfo.GetParticipant()

	if quoted == "" && ctxInfo.GetStanzaID() != "" && !evt.Info.IsGroup {
		quoted = evt.Info.Chat.String()
	}

	if quoted != "" {
		quoted = normalizeJIDString(quoted)
		jid, perr := types.ParseJID(quoted)
		if perr == nil {
			resolved, lerr := resolveLIDToPhone(ctx, jid)
			if lerr != nil {
				return "", "", lerr
			}
			quoted = resolved.ToNonAD().String()
		}
		title = strings.TrimSpace(args)
		if title == "" {
			return "", "", fmt.Errorf("judul kosong. contoh: reply chat orangnya + \"%splaycall Bertaut\"", Prefix)
		}
		return quoted, title, nil
	}
	parts := strings.SplitN(args, ",", 2)
	if len(parts) < 2 {
		return "", "", fmt.Errorf("format salah.\n• reply + \"%splaycall <judul>\"\n• atau \"%splaycall 628xxxx, <judul>\"", Prefix, Prefix)
	}
	number := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(parts[0]), "+"))
	number = strings.NewReplacer(" ", "", "-", "").Replace(number)
	title = strings.TrimSpace(parts[1])
	if number == "" || title == "" {
		return "", "", fmt.Errorf("nomor atau judul kosong")
	}
	return number, title, nil
}

func validateMP3(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() < 4096 {
		return fmt.Errorf("file terlalu kecil (%d bytes), kemungkinan corrupt", info.Size())
	}
	header := make([]byte, 3)
	if _, err := io.ReadFull(f, header); err != nil {
		return err
	}
	// ID3 tag atau frame sync MP3
	if header[0] != 0x49 && header[0] != 0xFF {
		return fmt.Errorf("bukan file MP3 valid (header: %x)", header)
	}
	return nil
}

func handlePlaycall(ctx context.Context, evt *events.Message, args string) {
	reactMsg(ctx, evt, "⏳")
	chat := evt.Info.Chat

	if ok, wait := checkCooldown(senderUser(evt)); !ok {
		reactMsg(ctx, evt, "❌")
		sendText(ctx, chat, fmt.Sprintf("⏳ Cooldown, tunggu %ds lagi.", int(wait.Seconds())+1))
		return
	}

	target, title, err := resolveTarget(ctx, evt, args)
	if err != nil {
		reactMsg(ctx, evt, "❌")
		sendText(ctx, chat, "❌ "+err.Error())
		return
	}
	cleanTarget := cleanJIDNumber(target)

	// Kalau target ini udah lagi ditelpon salah satu sender → lagunya masuk antrian
	// call itu (biar gak dobel-telpon orang yang sama).
	if existing := findSessionByTarget(cleanTarget); existing != nil {
		track, err := fetchSpotifyPlay(title)
		if err != nil {
			reactMsg(ctx, evt, "❌")
			sendText(ctx, chat, "❌ lagu gak ketemu: "+err.Error())
			return
		}
		file, err := downloadAudio(track.DownloadURL)
		if err != nil || validateMP3(file) != nil {
			os.Remove(file)
			reactMsg(ctx, evt, "❌")
			sendText(ctx, chat, "❌ gagal download/audio corrupt.")
			return
		}
		existing.mu.Lock()
		existing.queue = append(existing.queue, queueItem{track: track, file: file, evt: evt})
		pos := len(existing.queue)
		name, shortID := existing.sender.name, existing.shortID
		existing.mu.Unlock()
		reactMsg(ctx, evt, "✅")
		sendText(ctx, chat, fmt.Sprintf("🎵 *%s* - %s\n📋 Antrian #%d | 🤖 %s | 🔖 `%s`",
			track.Title, track.Artist, pos, name, shortID))
		return
	}

	// Ambil sender yang nganggur (multi-sender: sender1 sibuk → sender2, dst).
	snd := pool.acquireFree()
	if snd == nil {
		reactMsg(ctx, evt, "❌")
		sendText(ctx, chat, fmt.Sprintf(
			"❌ Semua sender lagi sibuk nelpon / gak ada yang online.\nTambah sender: *%saddsender 628xxxx*", Prefix))
		return
	}

	track, err := fetchSpotifyPlay(title)
	if err != nil {
		reactMsg(ctx, evt, "❌")
		sendText(ctx, chat, "❌ lagu gak ketemu: "+err.Error())
		return
	}

	file, err := downloadAudio(track.DownloadURL)
	if err != nil {
		reactMsg(ctx, evt, "❌")
		sendText(ctx, chat, "❌ gagal download: "+err.Error())
		return
	}
	if err := validateMP3(file); err != nil {
		os.Remove(file)
		reactMsg(ctx, evt, "❌")
		sendText(ctx, chat, "❌ audio corrupt, coba judul lain: "+err.Error())
		return
	}

	item := queueItem{track: track, file: file, evt: evt}

	// Bikin session baru di sender ini.
	sess := &callSession{
		sender:    snd,
		active:    true,
		target:    cleanTarget,
		requester: senderUser(evt),
		queue:     []queueItem{item},
		stopHB:    make(chan struct{}),
		done:      make(chan struct{}),
	}
	snd.mu.Lock()
	snd.sess = sess
	snd.mu.Unlock()

	sendText(ctx, chat, fmt.Sprintf("🎵 *%s* - %s\n📞 %s nelpon *+%s*...\n\n⏭️ Skip: *%sskip %s* | 🛑 Stop: *%sstopcall %s*",
		track.Title, track.Artist, snd.name, cleanTarget, Prefix, snd.name, Prefix, snd.name))
	fmt.Printf("[playcall] %s resolved target=%q cleanTarget=%q\n", snd.name, target, cleanTarget)

	var call *meowcaller.Call
	for attempt := 1; attempt <= 2; attempt++ {
		callCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		call, err = snd.call.Call(callCtx, target)
		cancel()
		if err == nil {
			break
		}
		fmt.Printf("[playcall] %s attempt %d gagal: %v\n", snd.name, attempt, err)
		if attempt < 2 {
			time.Sleep(2 * time.Second)
		}
	}
	if err != nil {
		reactMsg(ctx, evt, "❌")
		sendText(ctx, chat, "❌ gagal nelpon +"+cleanTarget+": "+err.Error())
		os.Remove(file)
		snd.mu.Lock()
		sess.reset()
		snd.sess = nil
		snd.mu.Unlock()
		return
	}

	shortID := call.ID()
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}

	sess.mu.Lock()
	sess.call = call
	sess.shortID = shortID
	sess.mu.Unlock()

	go keepCallAlive(snd.wa, call.ID(), sess.stopHB)

	// ── playNext closure ──────────────────────────────────────────────────────
	var playNextFn func()
	playNextFn = func() {
		sess.mu.Lock()
		if !sess.active || len(sess.queue) == 0 {
			sess.mu.Unlock()
			sendText(ctx, chat, fmt.Sprintf(
				"🎵 Semua lagu habis.\n📞 Call %s *+%s* masih aktif.\n➕ Tambah: *%splaycall <judul>*",
				snd.name, cleanTarget, Prefix))
			return
		}
		next := sess.queue[0]
		sess.queue = sess.queue[1:]
		p := sess.player
		prevFile := sess.curFile
		sess.mu.Unlock()

		if p == nil {
			os.Remove(next.file)
			return
		}

		src, err := meowcaller.MP3File(next.file)
		if err != nil {
			sendText(ctx, chat, fmt.Sprintf("❌ gagal load *%s*: %v", next.track.Title, err))
			os.Remove(next.file)
			playNextFn()
			return
		}

		// Simpen judul + file yang lagi diputar (buat prank resume). File lagu
		// SEBELUMnya baru dihapus sekarang (biar curFile masih valid selama diputer).
		sess.mu.Lock()
		sess.prankTitle = next.track.Title
		sess.curFile = next.file
		sess.mu.Unlock()
		if prevFile != "" && prevFile != next.file {
			os.Remove(prevFile)
		}

		sendText(ctx, chat, fmt.Sprintf(
			"▶️ *Now Playing*\n🎵 *%s* - %s (%s)\n🤖 %s | 🔖 `%s`",
			next.track.Title, next.track.Artist, next.track.Duration, snd.name, shortID))

		p.Play(src)
	}

	connected := make(chan struct{}, 1)

	call.OnReady(func() {
		p := meowcaller.NewPlayer()
		call.Subscribe(p)

		p.OnFinish(func() {
			// Kalau lagi prank, OnFinish-nya audio prank → balikin lagu asli (reload
			// file lagu yang lagi diputer dari awal).
			sess.mu.Lock()
			if sess.pranking {
				sess.pranking = false
				cf := sess.curFile
				pl := sess.player
				sess.mu.Unlock()
				if cf != "" && pl != nil {
					if src, e := meowcaller.MP3File(cf); e == nil {
						sendText(ctx, chat, "😹 Prank selesai, lagu dilanjutkan.")
						pl.Play(src)
						return
					}
				}
				playNextFn()
				return
			}
			sess.mu.Unlock()
			playNextFn()
		})

		sess.mu.Lock()
		sess.player = p
		sess.playNext = playNextFn
		sess.mu.Unlock()

		sendText(ctx, chat, fmt.Sprintf("✅ *%s tersambung ke +%s*\n🔖 `%s`", snd.name, cleanTarget, shortID))
		reactMsg(ctx, evt, "✅")

		select {
		case connected <- struct{}{}:
		default:
		}

		playNextFn()
	})

	call.OnEnd(func(reason string) {
		label := map[string]string{
			"timeout":      "timeout (gak diangkat)",
			"media_failed": "koneksi media gagal (relay gak nyambung)",
			"hangup":       "call ditutup",
			"":             "call ditutup",
		}[reason]
		if label == "" {
			label = reason
		}
		sendText(ctx, chat, fmt.Sprintf("📴 *Call %s Selesai*\n🔖 `%s` | %s", snd.name, shortID, label))
		reactMsg(ctx, evt, "📴")

		sess.mu.Lock()
		for _, q := range sess.queue {
			os.Remove(q.file)
		}
		sess.reset()
		sess.mu.Unlock()
		snd.mu.Lock()
		snd.sess = nil
		snd.mu.Unlock()

		sess.stopAll()
	})

	go func() {
		select {
		case <-connected:
		case <-time.After(45 * time.Second):
			select {
			case <-sess.done:
			default:
				sendText(ctx, chat, fmt.Sprintf("📵 *+%s* gak ngangkat (45s)\n🤖 %s | 🔖 `%s`", cleanTarget, snd.name, shortID))
				sess.mu.Lock()
				for _, q := range sess.queue {
					os.Remove(q.file)
				}
				sess.reset()
				sess.mu.Unlock()
				snd.mu.Lock()
				snd.sess = nil
				snd.mu.Unlock()
				_ = call.Hangup()
				sess.stopAll()
			}
		}
	}()

	<-sess.done
}

// findSessionByTarget cari session aktif yang lagi nelpon nomor target ini.
func findSessionByTarget(cleanTarget string) *callSession {
	for _, s := range pool.activeSessions() {
		s.mu.Lock()
		match := s.target == cleanTarget
		s.mu.Unlock()
		if match {
			return s
		}
	}
	return nil
}

// ─── Send / React ─────────────────────────────────────────────────────────────

func sendText(ctx context.Context, to types.JID, text string) {
	_, err := waClient.SendMessage(ctx, to, &waE2E.Message{
		Conversation: proto.String(text),
	})
	if err != nil {
		fmt.Println("[send] error:", to.String(), err)
	}
}

func reactMsg(ctx context.Context, evt *events.Message, emoji string) {
	_, err := waClient.SendMessage(ctx, evt.Info.Chat, &waE2E.Message{
		ReactionMessage: &waE2E.ReactionMessage{
			Key: &waCommon.MessageKey{
				RemoteJID:   proto.String(evt.Info.Chat.String()),
				FromMe:      proto.Bool(evt.Info.IsFromMe),
				ID:          proto.String(evt.Info.ID),
				Participant: proto.String(evt.Info.Sender.String()),
			},
			Text:              proto.String(emoji),
			SenderTimestampMS: proto.Int64(time.Now().UnixMilli()),
		},
	})
	if err != nil {
		fmt.Println("[react] error:", emoji, err)
	}
}
