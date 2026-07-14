package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	meowcaller "github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow/types/events"
)

// ─── addsender ────────────────────────────────────────────────────────────────

// handleAddSender bikin sender baru & mulai pairing. Balikin pairing code buat
// dimasukin di HP: WhatsApp → Perangkat Tertaut → Tautkan dgn nomor telepon.
func handleAddSender(ctx context.Context, evt *events.Message, args string) {
	chat := evt.Info.Chat
	phone := strings.NewReplacer(" ", "", "-", "", "+", "").Replace(strings.TrimSpace(args))
	if len(phone) < 8 {
		sendText(ctx, chat, fmt.Sprintf("❌ Format: *%saddsender 628xxxxxxxx* (nomor lengkap + kode negara)", Prefix))
		return
	}

	reactMsg(ctx, evt, "⏳")
	code, name, err := pool.addSender(ctx, phone)
	if err != nil {
		reactMsg(ctx, evt, "❌")
		sendText(ctx, chat, "❌ gagal mulai pairing: "+err.Error())
		return
	}
	reactMsg(ctx, evt, "✅")
	sendText(ctx, chat, fmt.Sprintf(
		"🔗 *Pairing %s* (+%s)\n\nKode: *%s*\n\n"+
			"Di HP nomor itu:\nWhatsApp → *Perangkat Tertaut* → *Tautkan perangkat* → "+
			"*Tautkan dengan nomor telepon* → masukin kode di atas.\n\n"+
			"⏱️ Buruan, kode cuma valid ~1 menit. Cek *%slistsender* buat mastiin udah connect.\n"+
			"Salah nomor? Batalin: *%scanceladd %s*",
		name, phone, code, Prefix, Prefix, name,
	))
}

// ─── listsender ───────────────────────────────────────────────────────────────

func handleListSender(ctx context.Context, evt *events.Message) {
	senders := pool.list()
	if len(senders) == 0 {
		sendText(ctx, evt.Info.Chat, "📭 Belum ada sender.")
		return
	}

	var b strings.Builder
	fmt.Fprintf(&b, "🤖 *Daftar Sender* (%d)\n\n", len(senders))
	for _, s := range senders {
		status := "🔴 Offline"
		if s.connected() {
			status = "🟢 Online"
		}
		call := "nganggur"
		s.mu.Lock()
		if s.sess != nil && s.sess.active {
			call = "📞 nelpon +" + s.sess.target
		} else if s.videoSess != nil && s.videoSess.isActive() {
			call = "🎬 video call"
		}
		s.mu.Unlock()
		fmt.Fprintf(&b, "• *%s* (+%s)\n  %s | %s\n", s.name, s.number(), status, call)
	}
	sendText(ctx, evt.Info.Chat, b.String())
}

// ─── prank ────────────────────────────────────────────────────────────────────

// handlePrank: reply sebuah audio/voice-note + "m!prank sender1". Audio itu di-swap
// ke call sender1 (motong lagu yang lagi jalan sebentar), abis audio prank habis,
// lagu lama lanjut lagi. 😹
func handlePrank(ctx context.Context, evt *events.Message, args string) {
	chat := evt.Info.Chat
	fields := strings.Fields(args)
	if len(fields) == 0 {
		sendText(ctx, chat, fmt.Sprintf("❌ Reply sebuah AUDIO + ketik *%sprank sender1*", Prefix))
		return
	}
	name := strings.ToLower(fields[0])

	sess, msg := pool.sessionFor(name)
	if sess == nil {
		sendText(ctx, chat, msg)
		return
	}

	// Ambil audio yang di-reply.
	ci := evt.Message.GetExtendedTextMessage().GetContextInfo()
	quoted := ci.GetQuotedMessage()
	if quoted == nil {
		sendText(ctx, chat, "❌ Kamu harus REPLY ke pesan audio/voice note-nya.")
		return
	}
	audio := quoted.GetAudioMessage()
	if audio == nil {
		sendText(ctx, chat, "❌ Yang di-reply bukan audio/voice note.")
		return
	}

	reactMsg(ctx, evt, "⏳")
	raw, err := waClient.Download(ctx, audio)
	if err != nil {
		reactMsg(ctx, evt, "❌")
		sendText(ctx, chat, "❌ gagal download audio prank: "+err.Error())
		return
	}

	// WA audio biasanya ogg/opus → convert ke mp3 buat MP3File.
	mp3Path, err := convertToMP3(raw)
	if err != nil {
		reactMsg(ctx, evt, "❌")
		sendText(ctx, chat, "❌ gagal convert audio: "+err.Error())
		return
	}

	src, err := meowcaller.MP3File(mp3Path)
	if err != nil {
		os.Remove(mp3Path)
		reactMsg(ctx, evt, "❌")
		sendText(ctx, chat, "❌ gagal load audio prank: "+err.Error())
		return
	}

	sess.mu.Lock()
	if sess.player == nil {
		sess.mu.Unlock()
		os.Remove(mp3Path)
		sendText(ctx, chat, "❌ Call-nya belum tersambung / belum ada yang diputar.")
		return
	}
	sess.pranking = true
	p := sess.player
	orig := sess.prankTitle
	sess.mu.Unlock()

	reactMsg(ctx, evt, "😹")
	sendText(ctx, chat, fmt.Sprintf("😹 *PRANK* ke call %s!\n(abis ini *%s* lanjut lagi)", name, orig))
	p.Play(src) // OnFinish di playcall bakal resume lagu lama pas prank habis
}

// convertToMP3 nulis raw audio ke temp lalu convert ke mp3 mono 16k via ffmpeg.
func convertToMP3(raw []byte) (string, error) {
	if err := os.MkdirAll("temp", 0o755); err != nil {
		return "", err
	}
	inPath := filepath.Join("temp", "prank_"+uuid.New().String())
	if err := os.WriteFile(inPath, raw, 0o644); err != nil {
		return "", err
	}
	defer os.Remove(inPath)

	outPath := inPath + ".mp3"
	var stderr bytes.Buffer
	cmd := exec.Command("ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
		"-i", inPath, "-ac", "1", "-ar", "16000", "-codec:a", "libmp3lame", outPath)
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		os.Remove(outPath)
		return "", fmt.Errorf("ffmpeg: %s", lastLines(stderr.String(), 3))
	}
	return outPath, nil
}
