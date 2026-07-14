package main

const (
	// OwnerNumber dipake buat command admin-only kalau nanti mau ditambah.
	// Format: kode negara + nomor, TANPA tanda "+", spasi, atau strip.
	OwnerNumber = "6285175272979"

	// BotNumber cuma buat informasi/README. whatsmeow login pake QR code,
	// jadi nomor ini gak dibaca langsung sama kode — yang penting pas scan QR
	// nanti pake WhatsApp yg login di nomor +62 889-9180-7515.
	BotNumber = "6288991807515"

	BotName = "YamzzBot"

	// Prefix command, misal "m!" -> m!allmenu, m!playcall
	Prefix = "m!"

	// Cooldown anti-spam buat .playcall, per pengirim.
	PlaycallCooldownSeconds = 30

	// TheresavAPIKey dipake buat command download video (theresav ytmp4, .playvideo).
	// Daftar dulu buat dapetin apikey-mu sendiri di https://api.theresav.biz.id
	// lalu isi di sini. Bisa juga di-override lewat environment THERESAV_APIKEY
	// (env selalu menang kalau keduanya keisi).
	//
	// 🙏 Bot ini TIDAK BERAFILIASI dengan API theresav — cuma pakai API-nya doang.
	TheresavAPIKey = "" // ← isi apikey kamu di sini

	// TheresavResolution: resolusi source yg didownload dari theresav (bukan resolusi
	// FINAL video call — itu tetep di-downscale ke 480p di videoencode.go buat
	// kompatibilitas decoder WA, JANGAN diubah di situ). Naikin ini ("1080") cuma
	// bikin hasil downscale-nya lebih tajam/bersih, karena source-nya lebih detail.
	TheresavResolution = "720"
)

func isOwner(number string) bool {
	return number == OwnerNumber
}
