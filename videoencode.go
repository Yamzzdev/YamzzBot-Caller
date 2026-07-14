package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/purpshell/meowcaller/rtp"
)

// videoFPS HARUS sinkron sama asumsi 90kHz/15fps di meowcaller (engine_media.go,
// videoRtpStepSamples). Kalau lib-nya nanti expose fps custom, ganti di sini juga.
const videoFPS = 15

// maxVideoSeconds ngebatesin durasi yang di-encode & di-loop. Video LEBIH PANJANG
// dari ini bakal KEPOTONG (cuma N detik pertama yang di-encode, abis itu loop dari
// situ) — itu BUKAN bug kalau kejadian, itu emang batesnya. Naikin nilainya kalau mau
// video lebih panjang, TAPI ada trade-off:
//   - Encode makan waktu lebih lama SEBELUM nelpon mulai (proporsional ke durasi)
//   - loadAccessUnits() baca SEMUA frame ke RAM sekaligus (bukan streaming dari disk),
//     jadi makin panjang = makin banyak RAM kepake. 600s (10 menit) di 480p/1200kbps
//     ≈ 100-150MB, aman buat VPS biasa. Kalau mau lebih dari ~20 menit, pertimbangin
//     refactor loadAccessUnits jadi streaming (baca+kirim per-frame, bukan load semua).
const maxVideoSeconds = 600

// probeDuration ngukur durasi asli sebuah file media (detik) pakai ffprobe. Dipake
// buat diagnostic ngebandingin durasi video vs audio hasil extract — kalau beda jauh,
// itu penyebab audio/video keliatan "gak sinkron" pas di-loop (lihat buildVideoAssets).
func probeDuration(path string) (float64, error) {
	out, err := exec.Command("ffprobe", "-v", "error", "-show_entries", "format=duration",
		"-of", "csv=p=0", path).Output()
	if err != nil {
		return 0, err
	}
	var sec float64
	_, err = fmt.Sscanf(strings.TrimSpace(string(out)), "%f", &sec)
	return sec, err
}

// checkFFmpeg mastiin binary ffmpeg ada di PATH sebelum kepake.
func checkFFmpeg() error {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return fmt.Errorf("ffmpeg gak ketemu di PATH — install dulu: sudo apt install -y ffmpeg")
	}
	return nil
}

// extractVideoAssets ngubah mp4 jadi (1) raw H.264 Annex-B elementary stream buat
// SendVideo(), dan (2) mp3 audio track buat Player biasa (sama kayak alur .playcall).
//
// SATU invokasi ffmpeg buat DUA output sekaligus → input cuma di-decode sekali (dulu
// dua pass terpisah, decode input 2x, itu salah satu sumber "encode super lama").
//
// Kunci kecepatan & kualitas tampil:
//   - preset ultrafast + tune zerolatency: dulu preset gak diset = default "medium"
//     yang lambat banget. ultrafast bisa 5-10x lebih cepat buat streaming realtime.
//   - -threads 0: pakai semua core.
//   - keyframe tiap `videoFPS` frame (1 IDR/detik) + repeat-headers (SPS/PPS ditaruh
//     ulang di depan tiap IDR). Dulu cuma ADA 1 keyframe di awal video — decoder H.264
//     wajib mulai dari IDR+SPS/PPS, jadi peer yang angkat telat gak akan pernah bisa
//     nge-decode (layar item). Dengan keyframe periodik, peer nge-sync di IDR
//     berikutnya (≤1 detik) berapapun telatnya dia angkat.
func extractVideoAssets(mp4Path string) (h264Path, audioPath string, err error) {
	base := strings.TrimSuffix(mp4Path, filepath.Ext(mp4Path))
	h264Path = base + ".h264"
	audioPath = base + ".mp3"

	keyint := fmt.Sprintf("%d", videoFPS)
	args := []string{
		"-y", "-hide_banner", "-loglevel", "error",
		"-t", fmt.Sprintf("%d", maxVideoSeconds), // input-side: berhenti decode setelah N detik
		"-i", mp4Path,
		// ── output #1: video H.264 Annex-B ──
		// Encode langsung ke raw H.264 ("-f h264") = bitstream udah Annex-B; JANGAN
		// pasang bsf h264_mp4toannexb di sini (itu buat sumber AVCC/mp4, di output raw
		// dia no-op / bisa error). SPS/PPS in-band diurus repeat-headers=1 + injeksi di
		// loadAccessUnits.
		"-map", "0:v:0",
		"-an",
		"-c:v", "libx264",
		// veryfast (bukan ultrafast): x264 kompres jauh lebih efisien di preset ini —
		// kualitas per-bit naik banyak, encode masih cukup cepat buat streaming.
		"-preset", "veryfast",
		"-tune", "zerolatency",
		// Match a REAL WhatsApp video stream (decrypted live from a genuine WA client):
		// profile Main dikonfirmasi diterima decoder WA (JANGAN ganti ke baseline/high).
		// Resolusi 480p (turun dari percobaan 720p yg bikin freeze/smear — bitrate+paket
		// per keyframe kegedean buat di-burst realtime, lihat pacing fix di
		// engine_media.go). 480p + bitrate moderat = keseimbangan antara "gak pecah"
		// dan "gak nge-lag/freeze".
		"-profile:v", "main",
		"-level", "3.1",
		"-pix_fmt", "yuv420p",
		"-r", keyint,
		"-vf", "scale=-2:480",
		"-g", keyint, // IDR tiap 1 detik
		"-keyint_min", keyint,
		"-sc_threshold", "0", // jangan sisipin keyframe ekstra dari scene-cut
		// repeat-headers=1: SPS/PPS in-band di depan tiap IDR.
		// slices=1 + sliced-threads=0: SATU slice NAL per frame. Tune zerolatency defaultnya
		// nyalain sliced-threads (banyak slice/frame); kalau dibiarin, loadAccessUnits (yang
		// nutup access unit per VCL slice) bakal salah anggap tiap slice = 1 frame → pacing
		// kacau & marker RTP ke-set di tengah gambar (decoder lihat frame kepotong → glitch).
		"-x264-params", "repeat-headers=1:bframes=0:open-gop=0:sliced-threads=0:slices=1",
		// Bitrate moderat: cukup buat gak pecah di 480p, gak segede kemarin yang bikin
		// keyframe overload burst-send.
		"-b:v", "1200k", "-maxrate", "1800k", "-bufsize", "3600k",
		"-threads", "0",
		"-f", "h264", h264Path,
		// ── output #2: audio mp3 mono 16k (opsional: '?' = jangan gagal kalau video
		// gak punya track audio) ──
		// ⚠️ loudnorm SENGAJA DICABUT (sempet dipasang buat normalisasi volume) —
		// filter itu punya lookahead/delay yang beresiko geser durasi output dikit,
		// dicurigai jadi salah satu penyebab audio "abis duluan" drpd video pas di-loop
		// (lihat playvideo.go: audio & video sama-sama loop tapi independent, kalau
		// durasinya beda dikit aja lama-lama kerasa gak sinkron). Sample rate TETEP
		// 16kHz mono, itu hard requirement codec MLow WhatsApp, JANGAN diubah.
		"-map", "0:a:0?",
		"-vn", "-ac", "1", "-ar", "16000",
		"-codec:a", "libmp3lame", "-q:a", "2", audioPath,
	}
	var stderr bytes.Buffer
	cmd := exec.Command("ffmpeg", args...)
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		os.Remove(h264Path)
		os.Remove(audioPath)
		return "", "", fmt.Errorf("ffmpeg encode gagal: %w\n%s", err, lastLines(stderr.String(), 6))
	}

	return h264Path, audioPath, nil
}

// lastLines motong output ffmpeg biar pesan error gak kepanjangan pas dikirim ke WA.
func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// loadAccessUnits baca file H.264 Annex-B, pecah jadi NAL unit, terus digabung ulang
// per access unit (satu "frame"): NAL non-VCL (AUD/SPS/PPS/SEI) yang mendahului
// disatuin sama NAL VCL (slice, type 1 atau 5) berikutnya jadi satu unit yang dikirim
// bareng ke call.SendVideo(). Start code Annex-B (0x00000001) ditambahin lagi di
// depan tiap NAL pas digabung, karena rtp.SplitAnnexB nyopot start code aslinya.
func loadAccessUnits(h264Path string) ([][]byte, error) {
	raw, err := os.ReadFile(h264Path)
	if err != nil {
		return nil, err
	}
	nalus := rtp.SplitAnnexB(raw)
	if len(nalus) == 0 {
		return nil, fmt.Errorf("gak ada NAL unit ke-parse dari %s", h264Path)
	}

	// SPS/PPS terakhir yang kelihatan, buat di-inject ulang ke IDR yang gak bawa
	// parameter set (jaga-jaga kalau repeat-headers gak ke-honor sama build ffmpeg).
	var sps, pps []byte
	var aus [][]byte
	var pending [][]byte
	for _, n := range nalus {
		if len(n) == 0 {
			continue
		}
		naluType := n[0] & 0x1F
		switch naluType {
		case 7:
			sps = append([]byte(nil), n...)
		case 8:
			pps = append([]byte(nil), n...)
		}
		pending = append(pending, n)
		if naluType == 1 || naluType == 5 { // slice non-IDR / IDR -> penutup access unit
			if naluType == 5 {
				pending = ensureParamSets(pending, sps, pps)
			}
			aus = append(aus, joinAnnexB(pending))
			pending = nil
		}
	}
	if len(pending) > 0 {
		aus = append(aus, joinAnnexB(pending))
	}
	if len(aus) == 0 {
		return nil, fmt.Errorf("gak ada access unit ke-bentuk dari %s", h264Path)
	}
	return aus, nil
}

// ensureParamSets mastiin access unit IDR bawa SPS+PPS di depannya; kalau belum ada
// (dan kita udah pernah lihat SPS/PPS), di-prepend biar decoder peer bisa mulai dari
// IDR ini tanpa nunggu awal stream.
func ensureParamSets(au [][]byte, sps, pps []byte) [][]byte {
	var hasSPS, hasPPS bool
	for _, n := range au {
		switch n[0] & 0x1F {
		case 7:
			hasSPS = true
		case 8:
			hasPPS = true
		}
	}
	var prefix [][]byte
	if !hasSPS && sps != nil {
		prefix = append(prefix, sps)
	}
	if !hasPPS && pps != nil {
		prefix = append(prefix, pps)
	}
	if len(prefix) == 0 {
		return au
	}
	return append(prefix, au...)
}

// joinAnnexB nempelin start code 4-byte di depan tiap NAL lalu digabung jadi satu
// access unit siap kirim.
func joinAnnexB(nalus [][]byte) []byte {
	var buf bytes.Buffer
	for _, n := range nalus {
		buf.Write([]byte{0x00, 0x00, 0x00, 0x01})
		buf.Write(n)
	}
	return buf.Bytes()
}
