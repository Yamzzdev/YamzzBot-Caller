package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

// API download video: theresav ytmp4. Butuh LINK YouTube + apikey (wajib).
//
// ⚠️ APIKEY diatur di config.go (TheresavAPIKey), BUKAN di file ini — biar semua
// setting bot ngumpul di satu tempat dan gampang dianalisis/di-audit.
// Daftar dulu buat dapet apikey-mu sendiri di: https://api.theresav.biz.id
//
// 🙏 Bot ini TIDAK BERAFILIASI dengan API theresav — cuma make API-nya doang.
const (
	theresavYtmp4URL = "https://api.theresav.biz.id/download/ytmp4"
)

// theresavAPIKey: pakai environment THERESAV_APIKEY kalau ada (buat deploy pakai
// secret manager), kalau nggak pakai TheresavAPIKey dari config.go.
func theresavAPIKey() string {
	if k := os.Getenv("THERESAV_APIKEY"); k != "" {
		return k
	}
	return TheresavAPIKey
}

// browserUA dipasang di request ke API & CDN download.
const browserUA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

// downloadClient MEMAKSA HTTP/1.1 (TLSNextProto kosong) — beberapa server (termasuk
// CDN theresav) nge-RST stream HTTP/2 dengan "INTERNAL_ERROR", yang tadi bikin
// "gagal nulis file: stream error". HTTP/1.1 nghindarin itu.
var downloadClient = &http.Client{
	Timeout: 300 * time.Second,
	Transport: &http.Transport{
		ForceAttemptHTTP2: false,
		TLSNextProto:      map[string]func(string, *tls.Conn) http.RoundTripper{},
	},
}

// videoTrack = respons API theresav (field-nya flat, gak nested). Field download_url
// itu link file mp4-nya.
type videoTrack struct {
	Status      bool   `json:"status"`
	Title       string `json:"title"`
	Channel     string `json:"channel"`
	DurationSec int    `json:"duration_sec"`
	Resolution  string `json:"resolution"`
	Ext         string `json:"ext"`
	Filesize    int64  `json:"filesize"`
	Thumbnail   string `json:"thumbnail"`
	DownloadURL string `json:"download_url"`
}

// fetchVideoPlay ambil metadata + link download dari API theresav. ytURL WAJIB link
// YouTube. apikey dikirim otomatis.
func fetchVideoPlay(ytURL string) (*videoTrack, error) {
	apikey := theresavAPIKey()
	if apikey == "" {
		return nil, fmt.Errorf("apikey theresav kosong — daftar dulu di https://api.theresav.biz.id, " +
			"terus isi ke TheresavAPIKey di config.go atau set env THERESAV_APIKEY")
	}
	endpoint := fmt.Sprintf("%s?url=%s&resolution=%s&apikey=%s",
		theresavYtmp4URL, url.QueryEscape(ytURL), TheresavResolution, url.QueryEscape(apikey))

	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", browserUA)

	httpClient := &http.Client{Timeout: 120 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request gagal: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API status %d", resp.StatusCode)
	}

	var parsed videoTrack
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("response gak valid: %w", err)
	}
	if !parsed.Status || parsed.DownloadURL == "" {
		return nil, fmt.Errorf("link gak ke-resolve — pastiin itu link YouTube valid")
	}
	return &parsed, nil
}

// downloadVideo download file video ke folder temp/, balikin path-nya. Retry 3x dan
// pakai HTTP/1.1 (via downloadClient) biar tahan "stream error INTERNAL_ERROR".
func downloadVideo(downloadURL string) (string, error) {
	if err := os.MkdirAll("temp", 0o755); err != nil {
		return "", err
	}
	path := filepath.Join("temp", uuid.New().String()+".mp4")

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		lastErr = downloadOnce(downloadURL, path)
		if lastErr == nil {
			return path, nil
		}
		fmt.Printf("[downloadVideo] attempt %d gagal: %v\n", attempt, lastErr)
		os.Remove(path)
		if attempt < 3 {
			time.Sleep(2 * time.Second)
		}
	}
	return "", lastErr
}

// downloadOnce sekali percobaan download ke path.
func downloadOnce(downloadURL, path string) error {
	req, err := http.NewRequest(http.MethodGet, downloadURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Accept", "*/*")

	resp, err := downloadClient.Do(req)
	if err != nil {
		return fmt.Errorf("request gagal: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}

	out, err := os.Create(path)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, resp.Body); err != nil {
		return fmt.Errorf("gagal nulis file: %w", err)
	}
	return nil
}
