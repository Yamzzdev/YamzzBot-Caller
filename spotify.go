package main

import (
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

const nexraySpotifyPlayURL = "https://api.nexray.eu.cc/downloader/spotifyplay"

type spotifyTrack struct {
	Title       string `json:"title"`
	Artist      string `json:"artist"`
	Duration    string `json:"duration"`
	Thumbnail   string `json:"thumbnail"`
	Album       string `json:"album"`
	URL         string `json:"url"`
	DownloadURL string `json:"download_url"`
}

type nexrayResponse struct {
	Status bool         `json:"status"`
	Result spotifyTrack `json:"result"`
}

// fetchSpotifyPlay query lagu by judul ke API nexray, balikin metadata + download_url.
func fetchSpotifyPlay(query string) (*spotifyTrack, error) {
	endpoint := nexraySpotifyPlayURL + "?q=" + url.QueryEscape(query)

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Get(endpoint)
	if err != nil {
		return nil, fmt.Errorf("request gagal: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API status %d", resp.StatusCode)
	}

	var parsed nexrayResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("response gak valid: %w", err)
	}
	if !parsed.Status || parsed.Result.DownloadURL == "" {
		return nil, fmt.Errorf("lagu \"%s\" gak ketemu", query)
	}
	return &parsed.Result, nil
}

// downloadAudio ngambil file audio dari download_url, simpen ke folder temp/, balikin path-nya.
func downloadAudio(downloadURL string) (string, error) {
	httpClient := &http.Client{Timeout: 90 * time.Second}
	resp, err := httpClient.Get(downloadURL)
	if err != nil {
		return "", fmt.Errorf("gagal request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}

	if err := os.MkdirAll("temp", 0o755); err != nil {
		return "", err
	}

	path := filepath.Join("temp", uuid.New().String()+".mp3")
	out, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer out.Close()

	if _, err := io.Copy(out, resp.Body); err != nil {
		os.Remove(path)
		return "", fmt.Errorf("gagal nulis file: %w", err)
	}
	return path, nil
}
