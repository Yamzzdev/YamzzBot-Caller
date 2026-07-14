package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mdp/qrterminal/v3"
	meowcaller "github.com/purpshell/meowcaller"
	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"

	_ "modernc.org/sqlite"
)

// waClient & callClient = shortcut ke sender UTAMA (pool.senders[0]). Kode lama
// (playvideo, dll) masih pakai ini; playcall multi-sender pakai pool langsung.
var (
	waClient   *whatsmeow.Client
	callClient *meowcaller.Client
	startTime  = time.Now()
)

func main() {
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "15:04:05"}).
		Level(zerolog.InfoLevel).
		With().Timestamp().Logger()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx = logger.WithContext(ctx)

	container, err := sqlstore.New(
		ctx, "sqlite",
		"file:yamzzbot.db?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)",
		waLog.Zerolog(logger).Sub("db"),
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("gagal buka session store (yamzzbot.db)")
	}

	pool.container = container
	pool.logger = logger

	// Load SEMUA device yang udah login → jadi sender pool. Device pertama = bot utama.
	devices, err := container.GetAllDevices(ctx)
	if err != nil {
		logger.Fatal().Err(err).Msg("gagal load devices dari session store")
	}

	if len(devices) == 0 {
		// Belum ada akun sama sekali → login bot utama via QR.
		device := container.NewDevice()
		devices = []*store.Device{device}
		firstTimeQRLogin(ctx, logger, device)
	}

	pool.initFromDevices(ctx, devices)

	// Shortcut ke sender utama buat kode lama (playvideo).
	mainS := pool.main()
	waClient = mainS.wa
	callClient = mainS.call

	// Connect semua sender.
	pool.connectAll()

	if err := waitUntilReady(ctx, waClient, 60*time.Second); err != nil {
		logger.Fatal().Err(err).Msg("timeout nunggu koneksi siap")
	}
	if waClient.Store.PushName == "" {
		waClient.Store.PushName = BotName
	}

	logger.Info().
		Str("self", waClient.Store.GetLID().String()).
		Int("senders", len(pool.list())).
		Msg("YamzzBot online, siap nerima command")
	fmt.Printf("\nYamzzBot jalan (%d sender). Kirim \"%sallmenu\". Ctrl+C buat stop.\n", len(pool.list()), Prefix)

	<-ctx.Done()
	for _, s := range pool.list() {
		s.wa.Disconnect()
	}
}

// firstTimeQRLogin nampilin QR buat login bot utama pertama kali. Client-nya di-connect
// di sini, di-disconnect abis pairing (nanti di-reconnect via pool.connectAll).
func firstTimeQRLogin(ctx context.Context, logger zerolog.Logger, device *store.Device) {
	cli := whatsmeow.NewClient(device, waLog.Zerolog(logger).Sub("qr"))
	qrChan, _ := cli.GetQRChannel(ctx)
	if err := cli.Connect(); err != nil {
		logger.Fatal().Err(err).Msg("gagal connect buat QR login")
	}
	for evt := range qrChan {
		if evt.Event == "code" {
			fmt.Println("\n========================================")
			fmt.Println(" SCAN QR INI DI WHATSAPP > LINKED DEVICES ")
			fmt.Println("========================================")
			qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
			fmt.Printf("Kode valid %d detik, scan sebelum habis.\n\n", int(evt.Timeout.Seconds()))
		} else {
			logger.Info().Str("event", evt.Event).Msg("login event")
			if evt.Event == "success" {
				break
			}
		}
	}
	cli.Disconnect()
	time.Sleep(2 * time.Second) // kasih waktu store ke-flush
}

// waitUntilReady nunggu sampai client beneran connected + logged in.
func waitUntilReady(ctx context.Context, client *whatsmeow.Client, timeout time.Duration) error {
	ready := make(chan struct{}, 8)
	id := client.AddEventHandler(func(evt any) {
		if _, ok := evt.(*events.Connected); ok {
			select {
			case ready <- struct{}{}:
			default:
			}
		}
	})
	defer client.RemoveEventHandler(id)

	deadline := time.After(timeout)
	for !(client.IsConnected() && client.IsLoggedIn()) {
		select {
		case <-ready:
		case <-deadline:
			return fmt.Errorf("timed out waiting for connection")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
