package main

import (
	"context"
	"fmt"
	"time"

	"github.com/purpshell/meowcaller/signaling"
	"go.mau.fi/whatsmeow"
)

// keepCallAlive kirim heartbeat tiap 8 detik biar call gak auto-mati. Pakai client
// SENDER yang naruh call ini (bukan selalu bot utama) — heartbeat harus dari akun
// yang bikin call-nya.
func keepCallAlive(wa *whatsmeow.Client, callID string, stop <-chan struct{}) {
	self := wa.Store.GetLID()
	if self.IsEmpty() {
		fmt.Println("[heartbeat] self LID kosong, heartbeat tidak berjalan")
		return
	}

	ticker := time.NewTicker(8 * time.Second)
	defer ticker.Stop()

	fmt.Println("[heartbeat] mulai untuk call", callID)
	for {
		select {
		case <-stop:
			fmt.Println("[heartbeat] stop untuk call", callID)
			return
		case <-ticker.C:
			wrapperID := wa.GenerateMessageID()
			node := signaling.BuildHeartbeat(callID, self, wrapperID)
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			err := wa.DangerousInternals().SendNode(ctx, node)
			cancel()
			if err != nil {
				fmt.Println("[heartbeat] error:", err)
			} else {
				fmt.Println("[heartbeat] ok", callID[:8])
			}
		}
	}
}
