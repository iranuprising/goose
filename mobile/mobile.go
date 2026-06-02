// Package mobile provides the GooseRelay VPN core bridge for Android/iOS.
package mobile

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/kianmhz/GooseRelayVPN/internal/carrier"
	"github.com/kianmhz/GooseRelayVPN/internal/config"
	"github.com/kianmhz/GooseRelayVPN/internal/session"
	"github.com/kianmhz/GooseRelayVPN/internal/socks"
)

var (
	cancelFunc context.CancelFunc
	carr       *carrier.Client
	mu         sync.Mutex
	activeWG   sync.WaitGroup
	isStopping bool
)

type MobileConfig struct {
	ScriptURLs     []string `json:"script_urls"`
	ScriptAccounts []string `json:"script_accounts"`
	AESKeyHex      string   `json:"aes_key_hex"`
	GoogleIP       string   `json:"google_ip"`
	SNIHosts       []string `json:"sni_hosts"`
	ListenAddr     string   `json:"listen_addr"`
	UseFronting    bool     `json:"use_fronting"`
	SocksUser      string   `json:"socks_user"`
	SocksPass      string   `json:"socks_pass"`
}

func init() {
	log.SetFlags(0)
}

// Start initializes and starts the GooseRelay VPN core.
func Start(configJSON string) string {
	mu.Lock()
	if cancelFunc != nil {
		mu.Unlock()
		return "already running"
	}
	isStopping = false
	mu.Unlock()

	log.Printf("[mobile] Start called")

	var mcfg MobileConfig
	if err := json.Unmarshal([]byte(configJSON), &mcfg); err != nil {
		return fmt.Errorf("parse config: %v", err).Error()
	}

	c, err := carrier.New(carrier.Config{
		ScriptURLs:     mcfg.ScriptURLs,
		ScriptAccounts: mcfg.ScriptAccounts,
		AESKeyHex:      mcfg.AESKeyHex,
		Fronting: carrier.FrontingConfig{
			GoogleIP: mcfg.GoogleIP,
			SNIHosts: mcfg.SNIHosts,
		},
	})
	if err != nil {
		return fmt.Errorf("carrier init: %v", err).Error()
	}

	ctx, cancel := context.WithCancel(context.Background())
	
	mu.Lock()
	cancelFunc = cancel
	carr = c
	mu.Unlock()

	activeWG.Add(1)
	go func() {
		defer activeWG.Done()
		if err := carr.Run(ctx); err != nil && ctx.Err() == nil {
			log.Printf("[mobile] carrier run fatal: %v", err)
		}
	}()

	factory := socks.SessionFactory(func(target string) *session.Session {
		return carr.NewSession(target)
	})

	activeWG.Add(1)
	go func() {
		defer activeWG.Done()
		if err := socks.Serve(ctx, mcfg.ListenAddr, mcfg.SocksUser, mcfg.SocksPass, false, factory); err != nil && ctx.Err() == nil {
			log.Printf("[mobile] socks serve fatal: %v", err)
		}
	}()

	return ""
}

// Stop shuts down the GooseRelay VPN core.
func Stop() {
	mu.Lock()
	if cancelFunc == nil || isStopping {
		mu.Unlock()
		return
	}
	isStopping = true
	cf := cancelFunc
	cancelFunc = nil
	mu.Unlock()

	cf()

	done := make(chan struct{})
	go func() {
		activeWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("[mobile] all workers exited cleanly")
	case <-time.After(5 * time.Second):
		log.Printf("[mobile] timeout waiting for workers")
	}

	mu.Lock()
	carr = nil
	isStopping = false
	mu.Unlock()
}

// IsRunning returns true if the GooseRelay core is active.
func IsRunning() bool {
	mu.Lock()
	defer mu.Unlock()
	return cancelFunc != nil
}

// GetStats returns current traffic statistics as a JSON string.
func GetStats() string {
	mu.Lock()
	defer mu.Unlock()
	if carr == nil {
		return "{}"
	}
	in, out := carr.GetStats()
	stats := map[string]interface{}{
		"bytesIn":  in,
		"bytesOut": out,
	}
	res, _ := json.Marshal(stats)
	return string(res)
}
