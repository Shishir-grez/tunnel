package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"tunnel"
)

var (
	name            = flag.String("name", "peer", "display name in chat")
	signalType      = flag.String("signal", "mock", "signal type: mock or cloudflare")
	workerURL       = flag.String("worker", "", "Cloudflare Worker URL (required when -signal=cloudflare)")
	room            = flag.String("room", "", "room token; auto-generated and printed if empty (first peer)")
	pingInterval    = flag.Duration("ping-interval", 2*time.Second, "interval for tunnel RTT samples; set to 0 to disable")
	turnAccessToken = flag.String("turn-token", "", "Worker access token for TURN fallback (prefer TUNNEL_TURN_TOKEN)")
)

func main() {
	flag.Parse()

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalChan
		cancelFunc()
	}()

	signalClient, roomToken, err := newSignal(ctx)
	if err != nil {
		fmt.Printf("signal error: %s\n", err)
		return
	}

	var options []tunnel.Option
	accessToken := strings.TrimSpace(*turnAccessToken)
	if accessToken == "" {
		accessToken = strings.TrimSpace(os.Getenv("TUNNEL_TURN_TOKEN"))
	}
	if accessToken != "" {
		if *signalType != "cloudflare" {
			fmt.Println("TURN fallback requires -signal=cloudflare")
			return
		}
		turnConfig, err := fetchTURNConfig(ctx, *workerURL, roomToken, accessToken)
		if err != nil {
			fmt.Printf("TURN setup error: %s\n", err)
			return
		}
		options = append(options, tunnel.WithTURN(turnConfig))
		fmt.Println("TURN fallback enabled.")
	}

	t, err := tunnel.NewTunnel(ctx, signalClient, options...)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer t.Close()
	if err = t.Connect(); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	peer, err := t.ConnectHTTP2()
	if err != nil {
		fmt.Printf("ConnectHTTP2 error: %s\n", err)
		return
	}

	runChat(ctx, peer, t.TransportPath())
}

func newSignal(ctx context.Context) (tunnel.Signal, string, error) {
	switch *signalType {
	case "cloudflare":
		if *workerURL == "" {
			return nil, "", fmt.Errorf("-worker is required for cloudflare signal")
		}
		roomToken := *room
		if roomToken == "" {
			b := make([]byte, 3)
			if _, err := rand.Read(b); err != nil {
				return nil, "", err
			}
			roomToken = hex.EncodeToString(b)
			fmt.Printf("Room token: %s\n", roomToken)
			fmt.Printf("Start peer with: -signal=cloudflare -worker=%s -room=%s\n", *workerURL, roomToken)
			return NewCloudflareSignal(ctx, *workerURL, roomToken, "server"), roomToken, nil
		}
		return NewCloudflareSignal(ctx, *workerURL, roomToken, "client"), roomToken, nil
	default:
		return NewMockSignal(), "", nil
	}
}

func runChat(ctx context.Context, peer *tunnel.Peer, transportPath string) {
	peer.Handle("/ping", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	peer.Handle("/chat", func(w http.ResponseWriter, r *http.Request) {
		message, err := io.ReadAll(io.LimitReader(r.Body, 64*1024))
		if err != nil {
			http.Error(w, "read message", http.StatusBadRequest)
			return
		}
		if text := strings.TrimSpace(string(message)); text != "" {
			fmt.Println(text)
		}
		w.WriteHeader(http.StatusNoContent)
	})

	fmt.Printf("Connected via %s. Type messages and press Enter.\n", transportPath)
	startRTTProbe(ctx, peer, *pingInterval)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		message := fmt.Sprintf("[%s] %s", *name, line)
		fmt.Println(message)

		req, err := http.NewRequestWithContext(
			ctx,
			http.MethodPost,
			"https://tunnel/chat",
			strings.NewReader(message),
		)
		if err != nil {
			fmt.Printf("send error: %s\n", err)
			continue
		}
		req.Header.Set("Content-Type", "text/plain; charset=utf-8")
		resp, err := peer.Client.Do(req)
		if err != nil {
			fmt.Printf("send error: %s\n", err)
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			fmt.Printf("send error: remote returned %s\n", resp.Status)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("console error: %s\n", err)
	}
}

func startRTTProbe(ctx context.Context, peer *tunnel.Peer, interval time.Duration) {
	if interval <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				start := time.Now()
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://tunnel/ping", nil)
				if err != nil {
					fmt.Printf("tunnel rtt error: %s\n", err)
					continue
				}
				resp, err := peer.Client.Do(req)
				if err != nil {
					fmt.Printf("tunnel rtt error: %s\n", err)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusNoContent {
					fmt.Printf("tunnel rtt error: unexpected status %d\n", resp.StatusCode)
					continue
				}
				fmt.Printf("tunnel rtt: %s\n", time.Since(start).Round(time.Millisecond))
			}
		}
	}()
}
