package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"tunnel"
)

type iceServer struct {
	URLs       []string `json:"-"`
	Username   string   `json:"username"`
	Credential string   `json:"credential"`
}

func (s *iceServer) UnmarshalJSON(data []byte) error {
	type wireServer struct {
		URLs       json.RawMessage `json:"urls"`
		Username   string          `json:"username"`
		Credential string          `json:"credential"`
	}
	var wire wireServer
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	s.Username = wire.Username
	s.Credential = wire.Credential
	if err := json.Unmarshal(wire.URLs, &s.URLs); err == nil {
		return nil
	}
	var singleURL string
	if err := json.Unmarshal(wire.URLs, &singleURL); err != nil {
		return err
	}
	s.URLs = []string{singleURL}
	return nil
}

func fetchTURNConfig(ctx context.Context, workerURL, room, accessToken string) (tunnel.TURNConfig, error) {
	endpoint := fmt.Sprintf("%s/turn-credentials?room=%s", strings.TrimRight(workerURL, "/"), room)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return tunnel.TURNConfig{}, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return tunnel.TURNConfig{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return tunnel.TURNConfig{}, fmt.Errorf("worker returned %s: %s", resp.Status, strings.TrimSpace(string(message)))
	}

	var payload struct {
		ICEServers []iceServer `json:"iceServers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return tunnel.TURNConfig{}, fmt.Errorf("decode TURN credentials: %w", err)
	}
	return selectUDPTURNConfig(payload.ICEServers)
}

func selectUDPTURNConfig(servers []iceServer) (tunnel.TURNConfig, error) {
	for _, server := range servers {
		if server.Username == "" || server.Credential == "" {
			continue
		}
		for _, rawURL := range server.URLs {
			address, ok := udpTURNAddress(rawURL)
			if !ok {
				continue
			}
			return tunnel.TURNConfig{
				Server:   address,
				Username: server.Username,
				Password: server.Credential,
			}, nil
		}
	}
	return tunnel.TURNConfig{}, fmt.Errorf("credential response contained no TURN-over-UDP server")
}

func udpTURNAddress(rawURL string) (string, bool) {
	if !strings.HasPrefix(rawURL, "turn:") || strings.HasPrefix(rawURL, "turns:") {
		return "", false
	}
	parts := strings.SplitN(strings.TrimPrefix(rawURL, "turn:"), "?", 2)
	if len(parts) == 2 && !strings.Contains(parts[1], "transport=udp") {
		return "", false
	}
	address := parts[0]
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		address = net.JoinHostPort(address, "3478")
		host, port, err = net.SplitHostPort(address)
	}
	portNumber, portErr := strconv.Atoi(port)
	if err != nil || host == "" || portErr != nil || portNumber < 1 || portNumber > 65535 {
		return "", false
	}
	return address, true
}
