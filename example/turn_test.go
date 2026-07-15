package main

import "testing"

func TestSelectUDPTURNConfig(t *testing.T) {
	config, err := selectUDPTURNConfig([]iceServer{
		{URLs: []string{"stun:stun.cloudflare.com:3478"}},
		{
			URLs: []string{
				"turn:turn.cloudflare.com:3478?transport=tcp",
				"turn:turn.cloudflare.com:3478?transport=udp",
			},
			Username:   "temporary-user",
			Credential: "temporary-password",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if config.Server != "turn.cloudflare.com:3478" {
		t.Fatalf("server = %q", config.Server)
	}
	if config.Username != "temporary-user" || config.Password != "temporary-password" {
		t.Fatalf("unexpected credentials: %#v", config)
	}
}
