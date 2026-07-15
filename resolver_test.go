package tunnel

import (
	"bytes"
	"net"
	"testing"
	"time"
)

func TestResolverCloseReleasesPacketConnReader(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	resolver, err := NewResolver(conn)
	if err != nil {
		t.Fatal(err)
	}
	resolver.Close()

	sender, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	want := []byte("hole-punch")
	destination := &net.UDPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: conn.LocalAddr().(*net.UDPAddr).Port,
	}
	if _, err = sender.WriteToUDP(want, destination); err != nil {
		t.Fatal(err)
	}
	if err = conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	got := make([]byte, len(want))
	n, _, err := conn.ReadFrom(got)
	if err != nil {
		t.Fatalf("packet conn was not released by resolver: %v", err)
	}
	if !bytes.Equal(got[:n], want) {
		t.Fatalf("received %q, want %q", got[:n], want)
	}
}
