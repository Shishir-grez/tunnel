package tunnel

import (
	"net"
	"sync"
	"testing"
	"time"
)

func TestCoordinatedHandshakeCompletesOnBothPeers(t *testing.T) {
	alice, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer alice.Close()

	bob, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer bob.Close()

	aliceTracked := &deadlineTrackingPacketConn{PacketConn: alice}
	bobTracked := &deadlineTrackingPacketConn{PacketConn: bob}

	type result struct {
		addr *net.UDPAddr
		err  error
	}
	aliceResult := make(chan result, 1)
	bobResult := make(chan result, 1)

	go func() {
		addr, err := coordinatedHandshake(
			aliceTracked,
			[]*net.UDPAddr{bob.LocalAddr().(*net.UDPAddr)},
			"aaaaaaaa",
			"bbbbbbbb",
			5*time.Second,
		)
		aliceResult <- result{addr: addr, err: err}
	}()
	go func() {
		addr, err := coordinatedHandshake(
			bobTracked,
			[]*net.UDPAddr{alice.LocalAddr().(*net.UDPAddr)},
			"bbbbbbbb",
			"aaaaaaaa",
			5*time.Second,
		)
		bobResult <- result{addr: addr, err: err}
	}()

	for peer, resultCh := range map[string]<-chan result{
		"Alice": aliceResult,
		"Bob":   bobResult,
	} {
		result := <-resultCh
		if result.err != nil {
			t.Fatalf("%s handshake failed: %v", peer, result.err)
		}
		if result.addr == nil {
			t.Fatalf("%s handshake returned no remote address", peer)
		}
	}
	if deadline := aliceTracked.readDeadline(); !deadline.IsZero() {
		t.Fatalf("Alice read deadline was not cleared: %s", deadline)
	}
	if deadline := bobTracked.readDeadline(); !deadline.IsZero() {
		t.Fatalf("Bob read deadline was not cleared: %s", deadline)
	}
}

func TestUDPReadUntilIgnoresUnrelatedDatagram(t *testing.T) {
	receiver, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()

	sender, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	want := NewHandshakeMessage("deadbeef")
	wantBytes, err := want.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = sender.WriteToUDP([]byte("early-quic-packet"), receiver.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	if _, err = sender.WriteToUDP(wantBytes, receiver.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}

	got, _, err := udpReadUntil(receiver, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got.token != "deadbeef" || got.mType != MessageTypeHandshake {
		t.Fatalf("unexpected message: %s", got)
	}
}

type deadlineTrackingPacketConn struct {
	net.PacketConn
	mu       sync.Mutex
	deadline time.Time
}

func (c *deadlineTrackingPacketConn) SetReadDeadline(deadline time.Time) error {
	c.mu.Lock()
	c.deadline = deadline
	c.mu.Unlock()
	return c.PacketConn.SetReadDeadline(deadline)
}

func (c *deadlineTrackingPacketConn) readDeadline() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deadline
}
