package tunnel

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

type Tunnel struct {
	ctx        context.Context
	conn       net.PacketConn
	directConn net.PacketConn
	localAddr  net.UDPAddr
	remoteAddr net.UDPAddr
	localNAT   *NATDetail
	remoteNAT  *NATDetail
	signal     Signal
	turnConfig *TURNConfig
	relay      *turnRelay
	path       string
	cancelFunc context.CancelFunc
	closeOnce  sync.Once
}

func NewTunnel(ctx context.Context, signal Signal, options ...Option) (*Tunnel, error) {
	ctx, cancelFunc := context.WithCancel(ctx)
	tunnel := &Tunnel{
		ctx:        ctx,
		signal:     signal,
		cancelFunc: cancelFunc,
	}
	for _, option := range options {
		if err := option(tunnel); err != nil {
			cancelFunc()
			return nil, err
		}
	}
	return tunnel, nil
}

func (t *Tunnel) Connect() error {
	if err := t.initTunnel(); err != nil {
		return err
	}

	hasRelay := t.relay != nil && t.remoteNAT.RelayAddr != ""
	directTimeout := defaultHandshakeTimeout
	if hasRelay {
		directTimeout = 6 * time.Second
	}

	var directErr error
	if t.localNAT.NATType == NATTypeSymmetric && t.remoteNAT.NATType == NATTypeSymmetric {
		directErr = fmt.Errorf("both peers reported symmetric NAT")
	} else {
		directErr = waitForHandshake(t, directTimeout)
		if directErr == nil {
			t.path = "direct"
			if t.relay != nil {
				t.relay.Close()
				t.relay = nil
			}
			log.Debugln("direct tunnel handshake success")
			log.Debugf("local addr: %s, remote addr: %s\n", t.localAddr.String(), t.remoteAddr.String())
			return nil
		}
	}

	if !hasRelay {
		return fmt.Errorf(
			"direct hole punch failed and TURN fallback is unavailable: local NAT %s; remote NAT %s: %w",
			t.localNAT,
			t.remoteNAT,
			directErr,
		)
	}

	log.Debugf("direct handshake failed (%v); switching to TURN relay\n", directErr)
	if t.conn != nil {
		_ = t.conn.Close()
	}
	remoteRelayAddr, err := net.ResolveUDPAddr("udp4", t.remoteNAT.RelayAddr)
	if err != nil {
		return fmt.Errorf("resolve remote TURN address: %w", err)
	}
	t.conn = t.relay.conn
	if localRelayAddr, ok := t.relay.conn.LocalAddr().(*net.UDPAddr); ok {
		t.localAddr = *localRelayAddr
	}
	selectedAddr, relayErr := coordinatedHandshake(
		t.conn,
		[]*net.UDPAddr{remoteRelayAddr},
		t.localNAT.Token,
		t.remoteNAT.Token,
		15*time.Second,
	)
	if relayErr != nil {
		return fmt.Errorf("direct path failed (%v); TURN relay handshake failed: %w", directErr, relayErr)
	}
	t.remoteAddr = *selectedAddr
	t.path = "turn-relay"
	log.Debugln("TURN relay handshake success")
	log.Debugf("local relay addr: %s, remote relay addr: %s\n", t.localAddr.String(), t.remoteAddr.String())
	return nil
}

func waitForHandshake(t *Tunnel, attemptTimeout time.Duration) error {
	done := handshake(t, attemptTimeout)
	err, open := <-done
	if !open {
		return nil
	}
	return err
}

// TransportPath reports the selected packet path after Connect succeeds.
func (t *Tunnel) TransportPath() string {
	return t.path
}

// TODO: temporary code ↓
func (t *Tunnel) Test(mode string) {
	if mode == "server" {
		t.runHandler()
	} else {
		go t.keepAlive()
		for {
			select {
			case <-t.ctx.Done():
				return
			default:
				fmt.Printf("Message: ")
				r := bufio.NewReader(os.Stdin)
				var in []byte
				for {
					var err error
					in, err = r.ReadBytes('\n')
					if err != io.EOF {
						if err != nil {
							break
						}
					}
					if len(in) > 0 {
						break
					}
				}
				msg, err := NewDataMessage(t.localNAT.Token, in).Marshal()
				if err != nil {
					log.Debugf("marshal message error: %s\n", err)
					continue
				}
				_, err = t.conn.WriteTo(msg, &t.remoteAddr)
				if err != nil {
					log.Debugf("send message error: %s\n", err)
					continue
				}
			}
		}
	}
}

func (t *Tunnel) runHandler() {
	bytes := make([]byte, 1024)
	timeout := time.NewTimer(time.Second * 10)
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-timeout.C:
			t.Close()
			return
		default:
			err := t.conn.SetReadDeadline(time.Now().Add(time.Second * 5))
			if err != nil {
				log.Debugf("set read deadline error: %s\n", err)
				continue
			}
			n, addr, err := t.conn.ReadFrom(bytes)
			if err != nil {
				log.Debugf("read from conn error: %s\n", err)
				continue
			}
			if addr.String() != t.remoteAddr.String() {
				log.Debugf("received message from unexpected addr: %s\n", addr.String())
				continue
			}
			msg, err := UnmarshalMessage(bytes[:n])
			if err != nil {
				log.Debugf("unmarshal message error: %s\n", err)
				continue
			}
			if msg.token != t.remoteNAT.Token {
				log.Debugf("received message with unexpected token: %s\n", msg.token)
				continue
			}
			switch msg.mType {
			case MessageTypeHandshake:
				log.Debugf("received handshake message from %s\n", addr.String())
			case MessageTypePing:
				if !timeout.Stop() {
					<-timeout.C
				}
				timeout.Reset(time.Second * 10)
			case MessageTypeData:
				log.Debugf("received data message from %s, payload: %s\n", addr.String(), string(msg.payload))
			}
		}
	}
}

func (t *Tunnel) keepAlive() {
	// send and wait receive ping message
	pingMsg, err := NewPingMessage(t.localNAT.Token).Marshal()
	if err != nil {
		return
	}
	tick := time.NewTicker(time.Second)
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-tick.C:
			_, _ = t.conn.WriteTo(pingMsg, &t.remoteAddr)
		}
	}
}

// TODO: temporary code ↑

func (t *Tunnel) initTunnel() error {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		return err
	}
	keepConn := false
	defer func() {
		if !keepConn {
			_ = conn.Close()
			if t.relay != nil {
				t.relay.Close()
				t.relay = nil
			}
		}
	}()
	resolver, err := NewResolver(conn)
	if err != nil {
		return err
	}
	localNAT, resolveErr := resolver.Resolve()
	resolver.Close()
	if resolveErr != nil {
		return resolveErr
	}
	if t.turnConfig != nil {
		relay, relayErr := newTURNRelay(*t.turnConfig)
		if relayErr != nil {
			return relayErr
		}
		t.relay = relay
		localNAT.RelayAddr = relay.conn.LocalAddr().String()
		log.Debugf("local TURN relay: %s\n", localNAT.RelayAddr)
	}
	t.localNAT = localNAT
	log.Debugf("local NAT: %s\n", localNAT)
	err = t.signal.SendSignal(localNAT)
	if err != nil {
		return err
	}
	remoteNAT, err := t.signal.ReadSignal()
	if err != nil {
		return err
	}
	log.Debugf("remote NAT: %s\n", remoteNAT)
	if remoteNAT.NATType == NATTypeSymmetric && localNAT.NATType == NATTypeSymmetric && t.relay == nil {
		return fmt.Errorf("symmetric NAT not supported")
	}
	t.localNAT = localNAT
	t.remoteNAT = remoteNAT
	t.localAddr = net.UDPAddr{
		IP:   net.IPv4zero,
		Port: conn.LocalAddr().(*net.UDPAddr).Port,
	}
	t.conn = conn
	t.directConn = conn
	keepConn = true
	return nil
}

func (t *Tunnel) Close() {
	t.closeOnce.Do(func() {
		t.cancelFunc()
		if t.conn != nil {
			_ = t.conn.Close()
		}
		if t.directConn != nil {
			_ = t.directConn.Close()
		}
		if t.relay != nil {
			t.relay.Close()
		}
	})
}
