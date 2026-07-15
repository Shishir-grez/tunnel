package tunnel

import (
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultHandshakeTimeout = 30 * time.Second
	handshakeRetryInterval  = 100 * time.Millisecond
	handshakeReadyGrace     = time.Second
	triesNum                = 512
)

func handshake(tunnel *Tunnel, attemptTimeout time.Duration) chan error {
	done := make(chan error, 1)
	local := tunnel.localNAT
	remote := tunnel.remoteNAT

	switch {
	case local.NATType != NATTypeSymmetric && remote.NATType != NATTypeSymmetric:
		go handshakeNonSymmetric(tunnel, done, attemptTimeout)
	case local.NATType == NATTypeSymmetric:
		go handshakeLocalSymmetric(tunnel, done, attemptTimeout)
	default:
		go handshakeRemoteSymmetric(tunnel, done, attemptTimeout)
	}
	return done
}

func handshakeLocalSymmetric(tunnel *Tunnel, done chan error, attemptTimeout time.Duration) {
	log.Debugln("handshake local symmetric ...")
	if tunnel.conn != nil {
		_ = tunnel.conn.Close()
		tunnel.conn = nil
	}

	remote := tunnel.remoteNAT
	local := tunnel.localNAT
	remoteAddr, err := net.ResolveUDPAddr("udp4", remote.Addr)
	if err != nil {
		done <- err
		return
	}

	selectedConn := make(chan *net.UDPConn, 1)
	stop := make(chan struct{})
	var selected int32
	for i := 0; i < triesNum; i++ {
		time.Sleep(time.Millisecond)
		go func() {
			conn, err := net.ListenUDP("udp4", nil)
			if err != nil {
				log.Debugf("udp listen err, %s\n", err)
				return
			}
			keepConn := false
			defer func() {
				if !keepConn {
					_ = conn.Close()
				}
			}()

			if err = udpWrite(conn, remoteAddr, NewHandshakeMessage(local.Token)); err != nil {
				return
			}
			msg, _, err := udpRead(conn, attemptTimeout)
			if err != nil || msg.token != remote.Token {
				return
			}
			if !atomic.CompareAndSwapInt32(&selected, 0, 1) {
				return
			}
			keepConn = true
			close(stop)
			selectedConn <- conn
		}()
	}

	select {
	case <-time.After(attemptTimeout):
		done <- fmt.Errorf("timeout")
	case conn := <-selectedConn:
		_ = conn.SetReadDeadline(time.Time{})
		tunnel.conn = conn
		tunnel.remoteAddr = *remoteAddr
		close(done)
	}
}

func handshakeRemoteSymmetric(tunnel *Tunnel, done chan error, attemptTimeout time.Duration) {
	log.Debugln("handshake remote symmetric ...")
	remote := tunnel.remoteNAT
	local := tunnel.localNAT
	candidates := candidateAddrs(local, remote)

	conn, err := tunnel.udpConn()
	if err != nil {
		done <- err
		return
	}

	stop := make(chan struct{})
	for _, baseAddr := range candidates {
		baseAddr := baseAddr
		go func() {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			randomPorts := r.Perm(65535)
			for i := 0; i < triesNum; i++ {
				time.Sleep(time.Millisecond)
				select {
				case <-stop:
					return
				default:
					dst := &net.UDPAddr{IP: baseAddr.IP, Port: randomPorts[i]}
					_ = udpWrite(conn, dst, NewHandshakeMessage(local.Token))
				}
			}
		}()
	}

	for {
		msg, dst, err := udpRead(conn, attemptTimeout)
		if err != nil {
			close(stop)
			done <- err
			return
		}
		if msg.token != remote.Token {
			continue
		}
		close(stop)
		_ = udpWrite(conn, dst, NewHandshakeMessage(local.Token))
		_ = conn.SetReadDeadline(time.Time{})
		tunnel.remoteAddr = *dst
		close(done)
		return
	}
}

func handshakeNonSymmetric(tunnel *Tunnel, done chan error, attemptTimeout time.Duration) {
	remote := tunnel.remoteNAT
	local := tunnel.localNAT
	candidates := candidateAddrs(local, remote)

	remoteAddr, err := coordinatedHandshake(
		tunnel.conn,
		candidates,
		local.Token,
		remote.Token,
		attemptTimeout,
	)
	if err != nil {
		log.Debugf("udp handshake error: %s\n", err)
		done <- err
		return
	}
	tunnel.remoteAddr = *remoteAddr
	close(done)
}

type handshakeStage int

const (
	handshakeProbe handshakeStage = iota + 1
	handshakeAck
	handshakeConfirm
	handshakeReady
)

func coordinatedHandshake(
	conn net.PacketConn,
	candidates []*net.UDPAddr,
	localToken string,
	remoteToken string,
	attemptTimeout time.Duration,
) (*net.UDPAddr, error) {
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no remote candidates")
	}
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
	}()

	var state struct {
		sync.RWMutex
		stage      handshakeStage
		remoteAddr *net.UDPAddr
	}
	state.stage = handshakeProbe

	stop := make(chan struct{})
	defer close(stop)

	send := func() {
		state.RLock()
		stage := state.stage
		remoteAddr := cloneUDPAddr(state.remoteAddr)
		state.RUnlock()

		msg := newHandshakeMessage(localToken, messageTypeForHandshakeStage(stage))
		if remoteAddr != nil {
			if err := udpWrite(conn, remoteAddr, msg); err != nil {
				log.Debugf("handshake write to %s error: %v\n", remoteAddr, err)
			}
			return
		}
		for _, candidate := range candidates {
			if err := udpWrite(conn, candidate, msg); err != nil {
				log.Debugf("handshake write to %s error: %v\n", candidate, err)
			}
		}
	}

	go func() {
		send()
		ticker := time.NewTicker(handshakeRetryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				send()
			}
		}
	}()

	deadline := time.Now().Add(attemptTimeout)
	var readyAt time.Time
	for {
		now := time.Now()
		if !readyAt.IsZero() && now.Sub(readyAt) >= handshakeReadyGrace {
			state.RLock()
			remoteAddr := cloneUDPAddr(state.remoteAddr)
			state.RUnlock()
			log.Debugf("coordinated handshake ready with %s\n", remoteAddr)
			return remoteAddr, nil
		}
		if !now.Before(deadline) {
			return nil, fmt.Errorf("i/o timeout")
		}

		readDeadline := now.Add(handshakeRetryInterval)
		if deadline.Before(readDeadline) {
			readDeadline = deadline
		}
		if !readyAt.IsZero() {
			readyDeadline := readyAt.Add(handshakeReadyGrace)
			if readyDeadline.Before(readDeadline) {
				readDeadline = readyDeadline
			}
		}

		msg, src, err := udpReadUntil(conn, readDeadline)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return nil, err
		}
		stage, ok := handshakeStageForMessageType(msg.mType)
		if !ok || msg.token != remoteToken {
			continue
		}

		state.Lock()
		if state.remoteAddr == nil {
			state.remoteAddr = cloneUDPAddr(src)
		}
		nextStage := stage + 1
		if nextStage > handshakeReady {
			nextStage = handshakeReady
		}
		if nextStage > state.stage {
			state.stage = nextStage
			log.Debugf("handshake stage %s with %s\n", handshakeStageName(nextStage), src)
		}
		if stage == handshakeReady && readyAt.IsZero() {
			readyAt = time.Now()
		}
		state.Unlock()
	}
}

func messageTypeForHandshakeStage(stage handshakeStage) MessageType {
	switch stage {
	case handshakeAck:
		return MessageTypeHandshakeAck
	case handshakeConfirm:
		return MessageTypeHandshakeConfirm
	case handshakeReady:
		return MessageTypeHandshakeReady
	default:
		return MessageTypeHandshake
	}
}

func handshakeStageForMessageType(messageType MessageType) (handshakeStage, bool) {
	switch messageType {
	case MessageTypeHandshake:
		return handshakeProbe, true
	case MessageTypeHandshakeAck:
		return handshakeAck, true
	case MessageTypeHandshakeConfirm:
		return handshakeConfirm, true
	case MessageTypeHandshakeReady:
		return handshakeReady, true
	default:
		return 0, false
	}
}

func handshakeStageName(stage handshakeStage) string {
	switch stage {
	case handshakeAck:
		return "ack"
	case handshakeConfirm:
		return "confirm"
	case handshakeReady:
		return "ready"
	default:
		return "probe"
	}
}

func cloneUDPAddr(addr *net.UDPAddr) *net.UDPAddr {
	if addr == nil {
		return nil
	}
	clone := *addr
	clone.IP = append(net.IP(nil), addr.IP...)
	return &clone
}

func (tunnel *Tunnel) udpConn() (*net.UDPConn, error) {
	conn, ok := tunnel.conn.(*net.UDPConn)
	if !ok || conn == nil {
		return nil, fmt.Errorf("tunnel connection is not a UDP connection")
	}
	return conn, nil
}

// candidateAddrs returns the public address first, followed by plausible
// same-LAN addresses.
func candidateAddrs(local, remote *NATDetail) []*net.UDPAddr {
	seen := map[string]bool{}
	var addrs []*net.UDPAddr

	add := func(rawAddr string) {
		if rawAddr == "" || seen[rawAddr] {
			return
		}
		seen[rawAddr] = true
		addr, err := net.ResolveUDPAddr("udp4", rawAddr)
		if err != nil {
			return
		}
		addrs = append(addrs, addr)
	}

	add(remote.Addr)
	for _, remoteLocalAddr := range remote.LocalAddrs {
		if isSameLANCandidate(local.LocalAddrs, remoteLocalAddr) {
			add(remoteLocalAddr)
		}
	}
	log.Debugf("candidate addresses: %v\n", addrs)
	return addrs
}

func isSameLANCandidate(localAddrs []string, remoteAddr string) bool {
	remoteUDPAddr, err := net.ResolveUDPAddr("udp4", remoteAddr)
	if err != nil || !isRFC1918(remoteUDPAddr.IP) {
		return false
	}
	for _, localAddr := range localAddrs {
		localUDPAddr, err := net.ResolveUDPAddr("udp4", localAddr)
		if err != nil || !isRFC1918(localUDPAddr.IP) {
			continue
		}
		if sameIPv4Prefix(localUDPAddr.IP, remoteUDPAddr.IP, 24) {
			return true
		}
	}
	return false
}

func isRFC1918(ip net.IP) bool {
	ip = ip.To4()
	if ip == nil {
		return false
	}
	return ip[0] == 10 ||
		(ip[0] == 172 && ip[1] >= 16 && ip[1] <= 31) ||
		(ip[0] == 192 && ip[1] == 168)
}

func sameIPv4Prefix(a, b net.IP, bits int) bool {
	a = a.To4()
	b = b.To4()
	if a == nil || b == nil {
		return false
	}
	mask := net.CIDRMask(bits, 32)
	return a.Mask(mask).Equal(b.Mask(mask))
}

func udpWrite(conn net.PacketConn, addr *net.UDPAddr, msg *Message) error {
	data, err := msg.Marshal()
	if err != nil {
		return err
	}
	_, err = conn.WriteTo(data, addr)
	return err
}

func udpRead(conn net.PacketConn, readTimeout time.Duration) (*Message, *net.UDPAddr, error) {
	return udpReadUntil(conn, time.Now().Add(readTimeout))
}

func udpReadUntil(conn net.PacketConn, deadline time.Time) (*Message, *net.UDPAddr, error) {
	if err := conn.SetReadDeadline(deadline); err != nil {
		return nil, nil, err
	}
	data := make([]byte, 2048)
	for {
		n, source, err := conn.ReadFrom(data)
		if err != nil {
			return nil, nil, err
		}
		msg, err := UnmarshalMessage(data[:n])
		if err != nil {
			log.Debugf("ignoring non-handshake packet from %s\n", source)
			continue
		}
		udpAddr, ok := source.(*net.UDPAddr)
		if !ok {
			udpAddr, err = net.ResolveUDPAddr("udp4", source.String())
			if err != nil {
				return nil, nil, err
			}
		}
		return msg, udpAddr, nil
	}
}
