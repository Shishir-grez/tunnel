package tunnel

import (
	"fmt"
	"math/rand"
	"net"
	"sync/atomic"
	"time"
)

// TODO: better timeout
const timeout = time.Second * 30

// probability of success is 98.34%
const triesNum = 512

func handshake(tunnel *Tunnel) chan error {
	cDone := make(chan error, 1)
	local := tunnel.localNAT
	remote := tunnel.remoteNAT

	if local.NATType != NATTypeSymmetric && remote.NATType != NATTypeSymmetric {
		// both are not symmetric NAT
		// handshake
		go handshakeNonSymmetric(tunnel, cDone)
	} else if local.NATType == NATTypeSymmetric {
		// local is symmetric NAT
		// select local port
		go handshakeLocalSymmetric(tunnel, cDone)
	} else if remote.NATType == NATTypeSymmetric {
		// remote is symmetric NAT
		// select remote port
		go handshakeRemoteSymmetric(tunnel, cDone)
	}
	return cDone
}

func handshakeLocalSymmetric(tunnel *Tunnel, done chan error) {
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
	c := make(chan *net.UDPConn, 1)
	stopChan := make(chan int, 1)
	var selected int32 = 0
	// birthday attack
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
			// send handshake
			err = udpWrite(conn, remoteAddr, NewHandshakeMessage(local.Token))
			if err != nil {
				return
			}
			// rev response
			msg, _, err := udpRead(conn)
			if err != nil {
				return
			}
			if msg.token != remote.Token {
				log.Debugf("token fail, token: %s\n", msg.token)
				return
			}
			// only select the first one
			if !atomic.CompareAndSwapInt32(&selected, 0, 1) {
				return
			}
			keepConn = true
			close(stopChan)
			c <- conn
		}()
	}
	select {
	case <-time.After(timeout):
		done <- fmt.Errorf("timeout")
	case conn := <-c:
		tunnel.conn = conn
		tunnel.remoteAddr = *remoteAddr
		close(done)
	}
}

func handshakeRemoteSymmetric(tunnel *Tunnel, done chan error) {
	log.Debugln("handshake remote symmetric ...")
	remote := tunnel.remoteNAT
	local := tunnel.localNAT
	candidates := candidateAddrs(local, remote)

	conn, err := tunnel.udpConn()
	if err != nil {
		done <- err
		return
	}

	stopChan := make(chan struct{})
	// spray handshakes at all candidates concurrently on the same conn
	for _, baseAddr := range candidates {
		baseAddr := baseAddr
		go func() {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			randPorts := r.Perm(65535)
			for i := 0; i < triesNum; i++ {
				time.Sleep(time.Millisecond)
				select {
				case <-stopChan:
					return
				default:
					dst := &net.UDPAddr{IP: baseAddr.IP, Port: randPorts[i]}
					_ = udpWrite(conn, dst, NewHandshakeMessage(local.Token))
				}
			}
		}()
	}

	for {
		msg, dst, err := udpRead(conn)
		if err != nil {
			close(stopChan)
			conn.Close()
			done <- err
			return
		}
		if msg.token != remote.Token {
			continue
		}
		close(stopChan)
		_ = udpWrite(conn, dst, NewHandshakeMessage(local.Token))
		tunnel.conn = conn
		tunnel.remoteAddr = *dst
		close(done)
		return
	}
}

func handshakeNonSymmetric(tunnel *Tunnel, done chan error) {
	remote := tunnel.remoteNAT
	local := tunnel.localNAT
	candidates := candidateAddrs(local, remote)

	conn, err := tunnel.udpConn()
	if err != nil {
		done <- err
		return
	}

	stopChan := make(chan struct{})
	// keep sending handshake to all candidates until we get a response
	for _, addr := range candidates {
		addr := addr
		go func() {
			for {
				select {
				case <-stopChan:
					return
				default:
					if err := udpWrite(conn, addr, NewHandshakeMessage(local.Token)); err != nil {
						log.Debugf("write err for %s: %s\n", addr, err)
					}
					time.Sleep(200 * time.Millisecond)
				}
			}
		}()
	}

	// read loop: reply to first handshake received, then wait for the reply-ack
	var remoteAddr *net.UDPAddr
	for {
		msg, src, err := udpRead(conn)
		if err != nil {
			close(stopChan)
			conn.Close()
			log.Debugf("udp read err, %s\n", err)
			done <- err
			return
		}
		if msg.token != remote.Token {
			continue
		}
		if remoteAddr == nil {
			// first valid handshake received — reply so the other side can finish
			remoteAddr = src
			_ = udpWrite(conn, src, NewHandshakeMessage(local.Token))
		}
		// once we've sent our reply (or received theirs), we're done
		close(stopChan)
		tunnel.conn = conn
		tunnel.remoteAddr = *remoteAddr
		close(done)
		return
	}
}

func (tunnel *Tunnel) udpConn() (*net.UDPConn, error) {
	conn, ok := tunnel.conn.(*net.UDPConn)
	if !ok || conn == nil {
		return nil, fmt.Errorf("tunnel connection is not a UDP connection")
	}
	return conn, nil
}

// candidateAddrs returns addresses to try for the remote peer:
// the public (STUN-mapped) address first, followed by plausible same-LAN addresses.
func candidateAddrs(local, remote *NATDetail) []*net.UDPAddr {
	seen := map[string]bool{}
	var addrs []*net.UDPAddr

	add := func(s string) {
		if s == "" || seen[s] {
			return
		}
		seen[s] = true
		a, err := net.ResolveUDPAddr("udp4", s)
		if err != nil {
			return
		}
		addrs = append(addrs, a)
	}

	add(remote.Addr)
	for _, s := range remote.LocalAddrs {
		if isSameLANCandidate(local.LocalAddrs, s) {
			add(s)
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

func udpWrite(conn *net.UDPConn, addr *net.UDPAddr, msg *Message) error {
	bytes, _ := msg.Marshal()
	_, err := conn.WriteTo(bytes, addr)
	if err != nil {
		return err
	}
	return nil
}

func udpRead(conn *net.UDPConn) (*Message, *net.UDPAddr, error) {
	err := conn.SetReadDeadline(time.Now().Add(timeout))
	if err != nil {
		return nil, nil, err
	}
	bytes := make([]byte, 128)
	n, dst, err := conn.ReadFrom(bytes)
	if err != nil {
		return nil, nil, err
	}
	msg, err := UnmarshalMessage(bytes[:n])
	if err != nil {
		return nil, nil, err
	}
	return msg, dst.(*net.UDPAddr), nil
}
