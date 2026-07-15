package tunnel

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/pion/logging"
	"github.com/pion/stun"
	"github.com/pion/turn/v2"
	"net"
	"sync"
	"time"
)

type request struct {
	stun       string
	changeIp   bool
	changePort bool
}

var stunServers = []request{
	{
		stun:       "stun.l.google.com:19302",
		changeIp:   false,
		changePort: false,
	},
	{
		stun:       "stun1.l.google.com:19302",
		changeIp:   false,
		changePort: false,
	},
	{
		stun:       "stun.miwifi.com:3478",
		changeIp:   true,
		changePort: true,
	},
	{
		stun:       "stun.miwifi.com:3478",
		changeIp:   false,
		changePort: true,
	},
}

type NATType int

const (
	NATTypeFullCone NATType = iota + 1
	NATTypeRestrictedCone
	NATTypePortRestrictedCone
	NATTypeSymmetric
)

func (n NATType) String() string {
	switch n {
	case NATTypeFullCone:
		return "full-cone"
	case NATTypeRestrictedCone:
		return "restricted-cone"
	case NATTypePortRestrictedCone:
		return "port-restricted-cone"
	case NATTypeSymmetric:
		return "symmetric"
	default:
		return fmt.Sprintf("unknown(%d)", n)
	}
}

type NATDetail struct {
	Addr       string   `json:"addr"`
	LocalAddrs []string `json:"local_addrs"`
	NATType    NATType  `json:"nat_type"`
	Token      string   `json:"token"`
	RelayAddr  string   `json:"relay_addr,omitempty"`
}

func (d *NATDetail) String() string {
	if d == nil {
		return "<nil>"
	}
	return fmt.Sprintf(
		"addr=%s local_addrs=%v nat_type=%s token=%s relay_addr=%s",
		d.Addr,
		d.LocalAddrs,
		d.NATType,
		d.Token,
		d.RelayAddr,
	)
}

type Resolver struct {
	conn       net.PacketConn
	client     *turn.Client
	readerDone chan struct{}
}

func (r Resolver) Resolve() (*NATDetail, error) {

	token, err := GenerateToken()
	if err != nil {
		return nil, err
	}
	log.Debugf("generate local token: %s\n", token)
	mappedAddrs := make([]string, len(stunServers))

	var wg sync.WaitGroup

	for idx, req := range stunServers {
		wg.Add(1)
		go func(idx int, req request) {
			defer wg.Done()
			mappedAddr, err := r.test(req.stun, req.changeIp, req.changePort)
			if err != nil {
				log.Debugf("stun[%d] %s error: %v\n", idx, req.stun, err)
				return
			}
			mappedAddrs[idx] = mappedAddr
		}(idx, req)
	}
	log.Debugln("wait for stun server response")
	wg.Wait()
	var nType NATType
	if mappedAddrs[0] == "" || mappedAddrs[1] == "" {
		return nil, fmt.Errorf("failed to resolve stun server")
	}

	if mappedAddrs[0] != mappedAddrs[1] {
		nType = NATTypeSymmetric
	} else if mappedAddrs[2] != "" && mappedAddrs[0] == mappedAddrs[2] {
		nType = NATTypeFullCone
	} else if mappedAddrs[3] != "" && mappedAddrs[0] == mappedAddrs[3] {
		nType = NATTypeRestrictedCone
	} else {
		// CHANGE-REQUEST unsupported by most public STUN servers; default to most restrictive type
		nType = NATTypePortRestrictedCone
	}

	localAddrs := collectLocalAddrs(r.conn)

	return &NATDetail{
		Addr:       mappedAddrs[0],
		LocalAddrs: localAddrs,
		NATType:    nType,
		Token:      token,
	}, nil
}

func (r Resolver) test(stunServer string, changeIp bool, changePort bool) (string, error) {
	toAddr, err := net.ResolveUDPAddr("udp4", stunServer)
	if err != nil {
		return "", fmt.Errorf("failed to resolve %s: %s", stunServer, err)
	}
	msg, err := buildMsg(changeIp, changePort)
	if err != nil {
		return "", fmt.Errorf("failed to build STUN message: %s", err)
	}
	res, err := r.client.PerformTransaction(msg, toAddr, false)
	if err != nil {
		return "", fmt.Errorf("failed to perform transaction: %s", err)
	}

	var mappedAddr stun.XORMappedAddress
	if err = mappedAddr.GetFrom(res.Msg); err != nil {
		return "", fmt.Errorf("failed to get MAPPED-ADDRESS: %s", err)
	}
	return mappedAddr.String(), nil
}

func (r Resolver) Close() {
	r.client.Close()
	// turn.Client.Close only closes its transactions; it does not stop the
	// Listen goroutine. Interrupt our resolver-owned reader before this socket
	// is handed to the hole-punch handshake.
	if err := r.conn.SetReadDeadline(time.Now()); err != nil {
		log.Debugf("stop STUN reader error: %v\n", err)
		return
	}
	<-r.readerDone
	log.Debugln("STUN reader stopped")
	if err := r.conn.SetReadDeadline(time.Time{}); err != nil {
		log.Debugf("clear STUN read deadline error: %v\n", err)
	}
}

func NewResolver(conn net.PacketConn) (r *Resolver, err error) {
	cfg := &turn.ClientConfig{
		Conn:          conn,
		LoggerFactory: logging.NewDefaultLoggerFactory(),
		RTO:           time.Second,
	}
	client, err := turn.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	r = &Resolver{
		conn:       conn,
		client:     client,
		readerDone: make(chan struct{}),
	}
	go r.readSTUNResponses()
	return r, nil
}

func (r *Resolver) readSTUNResponses() {
	defer close(r.readerDone)

	buf := make([]byte, 64*1024)
	for {
		n, from, err := r.conn.ReadFrom(buf)
		if err != nil {
			return
		}
		if _, err = r.client.HandleInbound(buf[:n], from); err != nil {
			log.Debugf("handle STUN response from %s error: %v\n", from, err)
		}
	}
}

func buildMsg(changeIp bool, changePort bool) (*stun.Message, error) {
	attrs := []stun.Setter{
		stun.TransactionID,
		stun.BindingRequest,
	}
	msg, err := stun.Build(attrs...)
	if err != nil {
		return nil, fmt.Errorf("failed to build STUN message: %s", err)
	}
	if !changePort && !changeIp {
		return msg, nil
	}

	var attr uint32

	if changeIp {
		attr |= 0x4 // changeIp
	}

	if changePort {
		attr |= 0x2 // changePort
	}

	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, attr)
	msg.Add(stun.AttrChangeRequest, bytes)
	return msg, nil
}

func collectLocalAddrs(conn net.PacketConn) []string {
	port := conn.LocalAddr().(*net.UDPAddr).Port
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var addrs []string
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		ifAddrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range ifAddrs {
			var ip net.IP
			switch v := a.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.To4() == nil {
				continue
			}
			addrs = append(addrs, fmt.Sprintf("%s:%d", ip.String(), port))
		}
	}
	return addrs
}

func GenerateToken() (string, error) {
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
