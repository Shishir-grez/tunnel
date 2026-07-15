package tunnel

import (
	"fmt"
	"net"
	"sync"

	"github.com/pion/logging"
	pionturn "github.com/pion/turn/v2"
)

// TURNConfig contains short-lived credentials for a UDP TURN allocation.
type TURNConfig struct {
	Server   string
	Username string
	Password string
}

// Option configures a Tunnel without breaking existing NewTunnel callers.
type Option func(*Tunnel) error

// WithTURN enables TURN as a fallback when direct UDP traversal fails.
func WithTURN(config TURNConfig) Option {
	return func(t *Tunnel) error {
		if config.Server == "" || config.Username == "" || config.Password == "" {
			return fmt.Errorf("TURN server, username, and password are required")
		}
		if _, err := net.ResolveUDPAddr("udp4", config.Server); err != nil {
			return fmt.Errorf("resolve TURN server %q: %w", config.Server, err)
		}
		t.turnConfig = &config
		return nil
	}
}

type turnRelay struct {
	baseConn  net.PacketConn
	client    *pionturn.Client
	conn      net.PacketConn
	closeOnce sync.Once
}

func newTURNRelay(config TURNConfig) (*turnRelay, error) {
	baseConn, err := net.ListenPacket("udp4", "0.0.0.0:0")
	if err != nil {
		return nil, fmt.Errorf("open TURN client socket: %w", err)
	}

	client, err := pionturn.NewClient(&pionturn.ClientConfig{
		STUNServerAddr: config.Server,
		TURNServerAddr: config.Server,
		Conn:           baseConn,
		Username:       config.Username,
		Password:       config.Password,
		LoggerFactory:  logging.NewDefaultLoggerFactory(),
	})
	if err != nil {
		_ = baseConn.Close()
		return nil, fmt.Errorf("create TURN client: %w", err)
	}
	if err = client.Listen(); err != nil {
		client.Close()
		_ = baseConn.Close()
		return nil, fmt.Errorf("start TURN client: %w", err)
	}

	relayConn, err := client.Allocate()
	if err != nil {
		client.Close()
		_ = baseConn.Close()
		return nil, fmt.Errorf("allocate TURN relay: %w", err)
	}

	return &turnRelay{
		baseConn: baseConn,
		client:   client,
		conn:     relayConn,
	}, nil
}

func (r *turnRelay) Close() {
	if r == nil {
		return
	}
	r.closeOnce.Do(func() {
		if r.conn != nil {
			_ = r.conn.Close()
		}
		if r.client != nil {
			r.client.Close()
		}
		if r.baseConn != nil {
			_ = r.baseConn.Close()
		}
	})
}
