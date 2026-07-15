# Tunnel

A Go library for peer-to-peer applications with NAT traversal, QUIC, and a
symmetric HTTP/2 API.

## How it works

```text
Signaling -> coordinated UDP hole punch -> optional TURN fallback -> QUIC -> HTTP/2
```

1. Both peers discover and exchange direct network candidates.
2. A four-stage, retransmitted handshake confirms that both peers can use the
   same direct path.
3. If direct traversal fails and TURN is configured, both peers switch to their
   exchanged relay addresses.
4. QUIC provides the encrypted packet transport.
5. Each peer serves and sends HTTP/2 requests over the QUIC connection.

Direct hole punching is inherently unable to traverse every NAT or firewall.
TURN is the reliability fallback for carrier-grade NAT, symmetric NAT, and
networks that filter unsolicited UDP.

## Deploy the signal Worker

Create a KV namespace, copy `worker/wrangler.toml.example` to the ignored local
file `worker/wrangler.toml`, and insert the namespace ID:

```powershell
cd worker
npx wrangler kv namespace create SIGNAL_KV
npx wrangler deploy
```

The Worker provides signaling without TURN by default. This is enough for
networks where direct UDP traversal works.

## Configure reliable TURN fallback

Cloudflare Realtime TURN uses a server-side TURN key to generate short-lived
client credentials. The long-lived key must never be placed in the Go client or
committed to Git.

1. Create a TURN key in the Cloudflare dashboard or with the Cloudflare API.
2. Put the returned key ID in the local `worker/wrangler.toml`:

```toml
[vars]
TURN_KEY_ID = "your-turn-key-id"
```

3. Store the key secret and a separate client access token as Worker secrets:

```powershell
npx wrangler secret put TURN_KEY_API_TOKEN
npx wrangler secret put TURN_ACCESS_TOKEN
npx wrangler deploy
```

`TURN_ACCESS_TOKEN` is a strong random value you choose. Configure the same
value on both client machines without putting it on the command line:

```powershell
$env:TUNNEL_TURN_TOKEN = "your-client-access-token"
```

The clients then request one-hour credentials from the Worker. They still try
the direct path first and only consume relay bandwidth when direct traversal
fails. See the [Cloudflare TURN documentation](https://developers.cloudflare.com/realtime/turn/)
for key creation, service availability, and pricing.

## Run the chat example

Build a stable executable on both machines so Windows Firewall rules apply to a
consistent path:

```powershell
go build -o tunnel-chat.exe ./example
$env:TUNNEL_LOG = "debug"
```

Start the first peer:

```powershell
.\tunnel-chat.exe -name=Alice -signal=cloudflare -worker=https://your-worker.workers.dev
```

Start the second peer with the printed room token:

```powershell
.\tunnel-chat.exe -name=Bob -signal=cloudflare -worker=https://your-worker.workers.dev -room=ROOM_TOKEN
```

The successful path is explicit in the output:

```text
Connected via direct. Type messages and press Enter.
```

or:

```text
Connected via turn-relay. Type messages and press Enter.
```

Set `-ping-interval=5s` to change the RTT probe interval or
`-ping-interval=0` to disable it.

## API

Direct-only use remains source compatible:

```go
t, err := tunnel.NewTunnel(ctx, signal)
```

Enable a TURN fallback with short-lived credentials:

```go
t, err := tunnel.NewTunnel(ctx, signal, tunnel.WithTURN(tunnel.TURNConfig{
    Server:   "turn.cloudflare.com:3478",
    Username: username,
    Password: credential,
}))
```

Connect and upgrade:

```go
if err := t.Connect(); err != nil {
    return err
}
peer, err := t.ConnectHTTP2()
if err != nil {
    return err
}

peer.Handle("/hello", func(w http.ResponseWriter, r *http.Request) {
    fmt.Fprint(w, "hello")
})

resp, err := peer.Client.Get("https://tunnel/hello")
```

The signaling contract remains:

```go
type Signal interface {
    SendSignal(detail *NATDetail) error
    ReadSignal() (*NATDetail, error)
}
```

`NATDetail` now includes an optional `relay_addr` field. Older direct-only
signal implementations can omit it.

## References

- [Cloudflare Realtime TURN](https://developers.cloudflare.com/realtime/turn/)
- [How NAT traversal works](https://tailscale.com/blog/how-nat-traversal-works/)
- [The birthday problem](https://en.wikipedia.org/wiki/Birthday_problem)
