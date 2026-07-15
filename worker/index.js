/**
 * Tunnel Signal Server — Cloudflare Worker
 *
 * API:
 *   PUT  /signal/:room/:role   Upload NATDetail for this role ("server" or "client")
 *   GET  /signal/:room/:role   Long-poll for the peer's NATDetail (opposite role)
 *   GET  /turn-credentials     Return short-lived TURN credentials
 *
 * KV binding: SIGNAL_KV
 * Key format: <room>:<role>   Value: NATDetail JSON   TTL: 120s
 *
 * Deploy:
 *   1. Create a KV namespace named SIGNAL_KV in Cloudflare dashboard
 *   2. wrangler kv namespace create SIGNAL_KV
 *   3. Add binding to wrangler.toml:
 *        [[kv_namespaces]]
 *        binding = "SIGNAL_KV"
 *        id = "<your-kv-id>"
 *   4. For TURN, add TURN_KEY_ID and set TURN_KEY_API_TOKEN and
 *      TURN_ACCESS_TOKEN with `wrangler secret put`.
 *   5. wrangler deploy
 */

const TTL = 120;          // seconds a signal entry lives in KV
const POLL_INTERVAL = 500; // ms between KV reads during long-poll
const POLL_TIMEOUT = 60;  // seconds before long-poll gives up
const TURN_CREDENTIAL_TTL = 3600;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const parts = url.pathname.split("/").filter(Boolean);

    if (parts.length === 1 && parts[0] === "turn-credentials") {
      return handleTurnCredentials(request, env, url);
    }

    // Expected: /signal/:room/:role
    if (parts.length !== 3 || parts[0] !== "signal") {
      return new Response("Not Found", { status: 404 });
    }

    const [, room, role] = parts;
    if (role !== "server" && role !== "client") {
      return new Response("role must be 'server' or 'client'", { status: 400 });
    }

    if (request.method === "PUT") {
      return handlePut(request, env, room, role);
    }
    if (request.method === "GET") {
      return handleGet(request, env, room, role);
    }
    return new Response("Method Not Allowed", { status: 405 });
  },
};

async function handleTurnCredentials(request, env, url) {
  if (request.method !== "GET") {
    return new Response("Method Not Allowed", { status: 405 });
  }
  if (!env.TURN_KEY_ID || !env.TURN_KEY_API_TOKEN || !env.TURN_ACCESS_TOKEN) {
    return new Response("TURN fallback is not configured", { status: 503 });
  }
  const room = url.searchParams.get("room") ?? "";
  if (!/^[0-9a-f]{6}$/i.test(room)) {
    return new Response("invalid room", { status: 400 });
  }

  const authorization = request.headers.get("Authorization") ?? "";
  const expected = `Bearer ${env.TURN_ACCESS_TOKEN}`;
  const actualBytes = new TextEncoder().encode(authorization);
  const expectedBytes = new TextEncoder().encode(expected);
  if (
    actualBytes.byteLength !== expectedBytes.byteLength ||
    !crypto.subtle.timingSafeEqual(actualBytes, expectedBytes)
  ) {
    return new Response("Unauthorized", { status: 401 });
  }

  const upstream = await fetch(
    `https://rtc.live.cloudflare.com/v1/turn/keys/${env.TURN_KEY_ID}/credentials/generate-ice-servers`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${env.TURN_KEY_API_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ ttl: TURN_CREDENTIAL_TTL }),
    },
  );
  if (!upstream.ok) {
    console.error(JSON.stringify({
      event: "turn_credential_generation_failed",
      status: upstream.status,
    }));
    return new Response("TURN credential generation failed", { status: 502 });
  }

  return new Response(upstream.body, {
    status: 200,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
    },
  });
}

async function handlePut(request, env, room, role) {
  const body = await request.text();
  try {
    JSON.parse(body); // validate
  } catch {
    return new Response("invalid JSON", { status: 400 });
  }
  await env.SIGNAL_KV.put(`${room}:${role}`, body, { expirationTtl: TTL });
  return new Response("ok");
}

// Long-poll: keep reading KV until the peer's entry appears or timeout.
async function handleGet(request, env, room, role) {
  const peerRole = role === "server" ? "client" : "server";
  const key = `${room}:${peerRole}`;
  const deadline = Date.now() + POLL_TIMEOUT * 1000;

  while (Date.now() < deadline) {
    const value = await env.SIGNAL_KV.get(key);
    if (value !== null) {
      return new Response(value, {
        headers: { "Content-Type": "application/json" },
      });
    }
    await sleep(POLL_INTERVAL);
  }
  return new Response("timeout waiting for peer", { status: 408 });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
