---
# Cartouche v1
title: "objc-holons — Objective-C SDK for Organic Programming"
author:
  name: "B. ALTER"
created: 2026-02-12
revised: 2026-02-13
access:
  humans: true
  agents: false
status: draft
---
# objc-holons

**Objective-C SDK for Organic Programming** — transport, serve, identity,
and Holon-RPC client utilities for building holons in Objective-C.

## Build & Test

```bash
clang -framework Foundation -I include src/Holons.m test/holons_test.m -o test_runner && ./test_runner
```

## API surface

| Symbol | Description |
|--------|-------------|
| `HOLParseURI(uri)` | Parse transport URI into normalized fields |
| `HOLListen(uri, &error)` | Create listener variant (`HOLTcpListener`, `HOLUnixListener`, `HOLStdioListener`, `HOLMemListener`, `HOLWSListener`) |
| `HOLAccept(listener, &error)` | Accept one runtime connection (`tcp`, `unix`, `stdio`, `mem`) |
| `HOLMemDial(listener, &error)` | Dial the client side of a `mem://` listener |
| `HOLConnectionRead(conn, buf, n)` | Read from connection |
| `HOLConnectionWrite(conn, buf, n)` | Write to connection |
| `HOLCloseConnection(conn)` | Close connection resources |
| `HOLScheme(uri)` | Extract transport scheme |
| `HOLParseFlags(args)` | CLI arg extraction |
| `HOLParseHolon(path, &error)` | Parse HOLON.md YAML frontmatter into `HOLHolonIdentity` |
| `HOLCloseListener(listener)` | Close/cleanup listener resources |
| `HOLHolonRPCClient` | `connect(url)`, `invoke(method, params)`, `registerMethod(method, handler)`, `close()` |

## Transport support

| Scheme | Support |
|--------|---------|
| `tcp://<host>:<port>` | Bound socket (`HOLTcpListener`) |
| `unix://<path>` | Bound UNIX socket (`HOLUnixListener`) |
| `stdio://` | Native runtime accept (single-connection semantics) |
| `mem://` | Native runtime in-process pair (`HOLMemDial` + `HOLAccept`) |
| `ws://<host>:<port>` | Listener metadata (`HOLWSListener`) |
| `wss://<host>:<port>` | Listener metadata (`HOLWSListener`) |

## Parity Notes vs Go Reference

Implemented parity:

- URI parsing and listener dispatch semantics
- Runtime accept path for `tcp`, `unix`, `stdio`, and `mem`
- In-process `mem://` client/server connection pair (`HOLMemDial` + `HOLAccept`)
- Holon-RPC client protocol support over `ws://` / `wss://` (JSON-RPC 2.0, heartbeat, reconnect)
- Standard serve flag parsing
- HOLON identity parsing

Not yet achievable in this minimal Objective-C core (justified gaps):

- `ws://` / `wss://` runtime listener parity:
  - Exposed as metadata only.
  - A full Go-style WebSocket runtime listener would require additional HTTP/WebSocket runtime integration not yet included.
- Full gRPC transport parity (`Dial("tcp://...")`, `Dial("stdio://...")`, `Listen("stdio://...")`, and `Serve.Run()` wiring):
  - Not present yet; requires a dedicated Objective-C gRPC integration layer and stdio transport adaptation.
