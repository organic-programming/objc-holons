---
# Cartouche v1
title: "objc-holons — Objective-C SDK for Organic Programming"
author:
  name: "B. ALTER"
created: 2026-02-12
access:
  humans: true
  agents: false
status: draft
---
# objc-holons

**Objective-C SDK for Organic Programming** — transport and serve utilities
for building holons in Objective-C.

## Build & Test

```bash
clang -framework Foundation -I include src/Holons.m test/holons_test.m -o test_runner && ./test_runner
```

## API surface

| Symbol | Description |
|--------|-------------|
| `HOLParseURI(uri)` | Parse transport URI into normalized fields |
| `HOLListen(uri, &error)` | Create listener variant (`HOLTcpListener`, `HOLUnixListener`, `HOLStdioListener`, `HOLMemListener`, `HOLWSListener`) |
| `HOLScheme(uri)` | Extract transport scheme |
| `HOLParseFlags(args)` | CLI arg extraction |
| `HOLCloseListener(listener)` | Close/cleanup listener resources |

## Transport support

| Scheme | Support |
|--------|---------|
| `tcp://<host>:<port>` | Bound socket (`HOLTcpListener`) |
| `unix://<path>` | Bound UNIX socket (`HOLUnixListener`) |
| `stdio://` | Listener marker (`HOLStdioListener`) |
| `mem://` | Listener marker (`HOLMemListener`) |
| `ws://<host>:<port>` | Listener metadata (`HOLWSListener`) |
| `wss://<host>:<port>` | Listener metadata (`HOLWSListener`) |
