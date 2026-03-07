# objc-holons

**Objective-C SDK for Organic Programming** — transport primitives,
serve-flag parsing, identity parsing, discovery, and a Holon-RPC
client.

## Build & Test

```bash
clang -framework Foundation -I include src/Holons.m test/holons_test.m -o test_runner && ./test_runner
```

## API surface

| Symbol | Description |
|--------|-------------|
| `HOLParseURI(uri)` | Parse transport URI into normalized fields |
| `HOLListen(uri, &error)` | Create a listener variant |
| `HOLAccept(listener, &error)` | Accept one runtime connection |
| `HOLMemDial(listener, &error)` | Dial the client side of a `mem://` listener |
| `HOLScheme(uri)` | Extract transport scheme |
| `HOLParseFlags(args)` | CLI arg extraction |
| `HOLParseHolon(path, &error)` | Parse `holon.yaml` into `HOLHolonIdentity` |
| `HOLDiscover(root, &error)` | Discover holons under a root |
| `HOLDiscoverLocal(&error)` | Discover from the current working directory |
| `HOLDiscoverAll(&error)` | Discover from local, `$OPBIN`, and cache roots |
| `HOLFindBySlug(slug, &error)` | Resolve a holon by slug |
| `HOLFindByUUID(prefix, &error)` | Resolve a holon by UUID prefix |
| `HOLHolonRPCClient` | Holon-RPC client |

## Current scope

- Runtime transports: `tcp://`, `unix://`, `stdio://`, `mem://`
- `ws://` and `wss://` are metadata-only at the transport layer
- Discovery scans local, `$OPBIN`, and cache roots

## Current gaps vs Go

- No generic slug-based `connect()` helper yet.
- No full gRPC `serve` lifecycle helper yet.
- No Holon-RPC server module yet.
