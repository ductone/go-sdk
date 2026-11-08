# C1 fork maintenance

`main` tracks `modelcontextprotocol/go-sdk` upstream. Keep C1-specific changes
off that branch so future upstream syncs remain straightforward.

`c1-main` is the integration branch for C1-specific changes. Open fork PRs
against `c1-main`; humans review and merge them there. C1 consumers pin an
immutable commit from this branch using a Go module replacement, preserving
the upstream module path.

For an upstream refresh:

1. Sync `main` with upstream.
2. Open a PR bringing `main` into `c1-main`, resolve conflicts with the fork
   changes, and run the SDK tests. Once fork changes have landed, preserve
   `c1-main` history rather than force-pushing a rewritten integration branch.
3. Update the consuming repository's replacement to the reviewed integration
   commit and run its MCP tests before releasing it.

The initial integration PR ports the distributed-session backend, originating
request context, synchronous initialize persistence, and stateful discovery
guard from the earlier fork stack. The restored-session eviction callback is
a separate follow-up; it is not part of that initial port.

Run `go test ./...` and `go build ./...` for the SDK. For distributed-session
changes, also run the focused race checks:

```sh
go test -race ./mcp -run 'Distributed|SessionBackend|Stateful|InitializeStateChange|InitializeResponseGated'
```
