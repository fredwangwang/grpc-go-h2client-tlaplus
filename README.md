# Modeling grpc-go's `http2Client` in TLA+

[中文版 README](README.zh-CN.md)

While working on [PR #9616](https://github.com/grpc/grpc-go/pull/9616) to fix [grpc/grpc-go#9559](https://github.com/grpc/grpc-go/issues/9559), [@eshitachandwani](https://github.com/eshitachandwani) had a super sharp catch during review and pointed out a second hang: a `NewStream` caller already parked waiting for stream quota wouldn't get woken up by `GracefulClose()` either.

While updating the PR based on her feedback, a thought hit me: if she hadn't spotted that, how long would that hang have stayed hidden in the code? And even after fixing both, how do we know there aren't *more* races hiding here—how do you actually **prove** it?

That's when TLA+ came to mind. I had AI help generate a small TLA+ model of `http2Client` and ran TLC (TLA+ Model Checker) on it.

You don't need to model all 1,900 lines of `http2_client.go` (framing, HPACK, flow control, BDP, etc. would just blow up the state space for no reason). Just slicing out the transport lifecycle (`reachable -> draining -> closing`) plus `NewStream`'s stream quota loop takes ~380 lines of TLA+.

Running TLC on it takes less than a second to:
1. Reproduce the original #9559 nil-channel hang on `master`.
2. Break the initial "just add a local `transportClosing` flag" fix by catching the exact second hang `@eshitachandwani` pointed out.
3. Verify that PR #9616's `gracefulClose` channel fixes both hangs.
4. Catch **two more edge cases** that were still hiding even after PR #9616.

## Quick start

You just need Java (JDK 11+). `run.sh` will grab `tla2tools.jar` on the first run if it's not there yet.

```bash
chmod +x run.sh

# run everything
./run.sh

# or run one specific scenario
./run.sh bug1_master
./run.sh verify_pr9616_3callers
./run.sh edge_busyspin
```

Or invoke TLC directly if you already have `tla2tools.jar`:

```bash
java -cp tla2tools.jar tlc2.TLC -deadlock -workers auto -cleanup \
  -config configs/bug1_master.cfg HTTP2Client.tla
```

## What's in here

To avoid copy-pasting the whole spec four times, all four code versions live in a single [HTTP2Client.tla](HTTP2Client.tla) behind a `FixMode` constant that switches the model's actual transition logic:

- `"none"` — `master` before #9616 (`gracefulClosed` never fires, `streamQuota` is decremented before the `draining` check).
- `"local_flag"` — returns early in `ExecuteCheckForStreamQuota` when `draining` is hit, but still has no `gracefulClosed` channel to wake callers already parked on quota.
- `"pr9616"` — enables the `gracefulClosed` channel (closed in `DoGracefulClose`, selected in `NewStreamSelectExit`) and stops decrementing `streamQuota` / `waitingStreams` when rejecting a stream on `draining`/`closing`.
- `"full_fix"` — builds on `"pr9616"` by failing fast in `ExecuteCheckForStreamQuota` (including `nextID > MaxStreamID`) instead of re-entering `select`, and also closing `gracefulClosed` when `Close()` transitions directly from `reachable` to `closing`.

Each scenario in `configs/*.cfg` just picks a `FixMode` and toggles optional behaviors (`EnableSettingsUpdate`, `EnableDirectClose`, `EnableStreamIDExhaustion`):

| Config | What it models | What happens |
| :--- | :--- | :--- |
| `configs/bug1_master.cfg` | `master` before #9616 (`InitMaxStreams=2`) | **Fails** `NewStreamTerminatesOnDrain` in 6 steps (163 states). Reproduces #9559: a long-lived stream keeps the transport in `draining`, `NewStream` sees `state == draining` with `ch == nil`, and parks in `select` forever. |
| `configs/bug2_local_flag.cfg` | The initial suggested fix (a local flag in `NewStream`, `InitMaxStreams=1`) | **Fails** `NewStreamTerminatesOnDrain` in 6 steps (184 states). When `streamQuota == 0`, `NewStream` hits the quota check *before* checking `draining` and parks on `streamsQuotaAvailable`—nothing wakes it after `GracefulClose()`. |
| `configs/verify_pr9616_m1.cfg` | PR #9616 (`gracefulClose chan`), `InitMaxStreams=1` | **Passes** (1,182 states, <1s). |
| `configs/verify_pr9616_m2.cfg` | PR #9616, `InitMaxStreams=2` | **Passes** (764 states, <1s). |
| `configs/verify_pr9616_3callers.cfg` | PR #9616 with 3 concurrent callers + `SETTINGS` updates + `Close()` + `MaxStreamID` exhaustion | **Passes** (24,662 states, ~1s). |
| `configs/edge_busyspin.cfg` | PR #9616 checking `NoBusySpinDuringClose` | **Fails** (614 states). See edge case #1 below. |
| `configs/edge_streamid.cfg` | PR #9616 checking `ValidStreamIDs` (`MaxStreamID=3`, 3 callers) | **Fails** (617 states). See edge case #2 below. |
| `configs/verify_full_fix.cfg` | Follow-up fix (`FixMode="full_fix"`) checking all invariants + liveness with 3 callers | **Passes** (14,347 states, ~1s). |

## The two extra edge cases TLC found

### 1. Busy-spin in `NewStream` during `Close()` (`edge_busyspin.cfg`)
When a server sends `SETTINGS` increasing `MAX_CONCURRENT_STREAMS`, `handleSettings` closes the old `t.streamsQuotaAvailable` channel and swaps in a new one.

Now imagine a `NewStream` goroutine is parked on that old `ch`, and `Close()` runs:
- `Close()` sets `t.state = closing` and `t.activeStreams = nil`, then waits up to `goAwayLoopyWriterTimeout` (5s) for `writerDone` *before* calling `t.cancel()`.
- Meanwhile `handleSettings` bumps `streamQuota > 0` and closes the old `ch`.
- The waiter wakes up from the closed `ch`, runs `checkForStreamQuota`, sees `t.activeStreams == nil`, and returns `false` **without clearing `ch` or returning an error**.
- Back in `select`, `<-ch` is still the old closed channel, so it's immediately ready again → tight 100% CPU loop hammering `controlBuf.mu` and `t.mu` until `t.cancel()` finally fires.

Fun detail: this bug already existed on `master`, where each loop decremented `t.streamQuota` (so with a huge `delta` it spun millions of times, e.g. ~2.2M iterations in 200ms in our Go repro, and only accidentally stopped for small `delta` by draining `streamQuota` down to 0). Once PR #9616 stopped decrementing `streamQuota` on `draining`/`closing`, even `delta = 1` turns into an unbounded spin for the whole `Close()` timeout.

### 2. Concurrent `NewStream` exceeding `MaxStreamID` (`edge_streamid.cfg`)
In `NewStream`, `t.nextID += 2` and `transportDrainRequired = t.nextID > MaxStreamID` happen under `t.mu`, but `t.GracefulClose()` isn't called until *after* `t.mu.Unlock()` (and after `statsHandler.HandleRPC`). Another `NewStream` slipping into that window still sees `t.state == reachable` and allocates a stream ID `> MaxStreamID`.

(In production `MaxStreamID` is `math.MaxInt32 * 3 / 4`, so there's a 25% buffer before hitting HTTP/2's 31-bit wire limit—still a neat race to catch, and a one-liner to plug by checking `t.nextID > MaxStreamID` under `t.mu`.)

## The trick that actually makes TLA+ catch #9559

Writing the spec isn't the hard part—getting the fairness assumptions right is.

If you put Weak Fairness (`WF`) on `CloseStream`, you're basically telling TLC "every open stream eventually finishes." Under that assumption, `draining` always reaches `activeStreams == {}`, `Close()` always cancels `t.ctx`, and `NewStream` always unblocks—so TLC happily says "no error found." That's the exact same blind spot that let `TestGracefulClose` pass for years.

Leaving `CloseStream` **unfair** (so a stream is allowed to stay open forever, like an etcd watch or lease keepalive stream) while keeping fairness on internal goroutine steps is what makes TLC immediately spit out the real-world hang.

## Follow-up fix (`FixMode = "full_fix"`)

On top of PR #9616:
- In `checkForStreamQuota`, check `t.state == draining || t.activeStreams == nil || t.nextID > MaxStreamID` under `t.mu`, record `errStreamDrain` (if `t.goAway` is closed) or `ErrConnClosing`, and fail fast in the `for` loop instead of re-entering `select` with a `nil` or already-closed `ch`.
- In `Close()`, when transitioning directly from `reachable` to `closing` (`t.state != draining`), also `close(t.gracefulClose)` so waiters don't sit around waiting on `goAwayLoopyWriterTimeout`.

`configs/verify_full_fix.cfg` checks all invariants (`TypeOK`, `NoBusySpinDuringClose`, `ValidStreamIDs`) plus `NewStreamTerminatesOnDrain` across 3 concurrent callers and passes with 0 errors.
