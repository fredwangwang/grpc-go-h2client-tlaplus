# 用 TLA+ 建模 grpc-go 的 `http2Client`

[English README](README.md)

在 [PR #9616](https://github.com/grpc/grpc-go/pull/9616) 里修 [grpc/grpc-go#9559](https://github.com/grpc/grpc-go/issues/9559) 的时候，[@eshitachandwani](https://github.com/eshitachandwani) 在 review 里非常敏锐地指出了另一个挂死问题：如果 `NewStream` 已经先卡在等流配额的 `select` 里，后面再调 `GracefulClose()` 照样没人叫醒它。

按她的建议改代码的时候我就在想：要不是她发现了这个问题，这个挂死是不是又得潜伏好久？怎么知道这块代码里还有没有其他竞态，到底要怎么去**证明**它没问题？然后就想到了 TLA+，顺手让 AI 帮忙生成了一份 `http2Client` 的 TLA+ 模型跑了下 TLC (TLA+ Model Checker)。

模型里没必要把 1900 行 `http2_client.go` 全搬进去（帧解析、HPACK、流控这些东西放进去只会把状态空间直接搞炸），只把 transport 生命周期（`reachable -> draining -> closing`）和 `NewStream` 抢流配额这块抽出来，大概 380 行就够了。

跑了一遍 TLC，不到 1 秒钟干了四件事：
1. 直接复现出 `master` 上 #9559 原来的 `nil` channel 卡死。
2. 顺手证伪了最初“加个局部 `transportClosing` flag”的改法——直接跑出了 `@eshitachandwani` 指出的那个配额挂死。
3. 验证了 PR #9616 加 `gracefulClose chan` 确实能把这两个挂死都修掉。
4. 还顺带抓出了**另外两个挺隐蔽的边界问题**（哪怕在 PR #9616 上也还存在）。

## 怎么跑

本机有 Java（JDK 11+）就行。`run.sh` 第一次跑会自动下 `tla2tools.jar`。

```bash
chmod +x run.sh

# 一口气全跑一遍
./run.sh

# 或者单跑某一个场景
./run.sh bug1_master
./run.sh verify_pr9616_3callers
./run.sh edge_busyspin
```

如果你手头已经有 `tla2tools.jar`，直接调 TLC 也一样：

```bash
java -cp tla2tools.jar tlc2.TLC -deadlock -workers auto -cleanup \
  -config configs/bug1_master.cfg HTTP2Client.tla
```

## 各个配置都在测什么

为了不把整份 spec 复制粘贴四遍，四个版本的代码逻辑都写在同一个 [HTTP2Client.tla](HTTP2Client.tla) 里，靠 `FixMode` 常量直接切换模型的状态转移逻辑：

- `"none"` — 修复前的 `master`（不会触发 `gracefulClosed`，且 `draining` 检查之前就会扣减 `streamQuota`）。
- `"local_flag"` — 撞到 `draining` 时在 `ExecuteCheckForStreamQuota` 里直接返回，但没有 `gracefulClosed` channel 去唤醒已经卡在配额上的调用方。
- `"pr9616"` — 开启 PR #9616 的改动：在 `DoGracefulClose` 里把 `gracefulClosed` 置 `TRUE`、在 `NewStreamSelectExit` 里监听它，并在 `draining`/`closing` 拒单时不再动 `streamQuota` 和 `waitingStreams`。
- `"full_fix"` — 在 `"pr9616"` 上再补两块：`ExecuteCheckForStreamQuota` 里同时拦 `nextID > MaxStreamID` 并直接报错返回（不再掉回 `select`），以及 `Close()` 从 `reachable` 直接切到 `closing` 时也把 `gracefulClosed` 置 `TRUE`。

`configs/*.cfg` 里就是挑不同的 `FixMode` 和几个环境开关（`EnableSettingsUpdate`、`EnableDirectClose`、`EnableStreamIDExhaustion`）：

| 配置文件 | 对应版本 / 场景 | TLC 结果 |
| :--- | :--- | :--- |
| `configs/bug1_master.cfg` | 修复前的 `master`（`InitMaxStreams=2`） | **挂**（163 个状态，6 步反例）。直接复现 #9559：有条长流挂着，`GracefulClose` 进 `draining`，新 `NewStream` 看到 `draining` 返回 `false` 但 `ch` 还是 `nil`，在 `select` 里永久死等。 |
| `configs/bug2_local_flag.cfg` | 只加局部 flag 的初版思路（`InitMaxStreams=1`） | **挂**（184 个状态，6 步反例）。`streamQuota == 0` 时，代码先判断配额不足直接进 `select` 等 `streamsQuotaAvailable`，根本没走到 `draining` 检查，后面调 `GracefulClose()` 照样没人叫醒它。 |
| `configs/verify_pr9616_m1.cfg` | PR #9616（`gracefulClose chan`），`InitMaxStreams=1` | **通过**（1,182 个状态，<1s）。 |
| `configs/verify_pr9616_m2.cfg` | PR #9616，`InitMaxStreams=2` | **通过**（764 个状态，<1s）。 |
| `configs/verify_pr9616_3callers.cfg` | PR #9616，3 个并发调用方 + `SETTINGS` 扩容 + 直接 `Close()` + `MaxStreamID` 耗尽 | **通过**（24,662 个状态，约 1s）。 |
| `configs/edge_busyspin.cfg` | PR #9616 下检查 `NoBusySpinDuringClose` | **挂**（614 个状态），见下面边界 1。 |
| `configs/edge_streamid.cfg` | PR #9616 下检查 `ValidStreamIDs`（`MaxStreamID=3`，3 个调用方） | **挂**（617 个状态），见下面边界 2。 |
| `configs/verify_full_fix.cfg` | 补丁后的完整版（`FixMode="full_fix"`），3 个调用方同时查所有不变量 + 活性 | **通过**（14,347 个状态，约 1s）。 |

## TLC 顺手抓出来的两个额外边界

### 1. `Close()` 期间旧 `streamsQuotaAvailable` 导致的 100% CPU 空转（`edge_busyspin.cfg`）
服务端发 `SETTINGS` 调大 `MAX_CONCURRENT_STREAMS` 时，`handleSettings` 会 `close` 掉旧的 `t.streamsQuotaAvailable` 再换个新的。

如果此时刚好卡在这么个交错里：
- 一个 `NewStream` 协程因为配额满了，正捏着旧 `ch` 停在 `select` 里；
- `Close()` 执行了前半段：把 `t.state = closing`、`t.activeStreams = nil`，然后卡在等 `writerDone`（最长等 `goAwayLoopyWriterTimeout` 也就是 5 秒），还没调 `t.cancel()`；
- `handleSettings` 这时进来把 `streamQuota` 加成 `> 0`，顺手 `close` 了那个旧 `ch`；
- 协程被关闭的旧 `ch` 唤醒，进 `checkForStreamQuota` 看到 `t.activeStreams == nil`，直接 `return false`，**但既没把 `ch` 清掉也没报错返回**；
- 回到 `select`，`<-ch` 还是那个已经关闭的旧 channel，立马就绪，于是陷入死循环疯狂抢 `controlBuf.mu` 和 `t.mu`，单核 CPU 直接打满，一直空转到 `t.cancel()` 执行为止。

比较有意思的是：这坑 `master` 上原本就有，只不过以前每次循环会顺手把 `t.streamQuota--`，所以 `delta` 大的时候会空转几百万次（Go 单测里 200ms 空转了 224 万次），`delta` 小的时候反而意外靠“把配额扣成 0 后覆盖 `ch`”刹住了车。PR #9616 合理地去掉了 `draining`/`closing` 时的 `streamQuota--`，结果哪怕 `delta = 1` 也会一直空转到 `Close()` 超时。

### 2. `nextID > MaxStreamID` 和 `GracefulClose()` 之间的并发窗口（`edge_streamid.cfg`）
`NewStream` 在持有 `t.mu` 时做 `t.nextID += 2` 和 `transportDrainRequired = t.nextID > MaxStreamID`，但释放完锁、甚至跑完 `statsHandler.HandleRPC` 之后才去调 `t.GracefulClose()`。在这中间溜进来的并发 `NewStream` 看到的还是 `state == reachable`，会直接分出超过 `MaxStreamID` 的 stream ID。

（生产里 `MaxStreamID` 设在 `math.MaxInt32 * 3 / 4`，离 HTTP/2 协议的 31-bit 上限还有 25% 缓冲，倒不至于直接炸协议；不过在 `t.mu` 里顺手加个 `t.nextID > MaxStreamID` 判断一行就能堵上。）

## 建模时最关键的一个坑

写这个模型本身不难，真正决定能不能抓出 #9559 的是 **Fairness 怎么设**。

如果你顺手给 `CloseStream` 加了 Weak Fairness（`WF`），就等于告诉 TLC：“所有打开的流最后都会关掉。”那 `draining` 迟早会等到 `activeStreams` 清空、`Close()` 迟早会关掉 `t.ctx`，TLC 跑完只会笑眯眯地告诉你“没问题”。这和原仓库里 `TestGracefulClose` 跑了这么多年一直过、却没拦住生产事故，其实是同一个盲区。

把 `CloseStream` 留成 **不公平的（允许一直不发生）**，来模拟 etcd watch / lease keepalive 这种永不主动结束的长流，只给客户端内部推进动作加公平性，TLC 立刻就能把真实的挂死路径吐出来。

## 补全修复（`FixMode = "full_fix"`）

在 PR #9616 的基础上再补两小块：
- `checkForStreamQuota` 在 `t.mu` 锁内把判断扩成 `t.state == draining || t.activeStreams == nil || t.nextID > MaxStreamID`，同时非阻塞看一眼 `t.goAway` 记下该返回的错误（`errStreamDrain` 或 `ErrConnClosing`），在 `for` 循环里直接返回，不再带着 `nil` 或已关闭的 `ch` 掉回 `select`。
- `Close()` 从 `reachable` 直接切到 `closing`（`t.state != draining`）时也顺手 `close(t.gracefulClose)`，免得等配额的调用干等 5 秒超时。

`configs/verify_full_fix.cfg` 用 3 个并发调用方把所有不变量（`TypeOK`、`NoBusySpinDuringClose`、`ValidStreamIDs`）和活性规约 `NewStreamTerminatesOnDrain` 一起验了一遍，全部 0 错误通过。
