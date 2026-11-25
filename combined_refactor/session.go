package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var globalTaskMutex sync.Mutex
var backgroundTaskMutex sync.Mutex

// S6 测试 hook（v7 §9.4；计划 §2.4）：变量与类型声明在生产代码，nil 默认。
// 赋值与清除仅在 _test.go。生产环境只执行一次 nil 检查，无行为变化。
// writeDelayGate.once 保证 entered 只关闭一次（多 worker 并发到达时第二次
// 关闭已关闭的通道会 panic；once 使后续到达只阻塞于 release，不重复关闭）。
type writeDelayGate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

var testWriteDelayGate *writeDelayGate
var testEndTaskEnteredHook func()

// 在途重放任务切换测试门（本轮修复）：重放循环在每行成功写出后（已释放
// resultSendMu）检查；写出数达到 afterRows 时关闭 entered 并阻塞于 release，
// 测试据此在重放中途启动新任务。生产为 nil，一次 nil 检查；赋值与清除仅
// 在 _test.go。once 保证 entered 只关闭一次。
type replayPauseGate struct {
	afterRows int
	entered   chan struct{}
	release   chan struct{}
	once      sync.Once
}

var testReplayPauseGate *replayPauseGate

// S7 测试门（v7.10 §9A.3/§10A.4/§11.5.7）：变量与类型声明在生产代码，nil 默认。
// 赋值与清除仅在 _test.go（经 testHookGet/testHookSet 同步，见 testHookMu 注释）。
// disconnectTestGate：entered 在到达门时关闭一次（once 保证），release 由测试关闭；
// 生产为 nil，一次 nil 检查。注意：arm 门在持有 taskMutex 时暂停（见 armDisconnectTimer），
// 回调门在锁释放后、cancel 调用前暂停（见 onDisconnectTimer）；测试必须在
// defer/cleanup 中关闭 release，否则生产 goroutine 将永久阻塞于门。
type disconnectTestGate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newDisconnectTestGate() *disconnectTestGate {
	return &disconnectTestGate{entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *disconnectTestGate) pass() {
	if g == nil {
		return
	}
	g.once.Do(func() { close(g.entered) })
	<-g.release
}

var testDisconnectArmGate *disconnectTestGate
var testDeferAEntryGate *disconnectTestGate
var testTimerCallbackGate *disconnectTestGate
var testTimerCallbackEntryGate *disconnectTestGate
var testTimerCallbackPhaseGate *disconnectTestGate

// testWatchdogTickGate 在看门狗 tick 的 Load 与 CAS 之间暂停（已读旧快照、
// 未提交）。用于 T7-29（旧代次 tick 与 reset 的 CAS 隔离）与 T7-40（暂停
// tick 以模拟挂起期间无 tick）。生产为 nil。门暂停时 tick goroutine 不持
// 有任何锁；测试必须在 defer/cleanup 中关闭 release，否则 tick 将永久阻塞
// 于门（且 stopWatchdog 会因 done 迟迟不关闭而阻塞——先放门、再停止，见
// stopWatchdog 注释）。
var testWatchdogTickGate *disconnectTestGate

// testStopWatchdogGate 在 stopWatchdog 的 done 检查之后、关闭步骤之前暂停
// 停止调用：该调用已确认要停止此实例、尚未触碰关闭保护。用于 T7-B2-R1 确
// 定性复现跨实例停止交错。生产为 nil。门内不持有任何锁；测试必须在
// cleanup 中关闭 release，否则停止调用将永久阻塞于门。
var testStopWatchdogGate *disconnectTestGate

// testTimerPublishGate 在每次 timer 发布点暂停发布线程：time.AfterFunc 已返回、
// self.Store 尚未执行。用于 T7-B1-R1/R2 确定性复现“发布尚未完成”的窗口。
// 生产为 nil。门暂停时发布线程持有 taskMutex，测试不得在门内读 taskMutex 保护状态。
var testTimerPublishGate *disconnectTestGate

// testTimerCallbackStartedHook 在 timer 闭包入口调用（等待 ready 之前）：
// 回调 goroutine 已启动的非阻塞信号，不暂停回调、不读指针、不触碰发布状态。
// 用于 T7-B1-R1/R2 区分“回调已启动”与“回调被发布屏障阻塞”。生产为 nil。
// 注意：生产环境（hook 为 nil）下闭包的第一项同步操作就是等待 ready；
// 该 hook 只是测试观测手段，不参与发布协议，不改变 happens-before。
var testTimerCallbackStartedHook func()

// testTimerCallbackLoadedHook 在回调通过 ready 屏障、Load 得到 timer 指针后调用
// （进入 onDisconnectTimer 之前）；测试用它记录 Load 返回的指针并与
// s.disconnectTimer 比较，验证发布正确性。生产为 nil。
var testTimerCallbackLoadedHook func(myTimer *time.Timer)

// testTimerRearmHook 在计时器回调的 defer 分支成功重武装后调用（锁外，
// 参数为本次重武装时长）；测试用它计数重武装次数（T7-23）。生产为 nil。
var testTimerRearmHook func(rearm time.Duration)

// S7 C1 测试门与 hook（认领协议）：生产声明、测试赋值；赋值与清除仅在
// _test.go（经 testHookGet/testHookSet 同步）。testBeginTaskCommitGate 在
// beginTask 内、pageId 已读且 taskMutex 已取、身份字段尚未写入时暂停
// （T7-30；暂停时持有 resultSendMu + globalTaskMutex + backgroundTaskMutex
// + taskMutex，测试不得在门内读任何一把锁保护的状态）；testClaimEntryGate
// 在认领 handler 入口、取 backgroundTaskMutex 之前暂停（T7-21/T7-34；
// 门内不持有任何锁）；testClaimCommitGate 在认领核验通过后、状态转换之前
// 暂停（T7-18/T7-20/T7-21；持有 backgroundTaskMutex + 候选 session 的
// taskMutex）；testArmCountHook 在 armDisconnectTimer 成功武装后调用
// （T7-16/T7-17 的武装计数，生产为 nil）。测试必须在 defer/cleanup 中
// 关闭 release，否则生产 goroutine 将永久阻塞于门。
var testBeginTaskCommitGate *disconnectTestGate
var testClaimEntryGate *disconnectTestGate
var testClaimCommitGate *disconnectTestGate
var testArmCountHook func()

// S7 C1 O-1 定点修正的测试门与 hook（task_started 身份快照）：生产声明、
// 测试赋值；赋值与清除仅在 _test.go（经 testHookGet/testHookSet 同步）。
// testTaskStartedGate 在 publishTaskStarted 入口、发送决策之前暂停
// （O-1 测试 B；门内不持有任何业务锁，测试可在门内执行
// resetBackendGeneration）；testTaskStartedDoneHook 在每次发布决策完成后
// 调用（发送或抑制均调用，生产为 nil）；testTaskStartedPayloadHook 在
// 实际发送之前调用，参数为本次将要发送的 (taskId, backendEpoch)，供测试
// 观测发布负载、判定混合身份（生产为 nil）。测试必须在 defer/cleanup 中
// 关闭 release，否则生产 goroutine 将永久阻塞于门。
var testTaskStartedGate *disconnectTestGate
var testTaskStartedDoneHook func()
var testTaskStartedPayloadHook func(taskID, epoch string)

// S7 C1 O-1 第二轮定点修正的测试门（taskStartedIdentityValid 竞态）：生产
// 声明、测试赋值；赋值与清除仅在 _test.go（经 testHookGet/testHookSet 同步）。
// testTaskStartedValidGate 在 taskStartedIdentityValid 取 taskMutex 后、读
// epoch 之前暂停——门内持有该 session 的 taskMutex，测试不得在门内取该锁
// 读状态（会死锁）；testResetEpochGate 在 resetBackendGeneration 发布新
// epoch 后、执行后续步骤（清空全局状态、关闭旧连接、取消孤儿）之前暂停——
// 门内持有 globalTaskMutex + backgroundTaskMutex，测试不得在门内取这两把
// 锁。测试必须在 defer/cleanup 中关闭 release，否则生产 goroutine 将永久
// 阻塞于门。
var testTaskStartedValidGate *disconnectTestGate
var testResetEpochGate *disconnectTestGate

// testWatchdogSnapshotOverride 是测试注入的看门狗快照（T7-23/T7-25）：
// 非 nil 时 loadWatchdogSnapshot 返回它而不读 watchdogSnapshot。
// 快照一经设置即不可变；测试须以新指针替换，不得原地修改字段。
var testWatchdogSnapshotOverride *watchdogState

var backgroundTaskSession *appSession

// 第一阶段性能日志：仅输出日志，不改变任何行为。
// wsConnectionSeq/taskSeq 分别为 WS 连接和任务分配单调递增标识，用于区分旧连接/旧任务与新连接/新任务。
var wsConnectionSeq int64
var taskSeq int64

// R3 代次与状态表（见设计 §2.3/§2.4/§2.9/§11）。
// 五张 map 与 runningTasksByGeneration 在包级声明时经 make 初始化，
// resetBackendGeneration 重建时用 make 赋新 map、永不赋 nil
// （nil-map 写会 panic，见 §7.4.1 的不变式）。
var backendGeneration uint64        // 进程级单调，从 0 开始；只由 resetBackendGeneration 递增
var backendEpochAtomic atomic.Value // epoch 的唯一来源；读取经 currentBackendEpoch()
var taskIdentityVersion int64       // 全局任务身份版本，每代次从 0 开始（见 §6.2）
var runningTasksByGeneration = make(map[uint64]int32)
var publishLedger = make(map[string]ledgerEntry)
var pageSessions = make(map[string]map[*appSession]pageSessionEntry)
var pendingRecoveries = make(map[string]*pendingRecoveryEntry)
var resolvedRecoveries = make(map[string]resolvedEntry)
var connLoopState = make(map[string]loopEntry)
var sweeperLive atomic.Int32  // 每代次 sweeper 唯一性（见 §2.10）
var watchdogLive atomic.Int32 // 每进程看门狗唯一性（S7 §11.5.4，Batch 2）
// watchdogInstance 是一次看门狗启动的身份：它的 stop/done 通道与保护其
// stop 通道的 closeOnce。每次 startWatchdog 成功调用创建一个新实例，永不
// 重置、永不跨实例复用——旧实例的停止调用永远只触碰旧实例的 Once。
type watchdogInstance struct {
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// watchdogInstances 按 stop 通道查找拥有它的实例。停止调用方手里的 stop
// 通道本身就是实例身份，因此无需改动 startWatchdog/stopWatchdog 的签名
// （main.go 的调用点保持不变）。
// 条目永不删除：延迟的停止调用可能在实例退出后、甚至在新实例启动后的任意
// 时刻才来查找，必须仍解析到原来的 Once；若删除条目，迟到的查找会新建一
// 个全新的 Once 并再次关闭已关闭的通道——正是本修正要消除的竞态。每启动一
// 次看门狗增加一个条目（生产每次后端启动一个；测试最多数十个），可忽略。
var watchdogInstances sync.Map        // map[chan struct{}]*watchdogInstance
var serverPublishSeq int64            // 兼容 publishId 生成计数器（见 §7.6）
var recoveryQueueRejects atomic.Int64 // 容量拒绝计数（观测用）

// S7: 断线宽限与看门狗参数（v7.10 §9.2/§11.5.8）。均为 var，供测试覆盖；
// 生产默认值不得更改：宽限 30 秒（已决策）；tick 间隔 1 秒、挂起识别阈值
// 5 秒、容差 1 秒（已确定）；minPostResumeOpportunity 10 秒为待确认建议值，
// 不是已单独批准的产品参数，不得描述为已完成真机验证或不可调的产品承诺。
// disconnectGracePeriod 例外：atomic.Int64（纳秒），读写一律经 Load/Store
// （H-03 P1）；零值 0 会使宽限失效，故 init() 存入默认值。
var disconnectGracePeriod atomic.Int64

func init() {
	disconnectGracePeriod.Store(int64(30 * time.Second))
}

var watchdogTickInterval = time.Second
var watchdogSuspendThreshold = 5 * time.Second
var watchdogTickTolerance = time.Second
var minPostResumeOpportunity = 10 * time.Second

// S7 C1 F-1: connLoopState 的 exited 条目保留期（sweeper 删除阈值）。var 供
// 测试覆盖（F-1 测试缩短保留期以触发删除路径）；生产默认值 5 分钟，不得更改。
var connLoopStateRetention = 5 * time.Minute

// R3 测试钩子（见 §16.3）：全部包级变量，生产恒 nil；
// 检查 if gate != nil 后才阻塞/记录，不改变生产时序。
// testMainListenerHook / testMainExitedHook 的落点在 main.go / mobile.go.in
// （第二轮补齐任务 4/5 已接线）；访问同样经 testHookGet/testHookSet。
var (
	testSlowHandlerGate      chan struct{}
	testRegisterGate         chan struct{}
	testRegisterAckGate      chan struct{}
	testRegisterReadGate     chan struct{}
	testReadDeadlineOverride *time.Duration
	testWriteTimestampLog    func(event string)
	testRecoverySendGate     chan struct{}
	testQueueLenGate         func(queueLen int)
	testPublishPanicGate     chan struct{}
	testPublishPanicGate2    chan struct{}
	testPublishDeferGate     chan struct{}
	testSetupGate            chan struct{}
	// testSetupGateReached：testSetupGate 的到达信号（实施期补充的第 18 个钩子，
	// §16.3 之外；生产恒 nil）。r3Dial 返回只保证客户端握手完成，不保证
	// 服务端 goroutine 已创建 session——若测试在 session 创建前调用 reset，
	// session 会带着新代次出生，代次隔离测试的前提即失效（-race 下已偶发）。
	// 服务端抵达 gate 时向本通道（cap 1）发一次信号，测试收到后再 reset。
	testSetupGateReached       chan struct{}
	testPublishLockGate        chan struct{}
	testPublishReturnGate      chan struct{}
	testPublishRecoverSendGate chan struct{}
	testRollbackPanicGate      chan struct{}
	testReconcilePanicGate     *reconcilePanicGate
	testMainListenerHook       func(net.Listener)
	testMainExitedHook         func()
)

// testHookMu 同步全部 R3 测试钩子（实施期修正，见交付报告）：
// hook 由测试代码写入、由服务端 goroutine（读循环、探测、sender）读取；
// 测试结束清理时探测可能仍在飞行，直接读写会在 -race 下报 data race。
// 因此全部 hook 访问都经本锁 + testHookGet/testHookSet；生产恒 nil，
// 读到 nil 即跳过，生产行为与时序不变。testHookMu 是叶子锁：
// 持有它时不取其他任何锁；生产读路径只取 RLock 读值后立即释放，
// 阻塞/调用都在锁外，不形成锁序（见 §12）。
var testHookMu sync.RWMutex

// testHookGet 在 RLock 内读一次 hook 值（调用方随后在锁外使用返回值）。
func testHookGet[T any](p *T) T {
	testHookMu.RLock()
	defer testHookMu.RUnlock()
	return *p
}

// testHookSet 在 Lock 内写一次 hook 值（仅测试代码调用）。
func testHookSet[T any](p *T, v T) {
	testHookMu.Lock()
	defer testHookMu.Unlock()
	*p = v
}

func perfLogLine(event, details string) {
	fmt.Printf("[perf ts=%s event=%s %s]\n", time.Now().Format(time.RFC3339Nano), event, details)
}

// taskIDSnapshot 在持有 taskMutex 时把 taskID 复制到局部变量再返回，
// 供其他 goroutine 的日志读取使用，避免与 beginTask/endTask 的写入形成数据竞争。
// connectionID 在会话创建后不再修改，读取它不需要加锁。
func (s *appSession) taskIDSnapshot() string {
	s.taskMutex.Lock()
	defer s.taskMutex.Unlock()
	return s.taskID
}

// anyTaskRunningLocked 假设已持有 globalTaskMutex（见 §2.4）。
// 返回当前代次是否有任务在运行（旧代次的孤儿任务不阻塞新代次）。
func anyTaskRunningLocked() bool {
	return runningTasksByGeneration[atomic.LoadUint64(&backendGeneration)] > 0
}

func anyTaskRunning() bool {
	globalTaskMutex.Lock()
	defer globalTaskMutex.Unlock()
	return anyTaskRunningLocked()
}

func safeGo(label string, session *appSession, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				msg := fmt.Sprintf("%s panic: %v", label, r)
				fmt.Printf("%s\n%s\n", msg, debug.Stack())
				recordProgramDebugError("panic", fmt.Sprintf("%s\n%s", msg, debug.Stack()))
				if session != nil {
					session.sendWSMessage("error", "内部错误: "+msg)
				}
			}
		}()
		fn()
	}()
}

// 五项修复问题 2：任务身份绑定（context 携带）。
// 任务启动时（startTaskNamed/runTaskSync 在 beginTask 成功后）把本任务的
// *taskResultLog 存入 ctx；worker 启动即继承该 ctx，发送时凭此证明身份。
// 身份 = 日志的 (taskID, epoch)——taskID 在进程内单调递增（task-N），epoch
// 区分进程代次；二者合起来唯一标识一个任务。不得只比较 resultLog 指针：
// 指针比较不验证任务身份，且与“当前日志”比较时存在 TOCTOU；此处比较的是
// 日志内记录的任务身份字段。
type taskLogKeyType struct{}

var taskLogKey taskLogKeyType

// withTaskLog 把任务日志绑定到 ctx（任务启动时调用一次）。
func withTaskLog(ctx context.Context, log *taskResultLog) context.Context {
	return context.WithValue(ctx, taskLogKey, log)
}

// taskLogFromCtx 从 ctx 取出绑定的任务日志；未绑定（直接调用 / 旧路径）返回 nil。
func taskLogFromCtx(ctx context.Context) *taskResultLog {
	if ctx == nil {
		return nil
	}
	log, _ := ctx.Value(taskLogKey).(*taskResultLog)
	return log
}

// S6: withSeq 把 seq 注入结果载荷并返回新值（不修改调用方的数据）。
// ScanResult / TestResult / nsbScanMessage 用小写 json:"seq" 字段（v7 §5.3）。
// 五项修复问题 1：speed_test_result 改用 map[string]interface{}，seq 以 JSON
// number 写入（int64），与 scan/test/nsb 一致；前端 applyResultRow 只接受
// number seq，四类结果至此进入同一序号处理路径。map[string]string 不能承载
// JSON number，故该分支已删除（不再有生产调用点使用它）。
func withSeq(payload interface{}, seq int64) interface{} {
	switch p := payload.(type) {
	case ScanResult:
		p.Seq = seq
		return p
	case *ScanResult:
		cp := *p
		cp.Seq = seq
		return cp
	case TestResult:
		p.Seq = seq
		return p
	case *TestResult:
		cp := *p
		cp.Seq = seq
		return cp
	case nsbScanMessage:
		p.Seq = seq
		return p
	case *nsbScanMessage:
		cp := *p
		cp.Seq = seq
		return cp
	case map[string]interface{}:
		out := make(map[string]interface{}, len(p)+1)
		for k, v := range p {
			out[k] = v
		}
		out["seq"] = seq
		return out
	default:
		return payload
	}
}

// S6: mustJSON 把载荷序列化为 JSON（用于日志行 Data）。本轮载荷均为可序列化
// 的结构体或 map，序列化不会失败；失败时记调试日志并返回 "{}"，绝不 panic。
func mustJSON(v interface{}) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		recordDebugNotice("must_json_failed", err.Error())
		return json.RawMessage("{}")
	}
	return json.RawMessage(b)
}

// S6: sendTaskResult 是结果行的唯一发送入口（v7 §5.2.2，选定方案 S1）。
// 在 resultSendMu 内串行完成“分配序号 + 记日志 + 写 socket”，保证序号顺序
// = 日志顺序 = 实际发送顺序。写失败不回滚日志行（日志记录“产生”而非“送达”，
// 失败的行由重放恢复）。冻结后到达的发送由守卫拦截（v7 §5.9.4）。
// resultLog == nil 时（非任务上下文，如代次重置后的孤儿 worker）退化为
// 普通 sendWSMessage，不分配序号、不记日志。
// 五项修复问题 2：ctx 携带调用方在 worker 启动时绑定的任务日志（见
// withTaskLog/taskLogFromCtx）。发送前验证绑定身份与当前日志身份一致
// （(taskID, epoch) 逐字段比较，不是只比较 resultLog 指针）：
//   - 一致 → 写入当前日志（即绑定任务的日志）并发送；
//   - 不一致（旧任务 worker 在新任务启动后发送）→ 整条发送丢弃：不记日志、
//     不发送、不分配序号，记 send_stale_task 调试日志。旧任务日志与新任务
//     日志由此保持隔离，旧行既不会获得新任务序号，也不会被新任务重放。
//   - ctx 未绑定（直接调用）→ 沿用当前日志（旧行为），供测试与非任务路径使用。
//
// 身份校验与“分配 + 追加 + 发送”同在 resultSendMu 内；beginTask 替换日志
// 前先取 resultSendMu（见 beginTask），故校验与替换之间不存在 TOCTOU：
// 要么发送在替换前完整完成（合法的旧任务行），要么替换先完成、发送被丢弃。
func (s *appSession) sendTaskResult(ctx context.Context, msgType string, payload interface{}) {
	s.resultSendMu.Lock()
	defer s.resultSendMu.Unlock()

	// 原子读取当前日志指针（atomic.Pointer，无锁，不参与锁顺序）。
	log := s.resultLog.Load()
	if bound := taskLogFromCtx(ctx); bound != nil {
		if log == nil || log.taskID != bound.taskID || log.epoch != bound.epoch {
			recordDebugNotice("send_stale_task", msgType)
			return
		}
	}
	if log == nil {
		s.sendWSMessage(msgType, payload)
		return
	}

	// 1. 分配序号并记日志（resultLog.mu 只覆盖“分配 + 追加”一小段，绝不覆盖发送）。
	log.mu.Lock()
	if log.frozen {
		// 冻结守卫（v7 §5.9.4）：不分配序号，不记日志，不发送，记调试日志后返回。
		log.mu.Unlock()
		recordDebugNotice("send_after_freeze", msgType)
		return
	}
	// 分配序号（超限后仍需分配以保持线上序号连续，新行仍发送，序号不能回退）。
	seq := log.nextSeq
	log.nextSeq++
	sequenced := withSeq(payload, seq)
	data := mustJSON(sequenced)
	// 五项修复问题 5：8 MiB 是严格上限。追加前计算本行实际字节数；只有
	// 追加后仍不超过上限才追加（bytes+len(data) <= maxResultLogBytes）。
	// 行数上限语义不变：行数已达 20,000 时不再追加（第 20,001 行起只发送）。
	// 超限时置 truncated，停止向日志追加新行；新行仍正常发送，只不再记入
	// 日志。重放请求遇 truncated 回 log-truncated。
	if !log.truncated && (len(log.rows) >= maxResultLogRows || log.bytes+int64(len(data)) > maxResultLogBytes) {
		log.truncated = true
	}
	if !log.truncated {
		log.rows = append(log.rows, loggedRow{Seq: seq, Type: msgType, Data: data})
		log.bytes += int64(len(data))
	}
	log.mu.Unlock()

	// 测试门（v7 §9.4.1）：序号分配 + 日志追加之后、写 socket 之前。
	// 已持有 resultSendMu；entered 只关闭一次（once），随后阻塞于 release。
	if g := testWriteDelayGate; g != nil {
		g.once.Do(func() { close(g.entered) })
		<-g.release
	}

	// 2. 写 socket（仍在 resultSendMu 内；sendWSMessage 内部取 taskMutex /
	// wsMutex / wsWriteMu，见计划 §4.1）。
	s.sendWSMessage(msgType, withSeq(payload, seq))
}

// 五项修复问题 3：向绑定的请求连接发送一条重放消息（行或 replay_complete）。
// 生产路径（emit == nil）经 sendWSMessageToConn：归属校验（s.ws == conn 且
// 未关闭）与写入在同一 wsMutex 临界区内完成，连接已换绑/关闭时拒绝发送并
// 返回 false，调用方据此安全中止重放——旧请求绝不会借助 session 当前连接
// 向新连接投递旧请求的结果或完成消息。测试 emit 路径无真实 socket，直接
// 交付给 emit 捕获并返回 true（归属概念在此路径不适用）。
func (s *appSession) sendReplayMsg(conn *websocket.Conn, msgType string, data interface{}) bool {
	if s.emit != nil {
		// emit 路径无真实 socket，但仍尊重 wsClosed（测试用它模拟连接已死；
		// 生产 sendWSMessageToConn 同样在 wsClosed 时拒绝发送）。
		s.wsMutex.Lock()
		closed := s.wsClosed
		s.wsMutex.Unlock()
		if closed {
			return false
		}
		s.emit(msgType, data)
		return true
	}
	return s.sendWSMessageToConn(conn, msgType, data)
}

// 在途重放任务切换归属（本轮修复）：重放开始时绑定的任务身份
// （boundTaskID/boundEpoch）必须在每次实际发送时复核。复核与发送在同一
// resultSendMu 临界区内完成：beginTask 替换日志前先取 resultSendMu（见
// beginTask），故“复核通过 → 发送”与“替换日志”之间不存在 TOCTOU——要么
// 发送在替换前完整完成（当时仍是当前任务，合法），要么替换先完成、发送
// 被拒绝。只在发送前做一次无锁 resultLog.Load() 是不够的：Load 与写 socket
// 之间任务可能切换，旧行仍会发出。
// 锁序：本函数取 resultSendMu（最外层）后经 sendReplayMsg 取 wsMutex →
// wsWriteMu，与 sendTaskResult（resultSendMu → taskMutex/wsMutex/wsWriteMu）、
// beginTask（resultSendMu → globalTaskMutex → taskMutex）、endTask 冻结屏障
// （resultSendMu → resultLog.mu）一致，不引入新锁、不改变既有锁序、无锁环。
// 调用方（replay_results handler）持有任何锁时不调用本函数；测试直接调用
// 时同样不持锁。
// 拒绝时记 replay_aborted_task_switch 调试日志并返回 false，调用方中止
// 本次重放：不再发送任何结果行，也不发送 replay_complete。
func (s *appSession) sendReplayChecked(conn *websocket.Conn, boundTaskID, boundEpoch, msgType string, data interface{}) bool {
	s.resultSendMu.Lock()
	defer s.resultSendMu.Unlock()
	cur := s.resultLog.Load()
	if cur == nil || cur.taskID != boundTaskID || cur.epoch != boundEpoch {
		recordDebugNotice("replay_aborted_task_switch", msgType)
		return false
	}
	return s.sendReplayMsg(conn, msgType, data)
}

// S6: replayResults 处理一次 replay_results 重放请求（v7 §5.4.2）。
// s 是后台任务会话（由 currentBackgroundTaskSession 查得）；conn 是请求连接。
// 返回 (completed, reason)：completed=true 表示全部边界内行已写出且已发送
// replay_complete；completed=false 且 reason 非空表示应回 replay_rejected；
// completed=false 且 reason 为空表示某行写失败（连接已死），不发 complete
// 也不发 rejected——客户端将因断线或超时重请求。
// 不保存任何请求级记录（选 B，v7 §5.4.5）：requestId 仅用于关联与日志。
func (s *appSession) replayResults(conn *websocket.Conn, taskID, epoch string, lastSeq int64, requestID string) (completed bool, reason string) {
	if s == nil {
		return false, "task-not-found"
	}
	// 原子读取当前日志指针（atomic.Pointer；任务切换时 beginTask 已替换指针，
	// 此处读到的是切换后的新日志，其 taskID/epoch 校验会拒绝旧任务请求，
	// 旧任务数据不会被当作新任务数据发送——问题 2 要求 8）。
	log := s.resultLog.Load()
	if log == nil {
		return false, "task-not-found"
	}
	// 1. 校验：任务 ID、epoch、归属连接（v7 §5.4.2 步骤 1）。
	if s.taskIDSnapshot() != taskID || log.taskID != taskID {
		return false, "task-not-found"
	}
	if log.epoch != epoch || epoch != currentBackendEpoch() {
		return false, "epoch-mismatch"
	}
	s.wsMutex.Lock()
	owned := s.ws == conn
	s.wsMutex.Unlock()
	if !owned {
		return false, "not-owner"
	}
	if lastSeq < 0 {
		lastSeq = 0
	}
	// 本轮修复：绑定重放开始时的任务身份（已验证的日志对象的 taskID/epoch，
	// 与请求参数相等）。后续每行与 replay_complete 发送前都在 resultSendMu
	// 内复核当前日志仍是该任务；任务切换后旧重放失效。
	boundTaskID, boundEpoch := log.taskID, log.epoch

	// 2. 在 resultLog.mu 内：检查截断/过期，复制行，捕获固定边界（v7 §5.4.2 步骤 2）。
	log.mu.Lock()
	if log.truncated {
		log.mu.Unlock()
		return false, "log-truncated"
	}
	if log.frozen && time.Since(log.frozenAt) > resultLogRetention {
		log.mu.Unlock()
		return false, "log-expired"
	}
	replayThroughSeq := log.nextSeq - 1
	var rows []loggedRow
	for _, row := range log.rows {
		if row.Seq > lastSeq && row.Seq <= replayThroughSeq {
			rows = append(rows, row)
		}
	}
	log.mu.Unlock()

	// 3. 按 seq 升序逐行写出（不走 sendWSMessage，避免快照重复计数，
	// 见 v7 §5.2.4/§7.5）。行的 Data 已含 seq（sendTaskResult 记录的是带
	// seq 的载荷），直接发送。
	// 五项修复问题 3：每行都经 sendReplayMsg 绑定原始请求连接发送；归属
	// 校验与写入在同一 wsMutex 临界区内完成。重放中途连接被另一条连接接管
	// （s.ws 已换绑）或关闭时，sendReplayMsg 返回 false，立即安全中止：
	// 不再发送任何结果行，也不发送 replay_complete（客户端重连后重请求）。
	// 本轮修复：每行改经 sendReplayChecked，在 resultSendMu 内复核任务身份
	// 后再发送——同一 session、同一连接上启动新任务导致日志替换后，旧重放
	// 的后续行被拒绝。测试门在每行成功写出且 resultSendMu 已释放后检查，
	// 故测试在门处暂停重放时 beginTask 仍可取锁，不死锁。
	sent := 0
	for _, row := range rows {
		if !s.sendReplayChecked(conn, boundTaskID, boundEpoch, row.Type, row.Data) {
			return false, ""
		}
		sent++
		if g := testReplayPauseGate; g != nil && sent == g.afterRows {
			g.once.Do(func() { close(g.entered) })
			<-g.release
		}
	}

	// 4. 全部行写出后，在 resultLog.mu 内重读 nextSeq-1、finalSeq 与 frozen。
	// 五项修复问题 4：final 不得仅凭 log.frozen == true 就置 true。
	// final 为真仅当任务已冻结且本次重放边界等于任务最终边界
	// （finalSeq == replayThroughSeq），此时必有
	// totalSeq == replayThroughSeq == finalSeq（v7 §5.4.1 不变量）。
	// 若任务在重放期间继续产生新行（如 seq=11）后才冻结，则
	// finalSeq(=11) != replayThroughSeq(=10)，本次重放 final=false，
	// totalSeq(=11) > replayThroughSeq——客户端据此知道还有新行，需重请求。
	// 五项修复问题 3：发送 replay_complete 前同样经 sendReplayMsg 复核
	// 连接归属；已换绑时不发送完成消息并中止。
	// 本轮修复：完成消息改经 sendReplayChecked，同样在 resultSendMu 内复核
	// 任务身份——任务切换后旧重放不发送 replay_complete。totalSeq/final
	// 仍从本次重放绑定的日志对象读取（与 §5.4.1 不变量一致；已切换时本次
	// 重放在此之前已被拒绝，读到的值不会发出）。
	log.mu.Lock()
	totalSeq := log.nextSeq - 1
	final := log.frozen && log.finalSeq == replayThroughSeq
	log.mu.Unlock()
	if !s.sendReplayChecked(conn, boundTaskID, boundEpoch, "replay_complete", map[string]interface{}{
		"taskId":           taskID,
		"backendEpoch":     epoch,
		"requestId":        requestID,
		"replayThroughSeq": replayThroughSeq,
		"lastSeq":          replayThroughSeq,
		"totalSeq":         totalSeq,
		"final":            final,
	}) {
		return false, ""
	}
	return true, ""
}

// S6: sendReplayRejected 向请求连接发送 replay_rejected。
// 请求连接的原始会话保存在 connLoopState[connectionID] 中。follow 重绑定后，
// handler 闭包的 session 变量已指向后台会话（其 ws 是新连接），不能用它向
// 旧连接发送（会错发给新连接）。原始会话的 ws 仍是请求连接，其写锁仍是
// 该连接的写锁，发送经它序列化。连接已退出时 entry 缺失，静默丢弃。
func sendReplayRejected(connectionID string, payload map[string]interface{}) {
	backgroundTaskMutex.Lock()
	le, ok := connLoopState[connectionID]
	backgroundTaskMutex.Unlock()
	if !ok || le.session == nil {
		return
	}
	le.session.sendWSMessageDirect("replay_rejected", payload)
}

func (s *appSession) sendWSMessage(msgType string, data interface{}) {
	updatedBackground := false
	if s.emit == nil {
		s.updateBackgroundSnapshotFromMessage(msgType, data)
		updatedBackground = s.isBackgroundTask()
	}
	if updatedBackground && msgType != "background_task_update" && msgType != "background_task_found" && msgType != "background_task_enabled" {
		// R3: update 携带快照 + epoch + version（见 §10.1/§10.4）。
		snap := s.backgroundSummary()
		s.sendWSMessageDirect("background_task_update", map[string]interface{}{"snapshot": snap, "backendEpoch": snap.BackendEpoch, "taskVersion": snap.TaskVersion})
	}
	s.sendWSMessageDirect(msgType, data)
}

// logWriteTimestamp 是 testWriteTimestampLog 的 nil 安全包装（见 §16.3/T43）。
func logWriteTimestamp(event string) {
	if f := testHookGet(&testWriteTimestampLog); f != nil {
		f(event)
	}
}

func (s *appSession) sendWSMessageDirect(msgType string, data interface{}) {
	if s.emit != nil {
		if msgType == "error" || msgType == "github_upload_error" || msgType == "edgetunnel_upload_error" {
			recordProgramDebugError(msgType, data)
		} else if msgType == "log" || msgType == "github_upload_status" || msgType == "edgetunnel_upload_status" || msgType == "version_info" {
			recordDebugNotice(msgType, data)
		}
		s.emit(msgType, data)
		return
	}
	// R3 §5.4：s.ws == nil 检查移入 wsMutex 内（消除 §0.2(7) 的锁外读竞争）。
	// 选项 A 修订（用户 2026-11-12 决定，替代 §5 写失败 Close 规则）：
	// 写入失败只由 writeWSMessageLocked 置 wsClosed=true，不再 Close；
	// 读循环随后按序读完已缓冲的消息再退出，socket 由 Defer C 关闭——
	// 否则一次写失败会丢弃缓冲里的 run_in_background 等消息（见 T34 根因）。
	s.wsMutex.Lock()
	logWriteTimestamp("lock")
	target := s.ws // 在锁内读取——本次写入的连接对象
	if target == nil || s.wsClosed {
		logWriteTimestamp("unlock")
		s.wsMutex.Unlock()
		return
	}
	s.writeWSMessageLocked(msgType, data)
	logWriteTimestamp("unlock")
	s.wsMutex.Unlock()
	// 写入失败时不 Close：wsClosed 已由 writeWSMessageLocked 置位，
	// 后续发送直接丢弃（见上）。
}

// writeWSMessageLocked 在持有 wsMutex 时写一条消息；调用方须先确认连接仍有效。
// 返回是否实际写入成功（写入失败会把连接标记为已关闭）。
func (s *appSession) writeWSMessageLocked(msgType string, data interface{}) bool {
	if s.wsClosed {
		return false
	}
	msg := map[string]interface{}{
		"type": msgType,
		"data": data,
	}
	if msgType == "error" || msgType == "github_upload_error" {
		recordProgramDebugError(msgType, data)
	} else if msgType == "log" || msgType == "github_upload_status" || msgType == "version_info" {
		recordDebugNotice(msgType, data)
	}
	// 写操作由按连接共享的 wsWriteMu 串行化（见 types.go 的字段注释）：
	// 调用方已持有 s.wsMutex；wsWriteMu 是叶子锁，不反向取 wsMutex。
	// 惰性初始化（同一把 wsMutex 保护）：生产 session 在 handleWebSocket
	// 已初始化；直接构造的测试 session 在此补齐，避免空指针。
	if s.wsWriteMu == nil {
		s.wsWriteMu = new(sync.Mutex)
	}
	s.wsWriteMu.Lock()
	defer s.wsWriteMu.Unlock()
	s.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := s.ws.WriteJSON(msg); err != nil {
		s.wsClosed = true
		recordProgramDebugError("websocket_write", err.Error())
		sendLog("WebSocket 发送失败: " + err.Error())
		return false
	}
	return true
}

// adoptConnWriteMu 在 follow/stop_task 换绑前调用：让新的持有者继承本连接的
// 写锁指针（见 types.go 的 wsWriteMu 注释）。调用方须保证 newOwner != s。
// 两步各取一把 wsMutex（先读旧的、再写新的，永不同时持有），无锁序问题；
// 换绑后旧 session 的探测/sender 与新 session 的写共用同一把锁，串行化。
func (s *appSession) adoptConnWriteMu(newOwner *appSession) {
	s.wsMutex.Lock()
	mu := s.wsWriteMu
	if mu == nil {
		// 换绑前尚未有过写操作（探测还未发送）：新建一把锁并同时赋给
		// 旧 session 与新 owner——换绑后两边必须共用同一把锁；若此处
		// 直接返回，两边随后各懒创建一个锁，同一连接的写将失去串行化
		//（与 round 1 修过的写竞态同类，见 server.go 的 sessionPtr 注释）。
		mu = new(sync.Mutex)
		s.wsWriteMu = mu
	}
	s.wsMutex.Unlock()
	newOwner.wsMutex.Lock()
	newOwner.wsWriteMu = mu
	newOwner.wsMutex.Unlock()
}

// sendWSMessageToConn 仅当会话当前仍绑定 conn 时才发送；连接已关闭、
// 已换成新连接或会话改用 emit 时静默丢弃并返回 false。
// 四项判断（emit、ws 为空、ws 不是 conn、ws 已关闭）与写入都在同一把 wsMutex
// 临界区内完成，避免 attachWebSocket 在校验与写入之间换走连接，
// 也避免 s.emit 在锁外读取时与 CLI 会话的 emit 赋值形成数据竞争。
// 供初始化异步探测使用：旧连接的探测结果绝不发给新连接；
// 也不经过 sendWSMessage 的后台任务快照更新，只发这一条消息本身。
func (s *appSession) sendWSMessageToConn(conn *websocket.Conn, msgType string, data interface{}) bool {
	if conn == nil {
		return false
	}
	s.wsMutex.Lock()
	logWriteTimestamp("lock")
	if s.emit != nil || s.ws == nil || s.ws != conn || s.wsClosed {
		logWriteTimestamp("unlock")
		s.wsMutex.Unlock()
		return false
	}
	ok := s.writeWSMessageLocked(msgType, data)
	logWriteTimestamp("unlock")
	s.wsMutex.Unlock()
	// 选项 A 修订（用户 2026-11-12 决定，替代 §5 写失败 Close 规则）：
	// 写入失败不 Close：writeWSMessageLocked 已在锁内置 wsClosed=true；
	// 读循环随后按序读完已缓冲的消息再退出，socket 由 Defer C 关闭
	// （失败时 conn 参数仍有效，但不再由本函数关闭——见 T34 根因）。
	return ok
}

// wsSnapshot 在 wsMutex 内一次性读取 (s.ws, s.wsClosed)（见 §4.3）。
// 调用方不得缓存返回值跨越一次发送——每次发送前重新读取。
func (s *appSession) wsSnapshot() (conn *websocket.Conn, closed bool) {
	s.wsMutex.Lock()
	defer s.wsMutex.Unlock()
	return s.ws, s.wsClosed
}

// isCurrentConn 在 wsMutex 内判断 conn 是否仍是本 session 的当前有效连接（见 §4.3）。
func (s *appSession) isCurrentConn(conn *websocket.Conn) bool {
	s.wsMutex.Lock()
	defer s.wsMutex.Unlock()
	return s.ws != nil && s.ws == conn && !s.wsClosed
}

func (s *appSession) isBackgroundTask() bool {
	s.taskMutex.Lock()
	defer s.taskMutex.Unlock()
	return s.backgroundTask
}

func (s *appSession) startTask(run taskStarter) {
	s.startTaskNamed("任务", "", nil, run)
}

func (s *appSession) startTaskNamed(label string, mode string, params map[string]interface{}, run taskStarter) {
	ctx, cancel := context.WithCancel(context.Background())
	ident, started := s.beginTask(cancel, label, mode, params)
	if !started {
		cancel()
		s.sendWSMessage("error", "已有任务正在运行，请等待完成后再试")
		return
	}
	// S7 C1 O-1: 任务成功创建后发送 task_started（身份取自 beginTask 在同一
	// 临界区内原子返回的快照 ident，不再分别重读 taskID 与当前 epoch；
	// beginTask 已返回，全部业务锁已释放；worker 启动之前、第一条结果消息
	// 之前发送）。前端据此保存前台任务身份，供断线后认领使用（C3 前端批次）。
	// 不为恢复重新生成 taskId。CLI 会话经 emit 收到本消息时由
	// handleCLIMessage 的 switch 忽略（无对应 case）。
	s.publishTaskStarted(ident)
	// 五项修复问题 2：把本任务的结果日志绑定进 ctx（任务身份）。此后派生的
	// 全部 worker 继承该 ctx，发送时凭 (taskID, epoch) 证明身份；旧任务的
	// 滞留 worker 持旧 ctx，无法写入新任务日志。
	ctx = withTaskLog(ctx, s.resultLog.Load())
	safeGo("task", s, func() {
		defer cancel()
		defer func() {
			s.endTask()
			if ctx.Err() != nil {
				s.sendWSMessage("task_stopped", map[string]interface{}{"taskId": ident.taskID, "backendEpoch": ident.backendEpoch})
				return
			}
			s.sendWSMessage("task_complete", map[string]interface{}{"taskId": ident.taskID, "backendEpoch": ident.backendEpoch})
		}()
		run(ctx, s)
	})
}

func (s *appSession) runTaskSync(run taskStarter) error {
	ctx, cancel := context.WithCancel(context.Background())
	ident, started := s.beginTask(cancel, "", "", nil)
	if !started {
		cancel()
		return context.Canceled
	}
	// S7 C1 O-1: 同 startTaskNamed，任务成功创建后发送 task_started（身份取自
	// beginTask 原子返回的快照 ident；锁已释放、同步执行体启动之前、第一条
	// 结果消息之前）。
	s.publishTaskStarted(ident)
	// 五项修复问题 2：同 startTaskNamed，把本任务日志绑定进 ctx（任务身份）。
	ctx = withTaskLog(ctx, s.resultLog.Load())
	defer cancel()
	defer s.endTask()
	run(ctx, s)
	return nil
}

// publishTaskStarted 发布 task_started（S7 C1 O-1 定点修正）：只发送
// beginTask 在同一临界区内原子返回的身份快照 ident；发送前复核快照仍有效
// （见 taskStartedIdentityValid），失效则抑制发送、不发任何事件。
// 调用时业务锁已释放（beginTask 已返回）；复核在 taskMutex 内一次读 epoch、
// 任务状态与日志指针，不持锁发送、不等待、不阻塞。时序：任务成功创建后、worker 启动前、第一条
// 结果消息前（调用方保证）。两个任务入口（startTaskNamed/runTaskSync）共用
// 本函数，门与 hook 的落点唯一。
func (s *appSession) publishTaskStarted(ident taskStartedIdentity) {
	// 发布决策完成后通知测试（发送或抑制均调用；defer 保证两路都触发）。
	defer func() {
		if hook := testHookGet(&testTaskStartedDoneHook); hook != nil {
			hook()
		}
	}()
	// 测试门（O-1 测试 B）：beginTask 已返回、发送决策之前暂停；门内不持有
	// 任何业务锁，测试可在门内执行 resetBackendGeneration。
	if g := testHookGet(&testTaskStartedGate); g != nil {
		g.pass()
	}
	if !s.taskStartedIdentityValid(ident) {
		perfLogLine("task_started_suppressed", fmt.Sprintf("conn=%s task=%s epoch=%s reason=identity_invalidated", s.connectionID, ident.taskID, ident.backendEpoch))
		return
	}
	if hook := testHookGet(&testTaskStartedPayloadHook); hook != nil {
		hook(ident.taskID, ident.backendEpoch)
	}
	s.sendWSMessage("task_started", map[string]interface{}{"taskId": ident.taskID, "backendEpoch": ident.backendEpoch})
}

// taskStartedIdentityValid 复核身份快照在发布时是否仍有效（S7 C1 O-1 第二轮
// 定点修正）。失效条件（任一成立即抑制）：backendEpoch 已推进
// （resetBackendGeneration 已执行，每代次 epoch 均为新随机值）；当前任务已
// 不是该快照的任务（已切换或已结束）；结果日志指针已被清理。只做发送/抑制
// 决策；发送负载始终取自快照本身，绝不重读拼接——混合身份（旧 taskId +
// 新 backendEpoch）不可能产生。
// 顺序（第二轮修正）：epoch 读取必须在 taskMutex 临界区内，与任务状态、
// 日志指针的读取在同一个临界区内完成。上一轮“先无锁读 epoch，再取锁读状态”
// 存在窗口：reset 的 2a 清理只覆盖 connLoopState 中的 session 与
// backgroundTaskSession；不在该集合中的 session（如 CLI 的 bare session，
// 经 runTaskSync 跑任务）reset 推进 epoch 时不会清理它——发布方先读到旧
// epoch，reset 再推进 epoch，发布方随后取锁看到旧任务仍 running、日志仍
// 非 nil，便错误地判定旧任务有效。上一轮注释“不存在观测窗口”的结论只对
// 2a 覆盖的 session 成立，本轮予以纠正。检查与发送之间 reset 介入的，事件
// 在决策时有效、身份内部一致，不构成混合身份（边界语义不变）。断线（计时
// 器武装/回调）不置 nil 日志指针、不改 taskID 与 epoch，故不影响本检查；
// 断线后的取消与认领仍由计时器/认领协议决定，与本次修正无关。
func (s *appSession) taskStartedIdentityValid(ident taskStartedIdentity) bool {
	s.taskMutex.Lock()
	defer s.taskMutex.Unlock()
	// 测试门（第二轮交错测试）：临界区入口暂停，生产为 nil。门内持有
	// taskMutex；测试不得在门内取该锁。
	if g := testHookGet(&testTaskStartedValidGate); g != nil {
		g.pass()
	}
	if currentBackendEpoch() != ident.backendEpoch {
		return false
	}
	return s.isTaskRunning && s.taskID == ident.taskID && s.resultLog.Load() != nil
}

func (s *appSession) stopTask() {
	s.cancelTask(true)
}

func (s *appSession) cancelTaskSilently() {
	s.cancelTask(false)
}

// enableBackgroundTaskLocked 假设调用方已持有 backgroundTaskMutex（见 §8.6）。
// 前置条件：调用方必须已持有 backgroundTaskMutex；本函数不取该锁。
// 只做指针置位 + 快照更新 + 版本递增；不碰 ledger，不发通知，不记日志。
func (s *appSession) enableBackgroundTaskLocked() bool {
	s.taskMutex.Lock()
	defer s.taskMutex.Unlock()
	if !s.isTaskRunning {
		return false
	}
	// 版本递增（仅不同 taskId 时，见 §6.2）：当前已发布 taskId 取自 backgroundTaskSession。
	// backgroundTaskMutex 已由调用方持有，同一时刻只有一个发布在进行，
	// 因此取另一 session 的 taskMutex 不会形成死锁；同一 session 时直接读字段
	// （sync.Mutex 不可重入，不得调 taskIDSnapshot）。
	var curPublishedTaskID string
	if backgroundTaskSession != nil {
		if backgroundTaskSession == s {
			curPublishedTaskID = s.taskID
		} else {
			curPublishedTaskID = backgroundTaskSession.taskIDSnapshot()
		}
	}
	s.backgroundTask = true
	s.backgroundSnapshot.Running = true
	s.backgroundSnapshot.UpdatedAt = time.Now()
	// 快照的 BackendEpoch 在发布时、与指针置位同一临界区写入（见 §10.4）。
	s.backgroundSnapshot.BackendEpoch = currentBackendEpoch()
	if curPublishedTaskID != s.taskID {
		taskIdentityVersion++
		s.backgroundSnapshot.TaskVersion = taskIdentityVersion
	}
	// 重复发布同一 taskId 时版本不变，快照 TaskVersion 保持原值（见 §6.6）。
	backgroundTaskSession = s
	return true
}

// enableBackgroundTask 是外层加锁入口（现有函数，保留）。
// 调用方必须未持有 backgroundTaskMutex。
func (s *appSession) enableBackgroundTask() bool {
	// 第四阶段 B 批次修正（问题 4）：转入后台的发布必须是原子的。
	// 原实现先在 taskMutex 下置位 backgroundTask=true，解锁后再去注册全局会话，
	// 中间隔着一次 perfLogLine；并发的新连接在此窗口内查询 currentBackgroundTaskSession()
	// 会得到 nil，误发 background_task_missing，前端随之清空数据并结束任务 UI。
	// 现改为先取 backgroundTaskMutex 再取 taskMutex（与 clearCompletedBackgroundTaskSession、
	// currentBackgroundTaskSession 的 backgroundTaskMutex → taskMutex 顺序一致，
	// 不产生锁序反转），在同一临界区内完成置位与全局注册：查询要么看到旧状态
	// （任务尚未转入后台），要么看到已注册的新会话，不存在中间态。
	// 日志仍用持锁复制的局部变量、解锁后打印。
	backgroundTaskMutex.Lock()
	ok := s.enableBackgroundTaskLocked()
	backgroundTaskMutex.Unlock()
	if ok {
		// 日志在锁外打印（快照副本经 backgroundSummary 在 taskMutex 内复制）。
		snap := s.backgroundSummary()
		perfLogLine("background_task_enabled", fmt.Sprintf("conn=%s task=%s label=%s mode=%s", s.connectionID, snap.TaskID, snap.Label, snap.Mode))
	}
	return ok
}

func (s *appSession) shouldCancelOnDisconnect() bool {
	s.taskMutex.Lock()
	defer s.taskMutex.Unlock()
	return s.isTaskRunning && !s.backgroundTask
}

func (s *appSession) cancelTask(withLog bool) {
	s.taskMutex.Lock()
	cancel := s.taskCancel
	// S7: 主动取消解除待取消计时器（§9.1 清理点 5），并同步离开断线状态：
	// 已发出取消信号的任务不得再被认领（认领只接受 disconnectPending；
	// 计时器回调的 V1 复核同样以此为准）。cancelTask 与计时器回调互斥于
	// taskMutex：回调先提交（状态已是 cancelling）则本处不再改状态，
	// endTask 置 cancelled；本处先执行则回调的 V1 失败退出，不重复取消。
	s.stopDisconnectTimerLocked()
	if s.disconnectState == stateRunning || s.disconnectState == stateDisconnectPending {
		s.disconnectState = stateCancelled
	}
	s.taskMutex.Unlock()
	if cancel != nil {
		cancel()
		if withLog {
			s.markBackgroundStopping()
			s.sendWSMessage("log", "已发送强制终止信号，正在清理当前任务...")
		}
	}
}

// ===== S7: 断线武装 helper、计时器回调与看门狗决策（v7.10 §9/§9A/§9B/§10A.2/§11.5）=====

// stopDisconnectTimerLocked 停当前待取消计时器并置字段为 nil（S7 §9.1 清理点）。
// 调用方必须已持有 taskMutex。Timer.Stop() 只阻止尚未开始的回调，正确性
// 不依赖它（见 §9A：状态转换才是决议点）。
func (s *appSession) stopDisconnectTimerLocked() {
	if s.disconnectTimer != nil {
		s.disconnectTimer.Stop()
		s.disconnectTimer = nil
	}
}

// loadWatchdogSnapshot 读当前看门狗快照：测试注入（testWatchdogSnapshotOverride）
// 优先，否则读 watchdogSnapshot（原子读，无锁）。返回的快照一经发布即不可变。
func loadWatchdogSnapshot() *watchdogState {
	if ov := testHookGet(&testWatchdogSnapshotOverride); ov != nil {
		return ov
	}
	return watchdogSnapshot.Load()
}

// evaluateDisconnect 是 v7.10 §11.5.3 的决策函数：输入 now（墙钟 UnixNano）、
// 快照、disconnectAt（墙钟 UnixNano）、window；只做纯算术与一次原子代次读取，
// 不取锁、不改状态、不读单调时钟。返回 (cancel, rearm)：cancel 时 rearm 为 0；
// defer 时 rearm 恒大于 0（可直接用于 time.AfterFunc）。
// nil、代次不匹配或不可信的快照一律 defer（安全方向），重武装取
// minPostResumeOpportunity，不得据此提前取消任务。
func evaluateDisconnect(now int64, snap *watchdogState, disconnectAt int64, window time.Duration) (cancel bool, rearm time.Duration) {
	if snap == nil || snap.generation != atomic.LoadUint64(&backendGeneration) {
		return false, minPostResumeOpportunity
	}
	gap := now - snap.lastTickAt
	trailing := max(int64(0), gap-watchdogTickInterval.Nanoseconds()-watchdogTickTolerance.Nanoseconds())
	liveElapsed := (now - disconnectAt) - snap.suspendedTotal - trailing
	// 挂起分支之前先判窗口——liveElapsed 已扣除全部可疑挂起时间（trailing + suspendedTotal），
	// 若其仍 >= window，说明即使按最宽容的挂起口径宽限期也已真实耗尽；此时再 defer 只会把任务锁进
	// “gap 恒 > 阈值 → 每 10s defer 一次、永不取消”的活锁（T7-31）。只有 liveElapsed < window 时
	// 才按挂起处理，真正挂起仍受保护。
	if liveElapsed >= window.Nanoseconds() {
		return true, 0
	}
	if gap > watchdogSuspendThreshold.Nanoseconds() {
		rearm = time.Duration(window.Nanoseconds() - liveElapsed)
		if rearm < minPostResumeOpportunity {
			rearm = minPostResumeOpportunity
		}
		return false, rearm
	}
	return false, time.Duration(window.Nanoseconds() - liveElapsed)
}

// armDisconnectTimer 是唯一的断线武装入口（v7.10 §10A.2）：Defer A 与读循环
// 错误分支都调它，不存在第二条武装路径。锁序 taskMutex → wsMutex（§14 L16）。
// 1. 取 taskMutex；2. 在持有 taskMutex 时取 wsMutex 检查连接归属
// （s.ws != nil && s.ws == ws），随后释放 wsMutex；3. 继续在 taskMutex 内
// 完成状态核验与武装。不检查 !wsClosed：wsClosed 是发送存活标志，不是归属
// 标志（§10A.2 论证；要求 !wsClosed 会把武装条件颠倒——连接死亡正是武装
// 的触发条件）。后台任务、未运行任务、状态不是 running、代次不匹配、连接已
// 换绑，均不得武装。重复调用幂等拒绝，不重置窗口、不调 Timer.Reset。
func (s *appSession) armDisconnectTimer(ws *websocket.Conn) bool {
	s.taskMutex.Lock()
	defer s.taskMutex.Unlock()
	// 归属检查与状态转换在同一 taskMutex 持有内完成：持有 taskMutex 期间，
	// 认领路径无法进入（它同样先取 taskMutex），检查与转换之间不存在交错。
	s.wsMutex.Lock()
	owned := s.ws != nil && s.ws == ws
	s.wsMutex.Unlock()
	if !owned {
		return false // 已换绑到新连接：不武装
	}
	// 测试门（T7-16）：已通过归属检查、持有 taskMutex、未转换状态时暂停。
	// 注意：门暂停期间测试不得在 taskMutex 内读状态（会阻塞至门释放）。
	if g := testHookGet(&testDisconnectArmGate); g != nil {
		g.pass()
	}
	if s.backgroundTask || !s.isTaskRunning {
		return false // 后台任务 / 未运行
	}
	if s.disconnectState != stateRunning {
		return false // 已待定或已终态：幂等拒绝，不重复武装
	}
	if s.taskGeneration != atomic.LoadUint64(&backendGeneration) {
		return false // 代次已切换：孤儿任务不武装
	}
	s.disconnectState = stateDisconnectPending
	armedTaskID, armedEpoch, armedGen := s.taskID, currentBackendEpoch(), s.taskGeneration
	s.disconnectArmedTaskID, s.disconnectArmedEpoch, s.disconnectArmedGen = armedTaskID, armedEpoch, armedGen
	s.disconnectAt = time.Now().UnixNano()
	armedAt := s.disconnectAt
	// 计时器用自引用闭包创建并存入字段（§9A.2）：回调据此执行 V6 计时器身份校验。
	// 闭包捕获本次武装的不可变身份（taskID/epoch/generation/disconnectAt），
	// 回调在锁外计算（阶段 1）时读捕获值，不读 taskMutex 保护字段（§9B.1）。
	// 发布屏障（Batch 1 修正）：timer 指针不能由闭包直接读局部变量——赋值
	// 发生在闭包创建之后，-race 会判定为数据竞态；仅用 atomic.Pointer 传递
	// 仍缺发布完成同步（Store 可能晚于回调的 Load，回调读到 nil 而误退，
	// 任务滞留 disconnectPending 且无后续重武装）。见 publishDisconnectTimerLocked。
	s.publishDisconnectTimerLocked(time.Duration(disconnectGracePeriod.Load()), armedTaskID, armedEpoch, armedGen, armedAt)
	// S7 C1: 武装计数 hook（T7-16/T7-17）：成功武装后调用，测试用它断言
	// “恰好武装一次”（Defer A 的重复武装是无副作用的 no-op，不重复计数）。
	if hook := testHookGet(&testArmCountHook); hook != nil {
		hook()
	}
	return true
}

// publishDisconnectTimerLocked 创建一个带发布完成屏障的断线计时器并完成发布。
// 调用方必须持有 taskMutex；发布全程（创建 → Store → 字段赋值 → close(ready)）
// 在该临界区内完成，不得在锁外更新 s.disconnectTimer。
//
// 每实例独立协议（武装与每次重武装各调一次，永不复用）：
//   - self 与 ready 由本次 timer 独占创建；
//   - 回调的第一项生产同步操作是等待 ready（生产环境 hook 为 nil 时；测试用的
//     started hook 只是非阻塞启动信号，不读指针、不参与发布）；回调不得在等待
//     ready 之前调用 self.Load()、进入 V6 或执行回调主体；
//   - 发布线程按 self.Store → s.disconnectTimer = timer → close(ready) 的顺序
//     执行；close(ready) 是发布屏障：channel 关闭的 happens-before 保证回调在
//     ready 关闭后能观察到此前的 Store 与字段赋值；
//   - 回调通过屏障后 Load 到的指针恒为本次 timer，与 s.disconnectTimer 同一指针，
//     V6 据此做指针身份比较（不改成只比较 taskID、generation 或状态）。
//
// nil 防御：ready 已关闭后 Load 理论上必非 nil；若仍为 nil，说明内部不变量被
// 破坏，回调记一条 perf 日志后安全退出——nil 不是有效 timer，不得进入 V6，
// 更不得据此取消任务。nil 防御不替代发布屏障。
func (s *appSession) publishDisconnectTimerLocked(delay time.Duration, armedTaskID, armedEpoch string, armedGen uint64, armedDisconnectAt int64) {
	self := new(atomic.Pointer[time.Timer])
	ready := make(chan struct{})
	t := time.AfterFunc(delay, func() {
		if hook := testHookGet(&testTimerCallbackStartedHook); hook != nil {
			hook()
		}
		<-ready
		myTimer := self.Load()
		if myTimer == nil {
			perfLogLine("s7_timer_publish_invariant", fmt.Sprintf("task=%s loaded nil timer after ready closed; callback exiting without cancel", armedTaskID))
			return
		}
		if hook := testHookGet(&testTimerCallbackLoadedHook); hook != nil {
			hook(myTimer)
		}
		s.onDisconnectTimer(myTimer, armedTaskID, armedEpoch, armedGen, armedDisconnectAt)
	})
	// 测试门（T7-B1-R1/R2）：AfterFunc 已返回、Store 尚未发生时暂停发布线程。
	if g := testHookGet(&testTimerPublishGate); g != nil {
		g.pass()
	}
	self.Store(t)
	s.disconnectTimer = t
	close(ready)
}

// onDisconnectTimer 是断线计时器回调（v7.10 §9A/§9B 三阶段协议）：
// 阶段 1（锁外计算）→ 阶段 2（taskMutex 内身份复核 + 新鲜快照重算）→
// 阶段 3（同一持有内提交）。回调只取 taskMutex 一把锁；cancel 在锁外执行。
func (s *appSession) onDisconnectTimer(myTimer *time.Timer, armedTaskID, armedEpoch string, armedGen uint64, armedDisconnectAt int64) {
	// 测试门（T7-19）：回调入口、取 taskMutex 之前暂停。
	if g := testHookGet(&testTimerCallbackEntryGate); g != nil {
		g.pass()
	}
	// 阶段 1 — 计算（锁外）：加载快照、取当前墙钟，执行 evaluateDisconnect
	// 得出提议决策。提议不具约束力：阶段 1 与阶段 2 之间可发生认领/取消/
	// 任务切换/代次重置，提议随之过期；阶段 2 的重算为准（此处结果有意丢弃）。
	_, _ = evaluateDisconnect(time.Now().UnixNano(), loadWatchdogSnapshot(), armedDisconnectAt, time.Duration(disconnectGracePeriod.Load()))
	// 测试门（T7-27/T7-28）：阶段 1 计算完成、阶段 2 复核之前暂停。
	if g := testHookGet(&testTimerCallbackPhaseGate); g != nil {
		g.pass()
	}
	// 阶段 2 — 身份复核（taskMutex 内）：六项 V1–V6，任何一项不成立即退出
	//（不取消、不重武装）。核验通过后在同一持有内用新鲜快照重新评估决策。
	s.taskMutex.Lock()
	if s.disconnectState != stateDisconnectPending || // V1：认领已先获胜或任务已终态
		!s.isTaskRunning || // V2：任务已结束
		s.taskID != armedTaskID || // V3：任务已切换
		currentBackendEpoch() != armedEpoch || // V4：epoch 已切换
		s.taskGeneration != armedGen || s.taskGeneration != atomic.LoadUint64(&backendGeneration) || // V5：任务代次已切换
		s.disconnectTimer != myTimer { // V6：本次触发的计时器已不是当前计时器
		s.taskMutex.Unlock()
		return
	}
	decisionCancel, rearm := evaluateDisconnect(time.Now().UnixNano(), loadWatchdogSnapshot(), armedDisconnectAt, time.Duration(disconnectGracePeriod.Load()))
	// 阶段 3 — 提交（同一 taskMutex 持有内，阶段 2 与阶段 3 之间无间隙）。
	if decisionCancel {
		s.disconnectState = stateCancelling // ★ 原子转换，决议点
		cancelFn := s.taskCancel            // 在锁内复制
		s.taskMutex.Unlock()
		// 测试门（T7-18/T7-20）：锁已释放、cancel 未调用时暂停。
		if g := testHookGet(&testTimerCallbackGate); g != nil {
			g.pass()
		}
		if cancelFn != nil {
			cancelFn() // ★ 释放锁后才调用
		}
		return
	}
	// defer：重武装并替换字段，状态保持 disconnectPending，认领仍可随时成功。
	// 新计时器到期后重新执行三阶段。防御：rearm 恒大于 0（evaluateDisconnect
	// 的 defer 分支保证）；若因未来改动返回非正值，取最小重连机会，避免
	// AfterFunc(<=0) 立即触发造成的空转。
	if rearm <= 0 {
		rearm = minPostResumeOpportunity
	}
	// 重武装使用与武装点相同的每实例发布协议（全新的 self 与 ready，见
	// publishDisconnectTimerLocked）；禁止复用首次武装或上一个 timer 的
	// ready/self，禁止在发布屏障打开前进入 V6，禁止先关 ready 再赋值字段。
	s.publishDisconnectTimerLocked(rearm, armedTaskID, armedEpoch, armedGen, armedDisconnectAt)
	s.taskMutex.Unlock()
	if hook := testHookGet(&testTimerRearmHook); hook != nil {
		hook(rearm)
	}
}

// ===== S7 C1: 前台任务认领查找（v7.10 §10.3.2）=====

// claimRequest 是认领请求的解析结果（resume_foreground_task 的 data）。
type claimRequest struct {
	TaskID       string
	BackendEpoch string
	PageID       string
}

// 认领核验的结论动作（claimVerdict.action 的取值）。
const (
	// claimActionClaim：可认领；sess 非 nil 且其 taskMutex 保持持有返回，
	// 调用方在同一持有内完成状态转换（线性化点）。
	claimActionClaim = "claim"
	// claimActionIdempotent：任务已是 running 且 s.ws == 请求连接（C-1 自认领
	// 幂等）；sess 非 nil，其 taskMutex 已释放，调用方直接回 resume_accepted。
	claimActionIdempotent = "idempotent"
	// claimActionReject：不可认领；reason 为拒绝理由，sess == nil。
	claimActionReject = "reject"
	// claimActionNotFound：预检未找到（请求者自己的 session 不持有该 taskId）；
	// 仅 verifyOwnSessionClaim 返回，调用方继续枚举。
	claimActionNotFound = "not-found"
)

// claimVerdict 是认领核验的结论。
type claimVerdict struct {
	action        string
	sess          *appSession
	reason        string
	matchedTaskID bool // 是否有候选的 taskId 与请求匹配（用于拒绝理由的优先级）
}

// enumerateClaimCandidates 是认领查找的步骤 1（v7.10 §10.3.2）：在
// backgroundTaskMutex 持有内迭代 connLoopState，按 pageId + generation 初筛，
// 排除请求者自己的连接。不按 state 过滤（live 与 exited 都是候选；state 只
// 决定拒绝理由）。调用方必须持有 backgroundTaskMutex；返回的 session 列表
// 不取 taskMutex。C2 的 findReplayCandidate 复用本步骤（谓词不同，见 §12.2）。
func enumerateClaimCandidates(pageID string, reqConn *websocket.Conn) []*appSession {
	curGen := atomic.LoadUint64(&backendGeneration)
	var candidates []*appSession
	for _, e := range connLoopState {
		if e.pageId != pageID {
			continue
		}
		if e.generation != curGen {
			continue
		}
		if e.ws == reqConn {
			continue
		}
		if e.session == nil {
			continue
		}
		candidates = append(candidates, e.session)
	}
	return candidates
}

// verifyClaimLocked 是认领核验的步骤 2（v7.10 §10.3.2）：在候选 session 的
// taskMutex 持有内核验身份与状态。调用方必须同时持有 backgroundTaskMutex
// （供原始 socket 对照读 connLoopState）与 sess.taskMutex；本函数不释放
// taskMutex——调用方在 action == claim 时保持持有返回，其余情况自行解锁。
// 身份核验顺序（先身份、后状态，不向错误页面的认领者泄露任务状态）：
// taskId → epoch/generation → taskPageId → 日志交叉核验 → 状态。
// 身份部分由 verifyIdentity 实现（S7 C2 抽取；行为与原内联代码一字不差）。
// 状态结论：
//   - disconnectPending → claim（taskMutex 保持持有返回）
//   - running：s.ws == 请求连接 → idempotent（C-1）；s.ws == 原始 socket
//     （connLoopState[s.connectionID].ws，任务从未被认领）→ reject not-pending
//     （情况 C：未进入等待/半开/健康，走 C4 有界等待）；否则 → reject
//     already-claimed（任务已被另一连接认领，旧连接立即停止重试）。
//   - cancelling/cancelled → reject already-cancelled
//   - completed → reject task-not-found（终态，不复活）
//
// 日志交叉核验失败（log == nil 或身份不符）→ reject task-not-found（日志是
// 任务身份的第二证据；截断只影响重放行数，不影响本核验——截断后 log 仍非 nil）。
// Grace window 不另设显式时间检查：窗口的到期由计时器回调的状态转换实现
// （disconnectPending → cancelling）；状态本身即窗口（§9.2）。
// verifyIdentity 是 S7 C2 从 verifyClaimLocked 抽取的共享身份核验
// （v7.10 §10.3.2 步骤 2 的身份部分；C2 报告 §8.2 A-2a）：
// taskId → epoch/generation → taskPageId → 日志交叉核验。
// 调用方必须已持有 sess.taskMutex；本函数内不取、不放任何锁。
// 返回 (matchedTaskID, reason)：reason == "" 表示身份核验通过；
// reason != "" 时 matchedTaskID 表示是否有候选的 taskId 与请求匹配
// （用于拒绝理由的优先级，与 C1 的 claimVerdict.matchedTaskID 同义）。
// 各分支的理由与 verifyClaimLocked 的原行为一字不差（抽取不得改理由）：
// taskId 不符 → task-not-found；epoch/generation 不符 → epoch-mismatch；
// taskPageId 不符 → not-owner；日志缺失或交叉核验失败 → task-not-found。
func verifyIdentity(sess *appSession, taskID, epoch, pageID string) (matchedTaskID bool, reason string) {
	if sess.taskID != taskID {
		return false, "task-not-found"
	}
	curGen := atomic.LoadUint64(&backendGeneration)
	if sess.taskGeneration != curGen || epoch != currentBackendEpoch() {
		return true, "epoch-mismatch"
	}
	if sess.taskPageId != pageID {
		return true, "not-owner"
	}
	if log := sess.resultLog.Load(); log == nil || log.taskID != sess.taskID || log.epoch != epoch {
		return true, "task-not-found"
	}
	return true, ""
}

func verifyClaimLocked(sess *appSession, req claimRequest, reqConn *websocket.Conn) claimVerdict {
	if matched, reason := verifyIdentity(sess, req.TaskID, req.BackendEpoch, req.PageID); reason != "" {
		return claimVerdict{action: claimActionReject, reason: reason, matchedTaskID: matched}
	}
	switch sess.disconnectState {
	case stateDisconnectPending:
		return claimVerdict{action: claimActionClaim, sess: sess, matchedTaskID: true}
	case stateRunning:
		// s.ws 读需 wsMutex（嵌套在 taskMutex 内，taskMutex → wsMutex）。
		sess.wsMutex.Lock()
		curWS := sess.ws
		sess.wsMutex.Unlock()
		if curWS == reqConn {
			return claimVerdict{action: claimActionIdempotent, sess: sess, matchedTaskID: true}
		}
		// 防御：任务运行但无 socket（从未绑定或已被清除）——无任何连接持有
		// 它，既非“已被认领”也非“可认领”；按 not-pending 拒绝（任务正常
		// 运行中，不进入认领流程）。设计澄清（C1 实施记录 §10.2/§10.5
		// case C）：s.ws == nil → not-pending；T7-37(b) 覆盖。
		if curWS == nil {
			return claimVerdict{action: claimActionReject, reason: "not-pending", matchedTaskID: true}
		}
		// 原始 socket 对照（调用方持有 backgroundTaskMutex）：任务仍在原始
		// 连接上 → 情况 C；已被另一连接认领（s.ws 已换绑）→ already-claimed。
		// 原始条目缺失不应发生（F-1 守卫 + 武装即退出保证运行任务的条目永不
		// 被删）；防御性地按已认领处理（保守：停止重试，任务在某处运行）。
		if le, ok := connLoopState[sess.connectionID]; ok && le.ws == curWS {
			return claimVerdict{action: claimActionReject, reason: "not-pending", matchedTaskID: true}
		}
		return claimVerdict{action: claimActionReject, reason: "already-claimed", matchedTaskID: true}
	case stateCancelling, stateCancelled:
		return claimVerdict{action: claimActionReject, reason: "already-cancelled", matchedTaskID: true}
	case stateCompleted:
		return claimVerdict{action: claimActionReject, reason: "task-not-found", matchedTaskID: true}
	default:
		return claimVerdict{action: claimActionReject, reason: "task-not-found", matchedTaskID: true}
	}
}

// verifyOwnSessionClaim 是 C-2 预检：先核验请求者自己的 session。调用方必须
// 持有 backgroundTaskMutex（供原始 socket 对照），不得持有 ownSession.taskMutex
// （本函数内部取锁）。ownSession 的 taskId 与请求不匹配 → 返回 not-found
// （调用方继续枚举）；匹配 → 返回完整核验结论（claim 时 taskMutex 保持持有，
// 其余情况已释放）。taskId 全局唯一，匹配即以此为准——枚举不可能找到同一
// taskId 的另一 session。
func verifyOwnSessionClaim(ownSession *appSession, req claimRequest, reqConn *websocket.Conn) claimVerdict {
	ownSession.taskMutex.Lock()
	if ownSession.taskID != req.TaskID {
		ownSession.taskMutex.Unlock()
		return claimVerdict{action: claimActionNotFound}
	}
	v := verifyClaimLocked(ownSession, req, reqConn)
	if v.action != claimActionClaim {
		ownSession.taskMutex.Unlock()
	}
	return v
}

// findClaimCandidate 是认领查找的完整步骤（v7.10 §10.3.2）：C-2 预检（请求者
// 自己的 session）→ 步骤 1 枚举 → 步骤 2 逐候选核验。调用方必须持有
// backgroundTaskMutex。返回 claimVerdict：claim 时 sess.taskMutex 保持持有
// （调用方在同一持有内完成状态转换——线性化点）；idempotent 时 sess 非 nil
// 且 taskMutex 已释放；reject 时 sess == nil。拒绝理由优先级：与 taskId
// 匹配的候选的理由优先（taskId 全局唯一，匹配即终局）；无任何候选匹配
// taskId → task-not-found。
func findClaimCandidate(req claimRequest, reqConn *websocket.Conn, ownSession *appSession) claimVerdict {
	if v := verifyOwnSessionClaim(ownSession, req, reqConn); v.action != claimActionNotFound {
		return v
	}
	for _, sess := range enumerateClaimCandidates(req.PageID, reqConn) {
		sess.taskMutex.Lock()
		v := verifyClaimLocked(sess, req, reqConn)
		if v.action == claimActionClaim {
			return v
		}
		if v.action == claimActionIdempotent {
			// 枚举路径的自认领：任务早先已被本连接认领（s.ws == 请求连接），
			// 但请求者自己的 session 是新连接的空 session（预检未命中）。
			// 典型场景是第一次 resume_accepted 丢失后的重试。
			sess.taskMutex.Unlock()
			return v
		}
		sess.taskMutex.Unlock()
		if v.matchedTaskID {
			return claimVerdict{action: claimActionReject, reason: v.reason}
		}
	}
	return claimVerdict{action: claimActionReject, reason: "task-not-found"}
}

// ===== S7 C2: 前台任务重放查找（v7.10 §12.2 步骤 2R；C2 报告 §8.2 A-2b）=====

// replayRequest 是重放查找的请求身份（replay_results 的 data 解析后）。
// PageID 不是请求体字段，而是请求连接自己的 loopEntry.pageId（见 C2 报告 §5 F-C2-3）。
type replayRequest struct {
	TaskID       string
	BackendEpoch string
	PageID       string
}

// replayVerdict 是重放查找的结论：sess 非 nil 表示找到可重放候选
// （返回时 taskMutex 已释放，与 findClaimCandidate 的 claim 分支不同——
// 重放不做状态转换，无需保持持有）；sess == nil 时 reason 为拒绝理由。
type replayVerdict struct {
	sess   *appSession
	reason string
}

// findReplayCandidate 是重放查找（v7.10 §12.2 步骤 2R）：步骤 1 枚举
// （复用 enumerateClaimCandidates，按 pageId + generation 初筛）→ 步骤 2
// 逐候选调 verifyIdentity → 状态谓词（C2 报告 §6.2：running 可重放；
// completed 须日志已冻结且在 TTL 内，否则 log-expired；cancelling/cancelled/
// disconnectPending/其他 → 不是重放对象，跳过）。
// 调用方必须持有 backgroundTaskMutex；返回时 taskMutex 已释放。
// 拒绝理由按 §6.1 的映射与优先级返回（matchedTaskID 优先；初筛未命中 →
// task-not-found，不得声称已核验出 taskId 匹配但 pageId 不符）。
// 锁纪律：查找层内部 backgroundTaskMutex → taskMutex；completed 分支的
// frozen/frozenAt 在 taskMutex 释放后取 log.mu 顺序读取、再重取 taskMutex
// 复核身份与状态——永不嵌套持锁，不新增锁序边（endTask 先冻结后转状态，
// 故读到 completed 即意味着冻结已发生；复核防止读冻结字段期间任务被替换）。
func findReplayCandidate(req replayRequest, reqConn *websocket.Conn) replayVerdict {
	for _, sess := range enumerateClaimCandidates(req.PageID, reqConn) {
		sess.taskMutex.Lock()
		matched, reason := verifyIdentity(sess, req.TaskID, req.BackendEpoch, req.PageID)
		if reason != "" {
			sess.taskMutex.Unlock()
			if matched {
				return replayVerdict{reason: reason}
			}
			continue
		}
		state := sess.disconnectState
		sess.taskMutex.Unlock()
		switch state {
		case stateRunning:
			return replayVerdict{sess: sess}
		case stateCompleted:
			// 顺序读取 frozen/frozenAt（taskMutex 已释放；见上锁纪律）。
			log := sess.resultLog.Load()
			if log == nil {
				return replayVerdict{reason: "task-not-found"}
			}
			log.mu.Lock()
			frozen, frozenAt := log.frozen, log.frozenAt
			log.mu.Unlock()
			if !frozen {
				return replayVerdict{reason: "task-not-found"}
			}
			if time.Since(frozenAt) > resultLogRetention {
				return replayVerdict{reason: "log-expired"}
			}
			// 复核：身份 + 状态仍为 completed + 日志指针未变。任一变化
			// 均按最新事实处理（新任务 → taskId 不符，继续找；状态变
			// 为取消 → task-not-found；epoch 变 → epoch-mismatch）。
			sess.taskMutex.Lock()
			matched2, reason2 := verifyIdentity(sess, req.TaskID, req.BackendEpoch, req.PageID)
			stillCompleted := sess.disconnectState == stateCompleted
			sameLog := sess.resultLog.Load() == log
			sess.taskMutex.Unlock()
			if reason2 != "" {
				if matched2 {
					return replayVerdict{reason: reason2}
				}
				continue
			}
			if !stillCompleted || !sameLog {
				return replayVerdict{reason: "task-not-found"}
			}
			return replayVerdict{sess: sess}
		default:
			continue
		}
	}
	return replayVerdict{reason: "task-not-found"}
}

func (s *appSession) markBackgroundStopping() {
	s.taskMutex.Lock()
	defer s.taskMutex.Unlock()
	if !s.backgroundTask {
		return
	}
	s.backgroundSnapshot.Phase = "正在停止"
	s.backgroundSnapshot.Message = "已发送停止信号，正在整理当前可用结果"
	s.backgroundSnapshot.UpdatedAt = time.Now()
}

func (s *appSession) beginTask(cancel context.CancelFunc, label string, mode string, params map[string]interface{}) (taskStartedIdentity, bool) {
	// 日志 defer 先注册、最后执行：各锁的 defer 后注册、先执行，
	// 因此打印时锁已释放；即使中间 panic，锁仍会按原有 defer 顺序释放。
	var logEvent, logDetails string
	defer func() {
		if logEvent != "" {
			perfLogLine(logEvent, logDetails)
		}
	}()
	// 五项修复问题 2：先取 resultSendMu（最外层锁），再取 globalTaskMutex /
	// taskMutex。日志替换与进行中的 sendTaskResult 由此串行化：要么发送在
	// 替换前完整完成（合法的旧任务行），要么替换先完成、旧 worker 发送被
	// 身份校验丢弃，二者之间不存在 TOCTOU。
	// 锁序：resultSendMu → globalTaskMutex → taskMutex，与 endTask 冻结屏障
	// （resultSendMu → resultLog.mu → globalTaskMutex → taskMutex）及
	// sendTaskResult（resultSendMu → taskMutex）一致，无锁环。
	s.resultSendMu.Lock()
	defer s.resultSendMu.Unlock()
	globalTaskMutex.Lock()
	defer globalTaskMutex.Unlock()
	clearCompletedBackgroundTaskSession()
	// S7 C1: taskPageId 原子提交边界（v7.10 §10.3.2 步骤 0）：在同一
	// backgroundTaskMutex 持有内读本连接的 pageId、取 taskMutex、写身份字段。
	// register_page 写 pageId 与本提交互斥，读到旧值、提交时已是新值的第三种
	// 情况不存在。锁序：resultSendMu → globalTaskMutex → backgroundTaskMutex
	// → taskMutex，与 endTask/认领/重置一致，无锁环。调用方须未持有
	// backgroundTaskMutex（生产调用点 startTaskNamed/runTaskSync 均在锁外调用，
	// 见 C1 报告 §4.3 的调用点核对）。
	// 防御默认值：无条目或 pageId 为空时 taskPageId = ""（任务照常运行，但
	// 认领的 pageId 校验对任何非空请求均失败 → not-owner）。
	backgroundTaskMutex.Lock()
	defer backgroundTaskMutex.Unlock()
	pageId := ""
	if le, ok := connLoopState[s.connectionID]; ok {
		pageId = le.pageId
	}
	s.taskMutex.Lock()
	defer s.taskMutex.Unlock()
	// 测试门（T7-30）：pageId 已读、taskMutex 已取、身份字段尚未写入时暂停。
	if g := testHookGet(&testBeginTaskCommitGate); g != nil {
		g.pass()
	}
	if anyTaskRunningLocked() {
		logEvent = "task_start_rejected"
		logDetails = fmt.Sprintf("conn=%s label=%s mode=%s reason=another_task_running", s.connectionID, label, mode)
		return taskStartedIdentity{}, false
	}
	if s.isTaskRunning {
		logEvent = "task_start_rejected"
		logDetails = fmt.Sprintf("conn=%s label=%s mode=%s reason=session_task_running", s.connectionID, label, mode)
		return taskStartedIdentity{}, false
	}
	s.isTaskRunning = true
	s.taskCancel = cancel
	s.backgroundTask = false
	s.taskID = fmt.Sprintf("task-%d", atomic.AddInt64(&taskSeq, 1))
	// R3: 记录任务所属代次（创建后不变）；代次感知计数（见 §2.4）。
	s.taskGeneration = atomic.LoadUint64(&backendGeneration)
	// S7 C1: 任务身份字段与 taskPageId 在同一 taskMutex 持有内写入（步骤 0
	// 的提交点；与 taskID/taskGeneration 同一临界区，认领核验据此读到一致身份）。
	s.taskPageId = pageId
	// S7: 新任务开始时防御性清理旧计时器并重置断线状态（v7.10 §9.1 清理点 2；
	// 正常情况下旧任务的 endTask 已停表，此处只防旧回调残留）。
	s.stopDisconnectTimerLocked()
	s.disconnectState = stateRunning
	s.disconnectAt = 0
	s.disconnectArmedTaskID = ""
	s.disconnectArmedEpoch = ""
	s.disconnectArmedGen = 0
	now := time.Now()
	s.backgroundSnapshot = backgroundTaskSnapshot{TaskID: s.taskID, Label: label, Mode: mode, Phase: "准备中", Message: "任务准备中", Running: true, StartedAt: now, UpdatedAt: now, Params: params}
	// S6: 初始化本任务的结果日志（v7 §5.9.1）。beginTask 在任何 worker 启动
	// 之前调用，日志初始化 happens-before 任何 worker 的第一次 sendTaskResult。
	// 原子 Store 发布新日志（五项修复问题 2 要求 5）。
	// S7 C1 O-1: epoch 在本临界区内只读一次（与 taskID/taskGeneration/
	// taskPageId 同一 taskMutex 持有内），既用于日志身份，也用于返回给
	// 调用方的任务身份快照；快照与日志身份同源，不可能出现旧 taskId 配
	// 新 epoch 的组合。
	epoch := currentBackendEpoch()
	s.resultLog.Store(&taskResultLog{taskID: s.taskID, epoch: epoch, nextSeq: 1})
	runningTasksByGeneration[s.taskGeneration]++
	logEvent = "task_start"
	logDetails = fmt.Sprintf("conn=%s task=%s label=%s mode=%s", s.connectionID, s.taskID, label, mode)
	return taskStartedIdentity{taskID: s.taskID, backendEpoch: epoch}, true
}

func (s *appSession) endTask() {
	// 与 beginTask 相同：日志 defer 先注册、最后执行，打印时两把锁已释放。
	var logDetails string
	defer func() {
		perfLogLine("task_end", logDetails)
	}()
	// S6: 测试 hook（v7 §9.4.2）：在 endTask 顶部、取任何锁之前触发；
	// 只关闭测试的通道，不取锁、非阻塞。生产为 nil，一次 nil 检查。
	// 注意：hook 只证明“endTask 已进入冻结路径”，不证明它正阻塞于 Lock()
	//（见 v7 §9.4.3 与计划 §5 的证明等级说明）。
	if hook := testEndTaskEnteredHook; hook != nil {
		hook()
	}
	// S6: 冻结屏障（v7 §5.9.3）：先取 resultSendMu，再取 resultLog.mu，
	// 置 frozen/finalSeq/frozenAt，释放两把锁后才取 taskMutex 做原有主体。
	// 若先取 taskMutex 再取 resultSendMu，将与 sendTaskResult 的
	// resultSendMu → taskMutex 构成锁环（见计划 §4.3 L1）。
	// 等待上界为一次写操作（≤10 秒写超时）；典型情况亚毫秒级。
	if log := s.resultLog.Load(); log != nil {
		s.resultSendMu.Lock()
		log.mu.Lock()
		log.frozen = true
		log.finalSeq = log.nextSeq - 1
		log.frozenAt = time.Now()
		log.mu.Unlock()
		s.resultSendMu.Unlock()
	}
	globalTaskMutex.Lock()
	defer globalTaskMutex.Unlock()
	s.taskMutex.Lock()
	defer s.taskMutex.Unlock()
	// 持锁复制到局部变量，日志字符串在锁内组装、锁外打印，不延长锁持有时间。
	connID, taskID, label, mode, wasRunning := s.connectionID, s.taskID, s.backgroundSnapshot.Label, s.backgroundSnapshot.Mode, s.isTaskRunning
	logDetails = fmt.Sprintf("conn=%s task=%s label=%s mode=%s was_running=%v", connID, taskID, label, mode, wasRunning)
	if s.isTaskRunning {
		// 按任务自身的代次递减（见 §2.4）；计数归零且为旧代次时删除条目。
		gen := s.taskGeneration
		runningTasksByGeneration[gen]--
		if runningTasksByGeneration[gen] == 0 && gen < atomic.LoadUint64(&backendGeneration) {
			delete(runningTasksByGeneration, gen)
		}
		// S7: 停计时器并置终态（v7.10 §9.1 清理点 1），与槽释放在同一
		// taskMutex 持有内。计时器回调与 endTask 互斥于 taskMutex：回调先
		// 提交 → 本处停掉已触发的计时器（无害）；本处先执行 → 回调的
		// V1/V2 失败退出。终态判定：cancelling（计时器已到期取消）→
		// cancelled；running/disconnectPending 自然结束 → completed；
		// cancelTask/reset 已同步置为 cancelled 的保留原值。
		s.stopDisconnectTimerLocked()
		switch s.disconnectState {
		case stateCancelling:
			s.disconnectState = stateCancelled
		case stateRunning, stateDisconnectPending:
			s.disconnectState = stateCompleted
		}
	}
	s.isTaskRunning = false
	s.taskCancel = nil
	s.backgroundSnapshot.Running = false
	s.backgroundSnapshot.UpdatedAt = time.Now()
}

func clearCompletedBackgroundTaskSession() {
	backgroundTaskMutex.Lock()
	defer backgroundTaskMutex.Unlock()
	if backgroundTaskSession == nil {
		return
	}
	// §2.8 第 5 项：待清除 session 的代次必须等于当前代次，否则不清指针
	// （旧代次的 session 不得清除新代次的指针）。
	if backgroundTaskSession.backendGeneration != atomic.LoadUint64(&backendGeneration) {
		return
	}
	backgroundTaskSession.taskMutex.Lock()
	defer backgroundTaskSession.taskMutex.Unlock()
	if backgroundTaskSession.isTaskRunning {
		return
	}
	oldTaskID := backgroundTaskSession.taskID
	backgroundTaskSession.backgroundTask = false
	// S6: 同步清理旧会话的结果日志（v7 §5.9.6）。在函数已持有的 taskMutex
	// 保护下执行；新会话的日志由 beginTask 初始化。原子 Store（问题 2 要求 5）。
	backgroundTaskSession.resultLog.Store(nil)
	backgroundTaskSession = nil
	// §6.6：指针被清除时全局版本 +1；旧 taskId 的 published 条目 → superseded
	// （与指针清除同一临界区）。
	taskIdentityVersion++
	for pid, e := range publishLedger {
		if e.taskId == oldTaskID && e.state == ledgerStatePublished {
			e.state = ledgerStateSuperseded
			e.terminalAt = time.Now()
			publishLedger[pid] = e
		}
	}
}

func currentBackgroundTaskSession() *appSession {
	backgroundTaskMutex.Lock()
	defer backgroundTaskMutex.Unlock()
	if backgroundTaskSession == nil {
		return nil
	}
	backgroundTaskSession.taskMutex.Lock()
	defer backgroundTaskSession.taskMutex.Unlock()
	if !backgroundTaskSession.backgroundTask {
		return nil
	}
	return backgroundTaskSession
}

// currentBackgroundTaskSnapshotLocked 在同一个 backgroundTaskMutex 持有内
// 读取后台指针 + 快照 + epoch + 版本（见 §2.8.1 原子快照规则与 §10.4）。
// 指针为 nil 时 ok=false，同时返回同一持有内捕获的当前 epoch 与全局版本。
// 前置条件：调用方已持有 backgroundTaskMutex。
func currentBackgroundTaskSnapshotLocked() (snap backgroundTaskSnapshot, epoch string, version int64, ok bool) {
	if backgroundTaskSession == nil {
		return backgroundTaskSnapshot{}, currentBackendEpoch(), taskIdentityVersion, false
	}
	sess := backgroundTaskSession
	sess.taskMutex.Lock()
	defer sess.taskMutex.Unlock()
	if !sess.backgroundTask {
		return backgroundTaskSnapshot{}, currentBackendEpoch(), taskIdentityVersion, false
	}
	snap = sess.backgroundSnapshot
	snap.Running = sess.isTaskRunning
	return snap, snap.BackendEpoch, snap.TaskVersion, true
}

func (s *appSession) backgroundSummary() backgroundTaskSnapshot {
	s.taskMutex.Lock()
	defer s.taskMutex.Unlock()
	s.backgroundSnapshot.Running = s.isTaskRunning
	return s.backgroundSnapshot
}

func (s *appSession) attachWebSocket(conn *websocket.Conn) {
	s.wsMutex.Lock()
	defer s.wsMutex.Unlock()
	s.ws = conn
	s.wsClosed = false
}

func (s *appSession) updateBackgroundSnapshotFromMessage(msgType string, data interface{}) {
	s.taskMutex.Lock()
	defer s.taskMutex.Unlock()
	if !s.isTaskRunning && !s.backgroundTask {
		return
	}
	now := time.Now()
	s.backgroundSnapshot.UpdatedAt = now
	s.backgroundSnapshot.Running = s.isTaskRunning
	s.applyBackgroundMessageLocked(msgType, data)
}

func (s *appSession) applyBackgroundMessageLocked(msgType string, data interface{}) {
	switch msgType {
	case "log":
		s.backgroundSnapshot.Message = fmt.Sprint(data)
	case "scan_progress", "test_progress":
		m, ok := data.(map[string]interface{})
		if !ok {
			return
		}
		phase := "扫描中"
		current := asInt(m["current"])
		total := asInt(m["total"])
		if msgType == "test_progress" {
			phase = "详细测试中"
		}
		s.backgroundSnapshot.ScanFailed = max(current-s.backgroundSnapshot.ScanSuccess, 0)
		s.updateProgressSnapshotLocked(phase, current, total, phase)
	case "official_speed_progress":
		m, ok := data.(map[string]interface{})
		if !ok {
			return
		}
		s.backgroundSnapshot.SpeedTotal = asInt(m["limit"])
		s.backgroundSnapshot.SpeedQualified = asInt(m["qualified"])
		s.updateProgressSnapshotLocked("测速中", asInt(m["qualified"]), asInt(m["limit"]), "测速中")
	case "nsb_progress":
		m, ok := data.(map[string]interface{})
		if !ok {
			return
		}
		text := fmt.Sprint(m["text"])
		if text == "" || text == "<nil>" {
			text = "处理中"
		}
		phase := fmt.Sprint(m["phase"])
		current := asInt(m["current"])
		total := asInt(m["total"])

		if phase == "scan" {
			s.backgroundSnapshot.ScanFailed = max(current-s.backgroundSnapshot.ScanSuccess, 0)
		} else if phase == "speed" {
			s.backgroundSnapshot.SpeedQualified = current
		}
		s.updateProgressSnapshotLocked(text, current, total, text)
	case "scan_result":
		s.backgroundSnapshot.ResultCount++
		s.backgroundSnapshot.ScanSuccess++
		s.backgroundSnapshot.ScanTotal++
	case "nsb_scan_result":
		if s.backgroundSnapshot.Phase == "测速中" {
			s.backgroundSnapshot.SpeedCount++
			s.updateBackgroundSpeedStatsLocked(data)
		} else {
			s.backgroundSnapshot.ResultCount++
			s.backgroundSnapshot.ScanSuccess++
			s.backgroundSnapshot.ScanTotal++
		}
	case "speed_test_result":
		s.backgroundSnapshot.SpeedCount++
		s.updateBackgroundSpeedStatsLocked(data)
	case "test_result":
		s.backgroundSnapshot.TestCount++
		s.backgroundSnapshot.ScanSuccess++
		s.backgroundSnapshot.ScanTotal++
	case "scan_complete_wait_dc":
		if list, ok := data.([]DataCenterInfo); ok {
			s.backgroundSnapshot.DCCount = len(list)
			names := make([]string, 0, len(list))
			for _, dc := range list {
				name := dc.DataCenter
				if dc.City != "" {
					name += "(" + dc.City + ")"
				}
				names = append(names, name)
			}
			s.backgroundSnapshot.DCInfo = strings.Join(names, ", ")
		}
		s.backgroundSnapshot.Phase = "扫描完成"
		s.backgroundSnapshot.Message = "扫描完成，等待详细测试或后续阶段"
	case "test_complete":
		s.backgroundSnapshot.Phase = "详细测试完成"
		s.backgroundSnapshot.Message = "详细测试完成"
	case "official_speed_complete", "nsb_speed_complete":
		s.backgroundSnapshot.Phase = "测速完成"
		s.backgroundSnapshot.Message = "测速完成"
		s.finishBackgroundStatsLocked()
	case "nsb_csv_complete":
		s.backgroundSnapshot.Phase = "完成"
		s.backgroundSnapshot.Message = "结果文件已生成"
		s.finishBackgroundStatsLocked()
		if payload, ok := data.(nsbCSVCompletePayload); ok {
			s.nsbMutex.Lock()
			s.nsbCompletePayload = &payload
			s.nsbMutex.Unlock()
		}
	case "task_stopped":
		s.backgroundSnapshot.Phase = "已停止"
		s.backgroundSnapshot.Message = "任务已停止"
		s.backgroundSnapshot.Running = false
	case "task_complete":
		s.backgroundSnapshot.Phase = "完成"
		s.backgroundSnapshot.Message = "任务完成"
		s.backgroundSnapshot.Running = false
	}
}

func (s *appSession) updateBackgroundSpeedStatsLocked(data interface{}) {
	s.backgroundSnapshot.SpeedSuccess++
	s.backgroundSnapshot.SpeedTotal++
	speedText := ""
	switch value := data.(type) {
	case map[string]string:
		speedText = value["speed"]
	case map[string]interface{}:
		speedText = fmt.Sprint(value["speed"])
	case nsbScanMessage:
		speedText = value.Speed
		if value.SpeedQualified {
			s.backgroundSnapshot.SpeedQualified++
			return
		}
	case *nsbScanMessage:
		if value != nil {
			speedText = value.Speed
			if value.SpeedQualified {
				s.backgroundSnapshot.SpeedQualified++
				return
			}
		}
	}
	if speedMB, ok := parseSpeedMBForSort(speedText); ok {
		if speedMB >= s.backgroundSpeedMinLocked() {
			s.backgroundSnapshot.SpeedQualified++
		}
		return
	}
	s.backgroundSnapshot.SpeedSuccess--
	s.backgroundSnapshot.SpeedFailed++
}

func (s *appSession) backgroundSpeedMinLocked() float64 {
	value, ok := s.backgroundSnapshot.Params["speedMin"]
	if !ok {
		return 0
	}
	switch n := value.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case string:
		parsed, ok := parseSpeedMBForSort(n + "MB/s")
		if ok {
			return parsed
		}
	}
	return 0
}

func (s *appSession) finishBackgroundStatsLocked() {
}

func (s *appSession) updateProgressSnapshotLocked(phase string, current int, total int, message string) {
	s.backgroundSnapshot.Phase = phase
	s.backgroundSnapshot.Message = message
	s.backgroundSnapshot.Current = current
	s.backgroundSnapshot.Total = total
	if total > 0 {
		s.backgroundSnapshot.Percent = float64(current) / float64(total) * 100
	}
}

// ===== R3: epoch / 代次初始化（见 §2.3/§2.9）=====

// newBackendEpoch 生成 <unixnano>-<hex16> 格式的 epoch（见 §2.3 步骤 2b）。
func newBackendEpoch() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// 极罕见回退：用纳秒 + 原子计数拼 16 位 hex。
		return fmt.Sprintf("%d-%016x", time.Now().UnixNano(), atomic.AddInt64(&serverPublishSeq, 1))
	}
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), hex.EncodeToString(b[:]))
}

// currentBackendEpoch 是 backendEpoch 唯一的读取来源（见 §2.9）。
// main() 顶部尚未 Store 之前返回 ""（epoch 未知）。
func currentBackendEpoch() string {
	v := backendEpochAtomic.Load()
	if v == nil {
		return ""
	}
	if str, ok := v.(string); ok {
		return str
	}
	return ""
}

// resetBackendGeneration 是后端启动代次初始化流程（见 §2.3）。
// 在 main() 顶部调用（setBackendHealthStatus("starting", "") 之后、net.Listen 之前）。
// 注意：main.go 的调用点属于本次授权范围外的文件，见交付报告的受阻项；
// 本函数本身在四个授权文件内实现，测试直接调用它构造代次切换（见 T38 A2/A4）。
// 无参数：epoch 由步骤 2b 在临界区内生成（§2.3 正文）；§2.3 标题的 newEpoch 参数
// 与正文不一致，以正文为准（见交付报告）。
func resetBackendGeneration() {
	globalTaskMutex.Lock()
	backgroundTaskMutex.Lock()
	// 步骤 2a：收集孤儿任务与全部旧连接（清空之前，同一临界区内）。
	orphans := make(map[*appSession]context.CancelFunc)
	oldConns := make(map[*websocket.Conn]struct{})
	collectOrphan := func(sess *appSession) {
		if sess == nil {
			return
		}
		sess.taskMutex.Lock()
		if sess.isTaskRunning && sess.taskCancel != nil {
			orphans[sess] = sess.taskCancel
			// S7: 孤儿任务一并取消时同步离开断线状态（§9.1 清理点 3 的一部分）：
			// 取消信号在锁外发出，状态已在临界区内同步置 cancelled，此后认领
			// 不可再接受该任务；计时器回调的 V1/V4/V5 同样失败退出。
			if sess.disconnectState == stateRunning || sess.disconnectState == stateDisconnectPending || sess.disconnectState == stateCancelling {
				sess.disconnectState = stateCancelled
			}
		}
		// S7: 代次切换时停待取消计时器（§9.1 清理点 3；§11.5.4：进程代次
		// 切换后旧计时器绝不能再触发）。无论是否收集到孤儿都执行（防御性）。
		sess.stopDisconnectTimerLocked()
		// S6: 代次切换时日志失效，同步清理指针（v7 §5.9.6）。孤儿任务的
		// 滞留 worker 此后调 sendTaskResult 时见 resultLog == nil，走无日志直发。
		// 原子 Store（五项修复问题 2 要求 5）。
		sess.resultLog.Store(nil)
		sess.taskMutex.Unlock()
	}
	for _, e := range connLoopState {
		if e.ws != nil {
			oldConns[e.ws] = struct{}{}
		}
		collectOrphan(e.session)
	}
	collectOrphan(backgroundTaskSession)
	// 步骤 2b：递增代次并发布新 epoch（与清空同一临界区）。
	atomic.AddUint64(&backendGeneration, 1)
	backendEpochAtomic.Store(newBackendEpoch())
	// 测试门（S7 C1 O-1 第二轮交错测试）：新 epoch 已发布、后续步骤（清空
	// 全局状态、关闭旧连接、取消孤儿）尚未执行时暂停；生产为 nil。门内持有
	// globalTaskMutex + backgroundTaskMutex，测试不得在门内取这两把锁。
	if g := testHookGet(&testResetEpochGate); g != nil {
		g.pass()
	}
	// S7: 同一临界区内重置看门狗快照（v7.10 §11.5.4）：新代次、新 lastTickAt、
	// suspendedTotal 清零。代次更新与快照 Store 在同一临界区，阻止旧代次
	// 数据进入新代次（tick 的 CAS 读到新快照后基于新代次计算；回调的 V4/V5
	// 复核同样以新代次为准）。
	watchdogSnapshot.Store(&watchdogState{
		generation:     atomic.LoadUint64(&backendGeneration),
		lastTickAt:     time.Now().UnixNano(),
		suspendedTotal: 0,
	})
	// 步骤 2c：清空代次相关全局状态。
	backgroundTaskSession = nil
	taskIdentityVersion = 0
	publishLedger = make(map[string]ledgerEntry)
	pageSessions = make(map[string]map[*appSession]pageSessionEntry)
	pendingRecoveries = make(map[string]*pendingRecoveryEntry)
	resolvedRecoveries = make(map[string]resolvedEntry)
	connLoopState = make(map[string]loopEntry)
	for gen, n := range runningTasksByGeneration {
		if n == 0 {
			delete(runningTasksByGeneration, gen)
		}
	}
	// 步骤 2d：释放两把锁。
	backgroundTaskMutex.Unlock()
	globalTaskMutex.Unlock()
	// 步骤 3a：锁外关闭旧连接 socket（先关 socket，后取消任务）。
	for c := range oldConns {
		c.Close()
	}
	// 步骤 3b：锁外取消孤儿任务。
	for _, cancel := range orphans {
		cancel()
	}
	perfLogLine("backend_generation_reset", fmt.Sprintf("generation=%d epoch=%s orphans=%d closed_conns=%d", atomic.LoadUint64(&backendGeneration), currentBackendEpoch(), len(orphans), len(oldConns)))
}

// ===== R3: refcount 与规范化（见 §11.5）=====

// normalizeWaitForPublishIds 是 pending 创建入口的稳定去重（见 §11.5.4）。
// 纯函数，不取锁、不读共享状态：去重键为完整 publishId 字符串，
// 保留第一次出现顺序，返回新切片（不修改原始输入）。
func normalizeWaitForPublishIds(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// countPendingWaitersLocked 扫描 pendingRecoveries，返回等待 publishId 的
// live pending 数（成员判定，每 pending 最多计 1 次；见 §11.5.4(ii)）。
// 前置条件：调用方已持有 backgroundTaskMutex。
func countPendingWaitersLocked(publishId string) int {
	n := 0
	for _, p := range pendingRecoveries {
		for _, pid := range p.waitForPublishIds {
			if pid == publishId {
				n++
				break
			}
		}
	}
	return n
}

// pendingWaitsOn 判断一条 pending 是否等待指定的 publishId。
func pendingWaitsOn(p *pendingRecoveryEntry, publishId string) bool {
	for _, pid := range p.waitForPublishIds {
		if pid == publishId {
			return true
		}
	}
	return false
}

// decrementRefcountsLocked 按规范化列表逐意图条件递减（见 §11.5.5 等价引理）。
// 两原语（resolveWaiterLocked / releaseWaiterLocked）共享同一删除+递减实现
// （见 §18 第 52 项）。前置条件：调用方已持有 backgroundTaskMutex。
func decrementRefcountsLocked(pendingEntry *pendingRecoveryEntry) {
	for _, pid := range pendingEntry.waitForPublishIds {
		if e, ok := publishLedger[pid]; ok {
			if e.refcount <= 0 {
				// 按等价引理不可达；若命中说明实现有 bug，记错误日志、不再递减。
				recordProgramDebugError("refcount_underflow", pid)
				continue
			}
			e.refcount--
			publishLedger[pid] = e
		}
	}
}

// evictResolvedIfNeededLocked 在写入 resolved 缓存前按 resolvedAt 最旧优先
// 淘汰一条（上限 1000，见 §11.3）。前置条件：调用方已持有 backgroundTaskMutex。
func evictResolvedIfNeededLocked() {
	if len(resolvedRecoveries) < 1000 {
		return
	}
	var oldestID string
	var oldest time.Time
	first := true
	for id, e := range resolvedRecoveries {
		if first || e.resolvedAt.Before(oldest) {
			oldestID, oldest, first = id, e.resolvedAt, false
		}
	}
	delete(resolvedRecoveries, oldestID)
}

// ===== R3: 发布事务（见 §7.2–§7.5）=====

// createDispatchedLedgerLocked 是新 dispatched ledger 条目的唯一生产实现（见 §7.3.2）。
// 前置条件：调用方已持有 backgroundTaskMutex；publishLedger[publishId] 不存在；
// s、pageId、conn 取自 handler 帧（ownerConn 必须由调用方传入，不得在本函数内读 s.ws）。
func createDispatchedLedgerLocked(s *appSession, publishId, pageId string, conn *websocket.Conn) {
	publishLedger[publishId] = ledgerEntry{state: ledgerStateDispatched, pageId: pageId, ownerSession: s, ownerConn: conn, taskId: s.taskIDSnapshot(), version: 0, backendEpoch: currentBackendEpoch(), generation: s.backendGeneration, createdAt: time.Now(), refcount: countPendingWaitersLocked(publishId)}
}

// publishBackgroundTask 是唯一的发布入口（见 §7.3）。
// 调用方必须未持有 backgroundTaskMutex（本函数自己取锁）。
// conn 是 handler 的闭包连接（即送达 run_in_background 的那个连接），用于登记
// ledger 条目的 ownerConn——在 backgroundTaskMutex 内不得取 wsMutex 读 s.ws，
// 因此 ownerConn 必须由调用方传入，不得在本函数内读取。
func publishBackgroundTask(s *appSession, publishId, pageId string, conn *websocket.Conn) publishResult {
	if gate := testHookGet(&testPublishLockGate); gate != nil {
		<-gate
	}
	backgroundTaskMutex.Lock()
	createdDispatched := false
	committed := false
	var snap backgroundTaskSnapshot
	// 独立 Unlock defer（先注册、后执行）：只解锁，不做业务逻辑。
	// 即使补救 defer 自身 panic，本 defer 仍执行，锁一定释放（见 §7.3）。
	defer backgroundTaskMutex.Unlock()
	// 事务补救 defer（后注册、先执行）：只做 rollback/reconcile，不调用
	// recover()，不负责解锁（解锁只由上方的独立 defer 执行；补救函数内
	// 永不调 Unlock——重复解锁会 fatal，见 §7.4/§7.5）。
	defer func() {
		if gate := testHookGet(&testPublishDeferGate); gate != nil {
			select {
			case gate <- struct{}{}:
			default:
			}
		}
		if !committed && createdDispatched {
			rollbackPublishLocked(s, publishId)
		} else if committed {
			reconcilePublishNotificationsLocked(publishId)
		}
	}()
	// 步骤 0：代次复核（见 §2.8）——取锁后、写之前再次验证代次。
	if s.backendGeneration != atomic.LoadUint64(&backendGeneration) {
		// publishBackgroundTask 不直接置位（它是包级函数，无连接帧访问权）；
		// 只返回 generation-mismatch，由 handler 在分支内置位（见 §7.6）。
		return publishResult{committed: false, outcome: "generation-mismatch"}
	}
	// 步骤 1：dispatched 登记——ledger 条目的唯一创建点（见 §11.1）。
	// 若 publishId 已存在，先校验归属（见 §7.3.1），再按状态幂等处理，不重复创建。
	if e, ok := publishLedger[publishId]; ok {
		// 归属校验：在任何状态分支之前执行。
		if e.pageId != pageId {
			return publishResult{committed: false, outcome: "page-mismatch", backendEpoch: currentBackendEpoch()}
		}
		if e.generation != s.backendGeneration || e.backendEpoch != currentBackendEpoch() {
			return publishResult{committed: false, outcome: "generation-mismatch"}
		}
		switch e.state {
		case ledgerStatePublished:
			// 已提交：返回 ledger 中存入的首次发布快照（不是当前 session 的快照）。
			return publishResult{committed: true, outcome: "already-published", snapshot: e.snapshot, hasSnapshot: true, backendEpoch: e.backendEpoch}
		case ledgerStateFailed:
			return publishResult{committed: false, outcome: "already-failed", backendEpoch: e.backendEpoch}
		case ledgerStateSuperseded:
			return publishResult{committed: false, outcome: "already-superseded", backendEpoch: e.backendEpoch}
		case ledgerStateDispatched:
			return publishResult{committed: false, outcome: "duplicate-in-flight", backendEpoch: e.backendEpoch}
		}
	}
	createDispatchedLedgerLocked(s, publishId, pageId, conn)
	createdDispatched = true
	// 步骤 2：指针置位（Locked 版本，不取锁——见 §8.6）。
	if !s.enableBackgroundTaskLocked() {
		// 无运行任务可发布：不内联写 failed——defer 的 rollbackPublishLocked
		// （createdDispatched==true）会把本次创建的 dispatched 条目标记为 failed
		// 并入队失败通知（单一失败路径，见 §7.4）。
		return publishResult{committed: false, outcome: "failed", backendEpoch: currentBackendEpoch()}
	}
	if testHookGet(&testPublishPanicGate2) != nil {
		panic("test: publish panic before commit")
	}
	// 步骤 3：ledger 置 published——提交点；同时存入原始快照（见 §7.3）。
	snap = s.backgroundSummary()
	e := publishLedger[publishId]
	e.state = ledgerStatePublished
	e.taskId = s.taskIDSnapshot()
	e.version = taskIdentityVersion
	e.snapshot = snap
	publishLedger[publishId] = e
	committed = true
	if testHookGet(&testPublishPanicGate) != nil {
		panic("test: publish panic after commit")
	}
	// 步骤 4：通知入队（全部等待者 + resolvedRecoveries）。
	enqueuePublishNotificationsLocked(publishId)
	return publishResult{committed: true, outcome: "committed", snapshot: snap, hasSnapshot: true, backendEpoch: snap.BackendEpoch}
}

// rollbackPublishLocked 撤销本次调用已做的部分提交（见 §7.4）。
// 前置条件：已持有 backgroundTaskMutex；本次调用在步骤 1 创建了 dispatched 条目
// （调用方 defer 中的 createdDispatched 守卫保证）。
func rollbackPublishLocked(s *appSession, publishId string) {
	// 仅当指针确实指向本次调用的 session 时才清除。
	if backgroundTaskSession == s {
		backgroundTaskSession = nil
	}
	// 仅当本 session 的 backgroundTask 标志是本次调用置位的才清除。
	if backgroundTaskSession == nil { // 上一步刚清除（或原本就为 nil）
		s.taskMutex.Lock()
		s.backgroundTask = false
		s.backgroundSnapshot.Running = s.isTaskRunning
		s.taskMutex.Unlock()
	}
	if testHookGet(&testRollbackPanicGate) != nil {
		panic("test: rollback panic")
	}
	// ledger：若仍为 dispatched（本次调用在步骤 1 创建、尚未提交），记为 failed。
	if e, ok := publishLedger[publishId]; ok && e.state == ledgerStateDispatched {
		e.state = ledgerStateFailed
		e.terminalAt = time.Now()
		publishLedger[publishId] = e
	}
	// 版本永不回退（见 §7.4）。
	enqueuePublishFailedLocked(publishId)
}

// reconcilePublishNotificationsLocked 在 committed == true 时执行（见 §7.5）。
// 遍历等待该 publishId 的全部条目，逐条按 §10.6 复合决议补发通知；
// 正常返回时步骤 4 已全部通知，本函数为 no-op；panic 在步骤 4 中途时补发剩余部分。
// 前置条件：已持有 backgroundTaskMutex。
func reconcilePublishNotificationsLocked(publishId string) {
	for recoveryId, pendingEntry := range pendingRecoveries {
		if !pendingWaitsOn(pendingEntry, publishId) {
			continue
		}
		// gate 检查点 (i)：非目标迭代跳过（见 §7.5.1）。
		if g := testHookGet(&testReconcilePanicGate); g != nil && recoveryId != g.targetRecoveryId {
			continue
		}
		if _, ok := resolvedRecoveries[recoveryId]; ok {
			// 防御纵深：已决议的 pending 已被删除；若命中，删条目、不递减、
			// 不重写入队，并记错误日志（见 §7.5）。
			recordProgramDebugError("reconcile_already_resolved", recoveryId)
			delete(pendingRecoveries, recoveryId)
			continue
		}
		sig, terminal, _ := decideRecoveryLocked(pendingEntry, recoveryId)
		resolved := false
		if terminal {
			resolveWaiterLocked(recoveryId, pendingEntry, sig)
			resolved = true
		}
		// gate 检查点 (ii)：目标等待者迭代的末尾（信号已入队 + 缓存已写入之后）panic。
		if g := testHookGet(&testReconcilePanicGate); g != nil && recoveryId == g.targetRecoveryId && resolved {
			panic("test: reconcile panic")
		}
	}
}

// resolveWaitersForPublishLocked 对每条等待 publishId 的 pending 按 §10.6
// 复合决议重算：整体终态时经 resolveWaiterLocked 决议；仍有意图未终态时
// 保持等待（多意图规则，不存在“过早 missing”，见 §10.6）。
// enqueuePublishNotificationsLocked（步骤 4）与 enqueuePublishFailedLocked
// （回滚）共用本函数——两者触发时的 ledger 状态不同，决议结果自然不同
// （published → found，failed → missing(publish-failed)）。
// 前置条件：调用方已持有 backgroundTaskMutex。
func resolveWaitersForPublishLocked(publishId string) {
	for recoveryId, pendingEntry := range pendingRecoveries {
		if !pendingWaitsOn(pendingEntry, publishId) {
			continue
		}
		sig, terminal, _ := decideRecoveryLocked(pendingEntry, recoveryId)
		if terminal {
			resolveWaiterLocked(recoveryId, pendingEntry, sig)
		}
	}
}

// enqueuePublishNotificationsLocked 是 publishBackgroundTask 步骤 4 的通知入队（见 §7.2）。
// 前置条件：调用方已持有 backgroundTaskMutex。
func enqueuePublishNotificationsLocked(publishId string) {
	resolveWaitersForPublishLocked(publishId)
}

// enqueuePublishFailedLocked 是 rollback 的失败通知入队（见 §7.4）。
// 前置条件：调用方已持有 backgroundTaskMutex。
func enqueuePublishFailedLocked(publishId string) {
	resolveWaitersForPublishLocked(publishId)
}

// ===== R3: 决议 / 释放原语（见 §11.5.6）=====

// resolveWaiterLocked 决议一条 pending（见 §11.5.6）。
// 第 0 步 identity check；第 1 步删 pending；第 2 步逐意图条件递减；
// 第 3 步写 resolved 缓存（信号全文，含 signalSeq）；第 4 步经 sender 队列入队信号。
// 前置条件：调用方已持有 backgroundTaskMutex。
func resolveWaiterLocked(recoveryId string, pendingEntry *pendingRecoveryEntry, signal recoverySignal) {
	if pendingRecoveries[recoveryId] != pendingEntry {
		recordProgramDebugError("pending_identity_mismatch", recoveryId)
		return
	}
	delete(pendingRecoveries, recoveryId)
	decrementRefcountsLocked(pendingEntry)
	var sender *recoverySender
	if pendingEntry.sender != nil {
		signal = pendingEntry.sender.assignSeq(signal)
		sender = pendingEntry.sender
	} else {
		recordProgramDebugError("resolve_waiter_no_sender", recoveryId)
	}
	evictResolvedIfNeededLocked()
	resolvedRecoveries[recoveryId] = resolvedEntry{
		ownerSession: pendingEntry.ownerSession,
		ownerPageId:  pendingEntry.ownerPageId,
		ownerConn:    pendingEntry.ownerConn,
		signal:       signal,
		resolvedAt:   time.Now(),
		epoch:        currentBackendEpoch(),
		generation:   pendingEntry.generation,
	}
	if sender != nil {
		sender.enqueue(signal)
	}
}

// releaseWaiterLocked 释放一条 pending（无终态信号，见 §11.5.6）。
// 第 0 步 identity check；第 1 步删 pending；第 2 步逐意图条件递减；
// 不写缓存、不入队信号。前置条件：调用方已持有 backgroundTaskMutex。
func releaseWaiterLocked(recoveryId string, pendingEntry *pendingRecoveryEntry) {
	if pendingRecoveries[recoveryId] != pendingEntry {
		recordProgramDebugError("pending_identity_mismatch", recoveryId)
		return
	}
	delete(pendingRecoveries, recoveryId)
	decrementRefcountsLocked(pendingEntry)
}

// ===== R3: 恢复查询决策（见 §10.6/§10.6.1/§10.6.2）=====

// pageIdFromPublishId 从 publishId 前缀 pub-<pageId>-<n> 解析 pageId（见 §10.6 A 阶段）。
// 解析失败返回 ""。
func pageIdFromPublishId(publishId string) string {
	if !strings.HasPrefix(publishId, "pub-") {
		return ""
	}
	rest := publishId[4:]
	i := strings.LastIndex(rest, "-")
	if i <= 0 {
		return ""
	}
	suffix := rest[i+1:]
	if suffix == "" {
		return ""
	}
	for _, c := range suffix {
		if c < '0' || c > '9' {
			return ""
		}
	}
	return rest[:i]
}

// pageIdEverExistedLocked 按 §10.6 A 阶段判定 pageId 是否曾经存在：
// pageSessions 含它，或 connLoopState 有它的条目（含 exited，5 分钟保留期内），
// 或 publishLedger 有它的条目，三处任一命中即为曾经存在。
// 前置条件：调用方已持有 backgroundTaskMutex。
func pageIdEverExistedLocked(pageId string) bool {
	if pageId == "" {
		return false
	}
	if sessions, ok := pageSessions[pageId]; ok && len(sessions) > 0 {
		return true
	}
	for _, e := range connLoopState {
		if e.pageId == pageId {
			return true
		}
	}
	for _, e := range publishLedger {
		if e.pageId == pageId {
			return true
		}
	}
	return false
}

// liveWaitSessionsLocked 返回该 pageId 下除请求者连接外活着的连接的 session
// （见 §10.6 B 阶段）。“除 B 外”的 B 是查询连接本身（以 ownerConn 指针识别），
// 而不是查询 session 对象——follow 重绑定后同一连接的 loop-local session
// 可能已是另一 session；按连接排除在全部情形下都正确，
// 按 session 对象排除会在 follow 后把请求者自己的旧 session 误算为等待者
// （导致 publish-lost 应得 pending），或把另一连接的 loop-local 误排除
// （导致 pending 应得 publish-lost）。见交付报告。
// 前置条件：调用方已持有 backgroundTaskMutex。
func liveWaitSessionsLocked(pageId string, requesterConn *websocket.Conn) map[*appSession]struct{} {
	live := make(map[*appSession]struct{})
	for _, e := range connLoopState {
		if e.pageId != pageId || e.state == loopStateExited || e.ws == requesterConn {
			continue
		}
		if e.session != nil {
			live[e.session] = struct{}{}
		}
	}
	return live
}

// computeServerDeadlineLocked 按 §3.2 计算 serverDeadline：
// 慢 handler 路径用 handlerStartedAt+7s；否则用 min(各等待 session 的 lastPongAt+90s)
// （从未 Pong 用 connectedAt+90s；两者皆无——如测试直接创建的 session——用 now+90s）。
// 前置条件：调用方已持有 backgroundTaskMutex。
func computeServerDeadlineLocked(waitSessions map[*appSession]struct{}) time.Time {
	now := time.Now()
	deadline := time.Time{}
	for s := range waitSessions {
		var cand time.Time
		if e, ok := connLoopState[s.connectionID]; ok && e.state == loopStateInHandler && !e.handlerDeadline.IsZero() {
			cand = e.handlerDeadline
		} else {
			base := s.lastPongAtNano.Load()
			if base == 0 {
				base = s.connectedAtNano.Load()
			}
			if base == 0 {
				cand = now.Add(wsReadDeadline)
			} else {
				cand = time.Unix(0, base).Add(wsReadDeadline)
			}
		}
		if deadline.IsZero() || cand.Before(deadline) {
			deadline = cand
		}
	}
	if deadline.IsZero() {
		return now.Add(wsReadDeadline)
	}
	return deadline
}

// decideRecoveryLocked 按 §10.6 两阶段决策 + 复合响应，对一条 pending 的
// 全部等待意图重算结局。调用者必须持有 backgroundTaskMutex。
// 返回终态信号 + terminal=true，或 pending 信号 + terminal=false，
// 以及 pending 时的 serverDeadline（终态时为零值）。
func decideRecoveryLocked(pending *pendingRecoveryEntry, recoveryId string) (recoverySignal, bool, time.Time) {
	type intentOutcome struct {
		publishId string
		outcome   string // published | failed | superseded | publish-lost | pending | intent-unknown
		taskId    string
		version   int64
	}
	outcomes := []intentOutcome{}
	anyPending := false
	waitSessions := map[*appSession]struct{}{}
	epoch := currentBackendEpoch()
	for _, pid := range pending.waitForPublishIds {
		// B 阶段：逐意图判定结局。
		if e, ok := publishLedger[pid]; ok {
			// 跨页意图忽略（见 §10.6 B 阶段）：按“未声明该意图”重新决议。
			if e.pageId != pending.waitForPageId {
				continue
			}
			switch e.state {
			case ledgerStatePublished:
				outcomes = append(outcomes, intentOutcome{pid, "published", e.taskId, e.version})
			case ledgerStateSuperseded:
				outcomes = append(outcomes, intentOutcome{pid, "superseded", e.taskId, e.version})
			case ledgerStateFailed:
				outcomes = append(outcomes, intentOutcome{pid, "failed", e.taskId, e.version})
			case ledgerStateDispatched:
				outcomes = append(outcomes, intentOutcome{pid, "pending", e.taskId, e.version})
				anyPending = true
				if e.ownerSession != nil {
					waitSessions[e.ownerSession] = struct{}{}
				}
			}
			continue
		}
		// ledger 无记录：确定 pageId（前缀解析），A 阶段查三处证据。
		intentPageId := pageIdFromPublishId(pid)
		if intentPageId != pending.waitForPageId {
			continue
		}
		if !pageIdEverExistedLocked(intentPageId) {
			outcomes = append(outcomes, intentOutcome{pid, "intent-unknown", "", 0})
			continue
		}
		// pageId 曾经存在：除查询连接外有活着的连接 → pending，否则 publish-lost。
		live := liveWaitSessionsLocked(intentPageId, pending.ownerConn)
		if len(live) > 0 {
			outcomes = append(outcomes, intentOutcome{pid, "pending", "", 0})
			anyPending = true
			for s := range live {
				waitSessions[s] = struct{}{}
			}
		} else {
			outcomes = append(outcomes, intentOutcome{pid, "publish-lost", "", 0})
		}
	}
	// publishOutcomes 由存储列表派生（每唯一 publishId 一条结局）。
	publishOutcomes := make([]map[string]interface{}, 0, len(outcomes))
	for _, o := range outcomes {
		m := map[string]interface{}{"publishId": o.publishId, "outcome": o.outcome}
		if o.taskId != "" {
			m["taskId"] = o.taskId
		}
		if o.outcome == "published" || o.outcome == "superseded" || o.outcome == "failed" {
			m["version"] = o.version
		}
		publishOutcomes = append(publishOutcomes, m)
	}
	// C 阶段：全局指针与意图结局合并。
	snap, snapEpoch, snapVersion, hasPointer := currentBackgroundTaskSnapshotLocked()
	if hasPointer {
		data := map[string]interface{}{
			"snapshot": snap, "backendEpoch": snapEpoch, "taskVersion": snapVersion,
			"recoveryId": recoveryId, "publishOutcomes": publishOutcomes,
		}
		return recoverySignal{Type: "background_task_found", Data: data, RecoveryID: recoveryId}, true, time.Time{}
	}
	// 指针 nil + 某意图 published 是不可能状态：记错误日志，按 found 处理（见 §10.6）。
	for _, o := range outcomes {
		if o.outcome == "published" {
			recordProgramDebugError("recovery_pointer_ledger_mismatch", o.publishId)
			if e, ok := publishLedger[o.publishId]; ok {
				data := map[string]interface{}{
					"snapshot": e.snapshot, "backendEpoch": e.snapshot.BackendEpoch, "taskVersion": e.snapshot.TaskVersion,
					"recoveryId": recoveryId, "publishOutcomes": publishOutcomes,
				}
				return recoverySignal{Type: "background_task_found", Data: data, RecoveryID: recoveryId}, true, time.Time{}
			}
		}
	}
	if anyPending {
		deadline := computeServerDeadlineLocked(waitSessions)
		data := map[string]interface{}{
			"recoveryId": recoveryId, "serverDeadline": deadline,
			"waitForPublishIds": pending.waitForPublishIds, "waitForPageId": pending.waitForPageId,
			"backendEpoch": epoch, "publishOutcomes": publishOutcomes,
		}
		return recoverySignal{Type: "background_task_pending", Data: data, RecoveryID: recoveryId}, false, deadline
	}
	// 整体 missing：摘要优先级 publish-lost > publish-failed > task-superseded > intent-unknown；
	// 无意图时 reason 为 no-task（见 §10.6 C 阶段）。
	reason := "no-task"
	priority := []struct {
		outcome string
		reason  string
	}{
		{"publish-lost", "publish-lost"},
		{"failed", "publish-failed"},
		{"superseded", "task-superseded"},
		{"intent-unknown", "intent-unknown"},
	}
	for _, p := range priority {
		for _, o := range outcomes {
			if o.outcome == p.outcome {
				reason = p.reason
				break
			}
		}
		if reason != "no-task" {
			break
		}
	}
	data := map[string]interface{}{
		"reason": reason, "recoveryId": recoveryId,
		"backendEpoch": epoch, "taskVersion": taskIdentityVersion,
		"publishOutcomes": publishOutcomes,
	}
	return recoverySignal{Type: "background_task_missing", Data: data, RecoveryID: recoveryId}, true, time.Time{}
}

// resolvePendingLocked 是重查与 sweeper 共用的 pending 终态决议步骤（见 §10.6.1/§12.3）。
// 调用者必须持有 backgroundTaskMutex。仅当全部等待意图已终态时决议；
// 否则不做任何事（调用方按新 pending 处理）。
func resolvePendingLocked(recoveryId string, pending *pendingRecoveryEntry) {
	sig, terminal, _ := decideRecoveryLocked(pending, recoveryId)
	if !terminal {
		return
	}
	resolveWaiterLocked(recoveryId, pending, sig)
}

// createPendingRecoveryLocked 是全新决议判定为 pending 时 pending 条目创建的
// 唯一生产实现（见 §10.6.2）。
// 前置条件（调用者必须保证，函数内不复核）：
//
//	(a) 调用者已持有 backgroundTaskMutex；
//	(b) 本次请求经 A/B/C 阶段全新决议判定为 pending；
//	(c) pendingRecoveries[recoveryId] 不存在；
//	(d) recoveryId、pageId、ownerConn、serverDeadline 取自 handler 帧；
//	(e) s 为本次请求的 session（仅用于取 backendGeneration 快照）。
func createPendingRecoveryLocked(s *appSession, recoveryId, pageId string, rawWaitForPublishIds []string, ownerConn *websocket.Conn, serverDeadline time.Time, attempt int) *pendingRecoveryEntry {
	normalized := normalizeWaitForPublishIds(rawWaitForPublishIds)
	entry := &pendingRecoveryEntry{ownerSession: s, ownerPageId: pageId, ownerConn: ownerConn, waitForPageId: pageId, waitForPublishIds: normalized, createdAt: time.Now(), serverDeadline: serverDeadline, attempt: attempt, epoch: currentBackendEpoch(), generation: s.backendGeneration}
	pendingRecoveries[recoveryId] = entry
	for _, pid := range normalized {
		if e, ok := publishLedger[pid]; ok {
			e.refcount++
			publishLedger[pid] = e
		}
	}
	return entry
}

// ===== R3: sender（见 §4.4/§4.5）=====

// newRecoverySender 创建每连接的 sender（见 §4.5）。由 handleWebSocket 调用。
func newRecoverySender(session *appSession) *recoverySender {
	return &recoverySender{
		session: session,
		notify:  make(chan struct{}, 1),
		done:    make(chan struct{}),
		exited:  make(chan struct{}),
	}
}

// assignSeq 在 sender 队列锁内为信号分配本 sender 单调递增的 signalSeq
// （见 §10.1/T43）。已有 seq 的信号（如缓存命中重发）保留原值。
func (rs *recoverySender) assignSeq(sig recoverySignal) recoverySignal {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if sig.SignalSeq == 0 {
		rs.nextSeq++
		sig.SignalSeq = rs.nextSeq
	}
	if sig.Data == nil {
		sig.Data = make(map[string]interface{})
	}
	sig.Data["signalSeq"] = sig.SignalSeq
	return sig
}

// enqueue 将信号入队并按 §4.5 懒启动 sender。
// 调用者持有 backgroundTaskMutex（锁边 backgroundTaskMutex → recoveryQueueMu，见 §12.1）。
func (rs *recoverySender) enqueue(sig recoverySignal) {
	rs.mu.Lock()
	if sig.SignalSeq == 0 {
		rs.nextSeq++
		sig.SignalSeq = rs.nextSeq
	}
	if sig.Data == nil {
		sig.Data = make(map[string]interface{})
	}
	sig.Data["signalSeq"] = sig.SignalSeq
	rs.queue = append(rs.queue, sig)
	if f := testHookGet(&testQueueLenGate); f != nil {
		f(len(rs.queue))
	}
	if !rs.running {
		rs.running = true
		rs.wg.Add(1)
		go rs.run()
	}
	select {
	case rs.notify <- struct{}{}:
	default:
	}
	rs.mu.Unlock()
}

// queueLen 返回当前队列长度（观测用，测试用）。
func (rs *recoverySender) queueLen() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return len(rs.queue)
}

// dequeue 在 recoveryQueueMu 内出队队首信号。
func (rs *recoverySender) dequeue() {
	rs.mu.Lock()
	if len(rs.queue) > 0 {
		rs.queue = rs.queue[1:]
	}
	rs.mu.Unlock()
}

// run 是 sender 的主循环（见 §4.4）。每次迭代顺序不可调换。
func (rs *recoverySender) run() {
	defer rs.wg.Done()
	defer func() {
		// 退出清理（见 §4.4 步骤 6）：在 recoveryQueueMu 内将 running 置 false，
		// 丢弃队列剩余信号（逐条记调试日志），关闭 exited。
		// exited 只关闭一次（sync.Once）：sender 可能在旧连接死亡后被
		// 迟到的决议再次启动，重复关闭已关闭的 channel 会 panic。
		rs.mu.Lock()
		for _, sig := range rs.queue {
			recordDebugNotice("recovery_sender_drop", sig.Type)
		}
		rs.queue = nil
		rs.running = false
		rs.mu.Unlock()
		rs.exitedOnce.Do(func() { close(rs.exited) })
	}()
	for {
		// 步骤 1：锁外检查 senderDone。
		select {
		case <-rs.done:
			return
		default:
		}
		// 步骤 2：取队首信号；队列为空则等待 notify 或 done。
		rs.mu.Lock()
		if len(rs.queue) == 0 {
			rs.mu.Unlock()
			select {
			case <-rs.done:
				return
			case <-rs.notify:
				continue
			}
		}
		sig := rs.queue[0]
		rs.mu.Unlock()
		// 写出前的 pending 存在性检查（尽力而为，见 §4.4/§13.6）：
		// 仅对 pending 型信号：pending 已不存在且 resolved 缓存也无记录
		// → 该 pending 是被取消的，出队丢弃、不写出；已决议的 pending
		// （缓存有记录）保留——FIFO 的 pending→found 顺序是设计要求的（见 §10.6.1）。
		if sig.Type == "background_task_pending" && sig.RecoveryID != "" {
			backgroundTaskMutex.Lock()
			_, inPending := pendingRecoveries[sig.RecoveryID]
			_, inResolved := resolvedRecoveries[sig.RecoveryID]
			backgroundTaskMutex.Unlock()
			if !inPending && !inResolved {
				rs.dequeue()
				continue
			}
		}
		// 步骤 3：受保护的连接快照。
		conn, closed := rs.session.wsSnapshot()
		if conn == nil || closed {
			return
		}
		// testRecoverySendGate：sender 写出之前的测试阻塞点（见 §16.3/T44）。
		if gate := testHookGet(&testRecoverySendGate); gate != nil {
			<-gate
		}
		// 步骤 4：权威发送（锁内身份检查 + 写入）。
		ok := rs.session.sendWSMessageToConn(conn, sig.Type, sig.Data)
		if ok {
			// 步骤 5（成功）：出队，继续下一迭代。
			rs.dequeue()
			continue
		}
		// 步骤 5（失败）：重新快照以区分原因；换绑或连接已死均出队丢弃并退出。
		// 写失败时 writeWSMessageLocked 已置 wsClosed=true，因此 isCurrentConn
		// 为 false 的分支覆盖全部失败情形（见 §4.4 步骤 5）。
		if !rs.session.isCurrentConn(conn) {
			rs.dequeue()
			return
		}
		// 防御性分支：连接仍是当前连接但写失败（理论上不可达，
		// 写失败必置 wsClosed）——同样出队丢弃并退出，避免无限循环。
		rs.dequeue()
		return
	}
}

// ===== R3: stateSweeper 生命周期（见 §2.10，第二轮补齐任务 5）=====

// sweeperTickInterval 是 stateSweeper 的扫描周期（var 而非 const，供测试
// 临时缩短以便快速验证 tick→扫描接线；生产值 60s 不变，见 §2.10）。
var sweeperTickInterval = 60 * time.Second

// runStateSweeper 是 stateSweeper 的循环体（见 §2.10）：每 tick 扫描一次
// （sweepOnceLocked，持 backgroundTaskMutex 的短临界区，无 I/O、无网络写）；
// 收到停止信号即返回，退出时 sweeperLive -1 并关闭 done。
// 只由 startStateSweeper 启动；sweeper 自身、其他 goroutine、测试代码
// 均不得直接调用它（停止信号只由 Main() 的 defer 关闭 stop）。
func runStateSweeper(stop <-chan struct{}, done chan<- struct{}) {
	defer func() {
		sweeperLive.Add(-1)
		close(done)
	}()
	ticker := time.NewTicker(sweeperTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			backgroundTaskMutex.Lock()
			sweepOnceLocked()
			backgroundTaskMutex.Unlock()
		}
	}
}

// startStateSweeper 启动本代次唯一的 stateSweeper（见 §2.10/任务 5.2）：
// sweeperLive 上 CAS 0→1 成功才启动，失败（已有 sweeper 在运行）返回
// started=false 且不启动第二个——重复启动在运行时被拒绝，不依赖调用方
// 不犯错。成功时返回 stop/done 通道；调用方（main()）随即注册停止 defer
// （关 stop、等 done）。Listen 失败时不调用本函数，sweeper 从不启动。
func startStateSweeper() (stop chan struct{}, done chan struct{}, started bool) {
	if !sweeperLive.CompareAndSwap(0, 1) {
		return nil, nil, false
	}
	stop = make(chan struct{})
	done = make(chan struct{})
	go runStateSweeper(stop, done)
	return stop, done, true
}

// ===== S7: Go 服务端存活看门狗运行生命周期（v7.10 §11.5.3/§11.5.4；Batch 2）=====

// watchdogTickOnce 执行一次看门狗 tick（§11.5.3）：读取当前快照，核验代次，
// 按已批准公式计算本次挂起贡献，用 CAS 提交新快照。tick 只更新快照，绝不
// 直接取消任务或改变任务状态（取消只由计时器回调的 evaluateDisconnect 决策）。
// CAS 失败时重新读取并重新计算（禁止用旧快照覆盖）；最多重试 3 次，3 次均
// 失败则放弃本次 tick（下次 tick 再算；lastTickAt 滞后 1 秒，无害，见 R15）。
// 3 次失败只允许 debug 级记录（recordDebugByLevel "all"），不得产生 error 级日志。
// 代次不匹配时放弃本次 tick（防御性：代次已 bump、快照尚未 Store 的微窗口）。
// nil 快照防御：理论上恒非 nil（init 与 reset 均 Store）；若仍为 nil，放弃本次 tick。
// 本函数不取任何锁（一次原子 Load + CAS 循环），与 taskMutex/wsMutex/
// backgroundTaskMutex 无锁交互；与 resetBackendGeneration 的 Store 以 CAS
// 失败→重读收敛。注意：tick 读真实快照指针（watchdogSnapshot.Load），不用
// loadWatchdogSnapshot——后者优先返回测试注入，注入只影响回调的决策视角，
// 绝不能进入 tick 的累计，否则注入的陈旧快照会被当作真实挂起累计。
func watchdogTickOnce() {
	for attempt := 0; attempt < 3; attempt++ {
		snap := watchdogSnapshot.Load()
		if snap == nil {
			return // 防御性：快照恒非 nil；nil 时不计算、不提交
		}
		curGen := atomic.LoadUint64(&backendGeneration)
		if snap.generation != curGen {
			return // 快照已是旧代次：放弃本次 tick，下次 tick 用新快照重新计算
		}
		now := time.Now().UnixNano() // 墙钟（UnixNano 丢弃 monotonic，见 §11.5.2）
		contribution := max(int64(0), now-snap.lastTickAt-watchdogTickInterval.Nanoseconds()-watchdogTickTolerance.Nanoseconds())
		next := &watchdogState{generation: snap.generation, lastTickAt: now, suspendedTotal: snap.suspendedTotal + contribution}
		// 测试门（T7-29/T7-40）：在 Load 与 CAS 之间暂停（已读旧快照、未提交）。
		if g := testHookGet(&testWatchdogTickGate); g != nil {
			g.pass()
		}
		if watchdogSnapshot.CompareAndSwap(snap, next) {
			return // 提交成功：本次 tick 的计算基于提交时刻仍是最新的快照
		}
		// CAS 失败：提交期间有并发写入（resetBackendGeneration 的 Store）→
		// 重新读取并重新计算（循环顶部）。
	}
	// 3 次均失败：放弃本次 tick（下次 tick 再算）。只允许 debug 级记录。
	recordDebugByLevel("all", "s7_watchdog_tick", "tick CAS abandoned after 3 attempts")
}

// runWatchdog 是看门狗的循环体（§11.5.4）：每 tick 更新一次快照；收到停止
// 信号即返回，退出时 watchdogLive -1 并关闭 done。只由 startWatchdog 启动；
// 看门狗自身、其他 goroutine、测试代码均不得直接调用它（停止信号只由
// Main() 的 defer 或测试 harness 的 stopWatchdog 关闭 stop）。
// defer 顺序（与 runStateSweeper 相同）：ticker.Stop() 先于 live/done 的
// defer 执行，因此 live 回 0 时 ticker 已停、循环已退出——新实例此时启动
// 是安全的，不会与旧实例的 tick 交错。
func runWatchdog(stop <-chan struct{}, done chan<- struct{}) {
	defer func() {
		watchdogLive.Add(-1)
		close(done)
	}()
	ticker := time.NewTicker(watchdogTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			watchdogTickOnce()
		}
	}
}

// startWatchdog 启动本进程唯一的看门狗（§11.5.4/§11.5.8，Batch 2 决策：
// main() 启动，方案 A）：watchdogLive 上 CAS 0→1 成功才启动，失败（已有
// 看门狗在运行）返回 started=false 且不启动第二个——重复启动在运行时被
// 拒绝，不依赖调用方不犯错。成功时返回 stop/done 通道；调用方（main() 或
// 测试 harness）随即注册停止（关 stop、等 done）。resetBackendGeneration
// 不停止看门狗（只重置快照）；旧实例完全退出（live 回 0）后才允许再次启动。
// 成功启动时创建新实例（新通道 + 新 closeOnce）并登记到 watchdogInstances，
// 随后返回 stop/done 通道。注意：启动不再重置任何全局停止标志——实例的关
// 闭保护属于实例自身（见 stopWatchdog），新实例的启动与旧实例的停止保护
// 互不影响。
func startWatchdog() (stop chan struct{}, done chan struct{}, started bool) {
	if !watchdogLive.CompareAndSwap(0, 1) {
		return nil, nil, false
	}
	stop = make(chan struct{})
	done = make(chan struct{})
	watchdogInstances.Store(stop, &watchdogInstance{stop: stop, done: done})
	go runWatchdog(stop, done)
	return stop, done, true
}

// stopWatchdog 幂等停止看门狗：关闭 stop channel 并等待 done（goroutine 完全
// 退出）。重复调用安全：done 已关闭时直接返回（顺序重复调用与陈旧通道）；
// 并发重复调用由实例自身的 closeOnce 保证只关闭一次该实例的 stop channel
// （重复 close 会 panic）。stop/done 为 nil 时直接返回（start 失败时调用方
// 拿到 nil 通道，停止为空操作）。
// 实例隔离（Batch 2 定点修正）：关闭保护绑定实例而非全局。停止调用用手里
// 的 stop 通道查到拥有它的实例，用该实例的 closeOnce 关闭；新实例启动时
// 创建的是另一套通道与另一个 Once，旧实例的延迟停止调用——即使在新实例启
// 动之后才恢复——仍解析到旧实例的 Once（其 Do 已由先到的停止调用执行过，
// 本次为空操作），既不会重复关闭旧通道，也不会触碰新实例的通道。注册表条
// 目永不删除（见 watchdogInstances 注释），因此迟到的查找不可能新建 Once。
// 注意：调用方必须先释放 testWatchdogTickGate 等可能阻塞 tick 迭代的测试门，
// 再调本函数——被门阻塞的 tick 不监听 stop，done 在门释放前不会关闭。
func stopWatchdog(stop chan struct{}, done chan struct{}) {
	if stop == nil || done == nil {
		return
	}
	select {
	case <-done:
		return // 已停止（done 已关闭）
	default:
	}
	if g := testHookGet(&testStopWatchdogGate); g != nil {
		g.pass()
	}
	// LoadOrStore 是原子的：每个 stop 通道有且仅有一个实例结构体，首次
	// 停止时创建、永不重置；非 startWatchdog 创建的通道（测试手工构造）
	// 也在此处得到自己的实例，与上述保证一致。
	v, _ := watchdogInstances.LoadOrStore(stop, &watchdogInstance{stop: stop, done: done})
	inst := v.(*watchdogInstance)
	inst.closeOnce.Do(func() { close(stop) })
	<-done
}

// ===== R3: sweeper 扫描（见 §2.10/§11.1）=====

// shouldRetainEntry 是 S7 C2 D-1 扩展的保留谓词（C2 报告 §8.2 A-2c；§5 F-C2-2）。
// 调用方持有 backgroundTaskMutex。本函数不嵌套持锁：先取 taskMutex 读状态
// 并释放，再原子 Load 日志指针、取 log.mu 读冻结字段并释放。返回 true 当且
// 仅当：会话有运行任务（既有 F-1）；或状态为 completed 且日志非 nil、已冻结、
// 且在 TTL 内。其他一切情形（cancelling/cancelled、disconnectPending 且未运行、
// 日志为 nil、日志未冻结且未运行）均返回 false。
// 留存上界：至多 frozenAt+5m 后的第一个 sweep tick（≤60 秒）。
func shouldRetainEntry(e loopEntry) bool {
	if e.session == nil {
		return false
	}
	e.session.taskMutex.Lock()
	running := e.session.isTaskRunning
	state := e.session.disconnectState
	e.session.taskMutex.Unlock()
	if running {
		return true // F-1，不变
	}
	if state != stateCompleted {
		return false
	}
	log := e.session.resultLog.Load()
	if log == nil {
		return false // reset 2a 或 clearCompleted 已清理
	}
	log.mu.Lock()
	frozen, frozenAt := log.frozen, log.frozenAt
	log.mu.Unlock()
	if !frozen {
		return false // 不一致态；不得“以防万一”保留（无界）
	}
	return time.Since(frozenAt) <= resultLogRetention
}

// sweepOnceLocked 执行一次状态扫描（见 §2.10）。
// 前置条件：调用方已持有 backgroundTaskMutex。
// sweeper 每 tick 调一次；同一包测试可直接调用以验证收敛，不等待 60s tick。
func sweepOnceLocked() {
	now := time.Now()
	// 1. 孤儿 dispatched → failed（见 §11.1/§7.4.1）：
	// 创建后超 60s 的 dispatched 都是已脱离其创建调用的孤儿，不再要求其 loop 已退出；
	// 转为 failed 后决议其等待者（与重查同一 resolvePendingLocked，见 §10.6.1）。
	for pid, e := range publishLedger {
		if e.state == ledgerStateDispatched && now.Sub(e.createdAt) > 60*time.Second {
			e.state = ledgerStateFailed
			e.terminalAt = now
			publishLedger[pid] = e
			for rid, p := range pendingRecoveries {
				if pendingWaitsOn(p, pid) {
					resolvePendingLocked(rid, p)
				}
			}
		}
	}
	// 2. resolved 缓存过期删除（TTL 5 分钟）与容量淘汰（上限 1000，最旧优先）。
	for rid, e := range resolvedRecoveries {
		if now.Sub(e.resolvedAt) > 5*time.Minute {
			delete(resolvedRecoveries, rid)
		}
	}
	for len(resolvedRecoveries) > 1000 {
		var oldestID string
		var oldest time.Time
		first := true
		for id, e := range resolvedRecoveries {
			if first || e.resolvedAt.Before(oldest) {
				oldestID, oldest, first = id, e.resolvedAt, false
			}
		}
		delete(resolvedRecoveries, oldestID)
	}
	// 3. connLoopState：exited 超保留期删除；容量上限 1000 只淘汰 exited 条目。
	// S7 C1 F-1：删除前检查关联 session 是否仍有运行任务——认领成功后任务的
	// 唯一可见引用就是旧连接的 exited 条目；删除它会使任务从
	// resetBackendGeneration 的孤儿收集范围中消失（僵尸任务：旧代次下继续
	// 运行、继续写行，新代次任务可并行启动）。有运行任务的条目不得删除；
	// 容量淘汰同样跳过；无可淘汰候选时允许暂时超过软上限（已有软上限警告日志）。
	// S7 C2 D-1：守卫扩展为 shouldRetainEntry——除运行任务外，已完成且日志
	// 已冻结、在 TTL 内的任务条目也保留（见 shouldRetainEntry 的有限保留条件）。
	// 锁序：调用方持有 backgroundTaskMutex；此处嵌套取 session.taskMutex
	// （backgroundTaskMutex → taskMutex，与认领/重置一致，无反向边）。
	// 任务结束后 isTaskRunning == false，后续 sweep 正常清理。
	for id, e := range connLoopState {
		if e.state == loopStateExited && now.Sub(e.exitedAt) > connLoopStateRetention {
			if shouldRetainEntry(e) {
				continue
			}
			delete(connLoopState, id)
		}
	}
	if len(connLoopState) > 1000 {
		type exitedInfo struct {
			id string
			at time.Time
		}
		exited := []exitedInfo{}
		for id, e := range connLoopState {
			if e.state == loopStateExited && !shouldRetainEntry(e) {
				exited = append(exited, exitedInfo{id, e.exitedAt})
			}
		}
		sort.Slice(exited, func(i, j int) bool { return exited[i].at.Before(exited[j].at) })
		for _, info := range exited {
			if len(connLoopState) <= 1000 {
				break
			}
			delete(connLoopState, info.id)
		}
		if len(connLoopState) > 1000 {
			// 全部为活连接：软上限 + 警告日志，永不淘汰活着的连接（见 §11.4）。
			recordProgramDebugError("conn_loop_state_soft_cap", len(connLoopState))
		}
	}
	// 4. publishLedger 终态删除（三者缺一不可，见 §11.1）：
	// refcount==0、所属连接的 loop 已退出、进入终态已满 5 分钟。
	// 容量上限 1000 为软上限：只淘汰满足删除规则的最旧终态条目；
	// 若无可淘汰条目，新条目仍被添加（警告日志），永不提前删除仍被引用的 ledger。
	ledgerTerminalTime := func(e ledgerEntry) time.Time {
		if !e.terminalAt.IsZero() {
			return e.terminalAt
		}
		return e.createdAt
	}
	ownerLoopExited := func(e ledgerEntry) bool {
		if e.ownerSession == nil {
			return true
		}
		le, ok := connLoopState[e.ownerSession.connectionID]
		if !ok {
			// 条目已被 sweeper 删除（exited 超 5 分钟才删）→ 连接确已退出。
			return true
		}
		return le.state == loopStateExited
	}
	for pid, e := range publishLedger {
		if (e.state == ledgerStatePublished || e.state == ledgerStateFailed || e.state == ledgerStateSuperseded) &&
			e.refcount == 0 && ownerLoopExited(e) && now.Sub(ledgerTerminalTime(e)) > 5*time.Minute {
			delete(publishLedger, pid)
		}
	}
	if len(publishLedger) > 1000 {
		type terminalInfo struct {
			id string
			at time.Time
		}
		eligible := []terminalInfo{}
		for pid, e := range publishLedger {
			if (e.state == ledgerStatePublished || e.state == ledgerStateFailed || e.state == ledgerStateSuperseded) &&
				e.refcount == 0 && ownerLoopExited(e) {
				eligible = append(eligible, terminalInfo{pid, ledgerTerminalTime(e)})
			}
		}
		sort.Slice(eligible, func(i, j int) bool { return eligible[i].at.Before(eligible[j].at) })
		for _, info := range eligible {
			if len(publishLedger) <= 1000 {
				break
			}
			delete(publishLedger, info.id)
		}
		if len(publishLedger) > 1000 {
			recordProgramDebugError("publish_ledger_soft_cap", len(publishLedger))
		}
	}
}
