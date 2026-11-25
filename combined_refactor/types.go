package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	requestURL      = "speed.cloudflare.com/cdn-cgi/trace"
	scanModeTCPing  = "tcping"
	scanModeHTTPing = "httping"
)

func latencyMultiplier(scanMode string, isTLS bool) float64 {
	if scanMode != scanModeHTTPing {
		return 1.0
	}
	if isTLS {
		return 4.0
	}
	return 1.3
}

const (
	timeout     = 3 * time.Second
	maxDuration = 5 * time.Second
)

var (
	//go:embed index.html login.html favicon.png
	staticFiles     embed.FS
	customDNSServer string
	customDNSForced bool
	customResolver  *net.Resolver
)

const defaultDNSServers = "223.5.5.5,8.8.8.8"

type location struct {
	Iata      string  `json:"iata"`
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	Cca2      string  `json:"cca2"`
	Region    string  `json:"region"`
	City      string  `json:"city"`
	Region_zh string  `json:"region_zh"`
	Country   string  `json:"country"`
	City_zh   string  `json:"city_zh"`
	Emoji     string  `json:"emoji"`
}

type DataCenterInfo struct {
	DataCenter string
	DCCountry  string
	City       string
	IPCount    int
	MinLatency int
}

type ScanResult struct {
	IP          string
	Port        int
	DataCenter  string
	DCCountry   string
	Region      string
	City        string
	LatencyStr  string
	TCPDuration time.Duration
	// S6: 本任务内单调递增序号（从 1 开始），小写 json 键与前端读取一致（v7 §5.3）。
	Seq int64 `json:"seq"`
}

type TestResult struct {
	IP         string
	Port       int
	DataCenter string
	DCCountry  string
	Region     string
	City       string
	MinLatency time.Duration
	MaxLatency time.Duration
	AvgLatency time.Duration
	LossRate   float64
	Speed      string
	// S6: 本任务内单调递增序号（从 1 开始），小写 json 键与前端读取一致（v7 §5.3）。
	Seq int64 `json:"seq"`
}

type iptestResult struct {
	ipAddr         string
	originalInput  string
	port           int
	dataCenter     string
	locCode        string
	region         string
	city           string
	latency        string
	lossRate       float64
	tcpDuration    time.Duration
	outboundIP     string
	ipType         string
	asnNumber      string
	asnOrg         string
	visitScheme    string
	tlsVersion     string
	sni            string
	httpVersion    string
	warp           string
	gateway        string
	rbi            string
	kex            string
	timestamp      string
	downloadSpeed  float64
	speedText      string
	speedTested    bool
	speedQualified bool
}

type nsbScanMessage struct {
	IP             string `json:"ip"`
	OriginalInput  string `json:"originalInput,omitempty"`
	Port           string `json:"port"`
	TLS            string `json:"tls,omitempty"`
	DC             string `json:"dc,omitempty"`
	Loc            string `json:"loc,omitempty"`
	Region         string `json:"region,omitempty"`
	City           string `json:"city,omitempty"`
	Latency        string `json:"latency,omitempty"`
	LossRate       string `json:"lossRate,omitempty"`
	Speed          string `json:"speed,omitempty"`
	SpeedQualified bool   `json:"speedQualified,omitempty"`
	// S6: 本任务内单调递增序号（从 1 开始），小写 json 键与前端读取一致（v7 §5.3）。
	Seq         int64  `json:"seq"`
	OutboundIP  string `json:"outboundIP,omitempty"`
	IPType      string `json:"ipType,omitempty"`
	ASNNumber   string `json:"asnNumber,omitempty"`
	ASNOrg      string `json:"asnOrg,omitempty"`
	VisitScheme string `json:"visitScheme,omitempty"`
	TLSVersion  string `json:"tlsVersion,omitempty"`
	SNI         string `json:"sni,omitempty"`
	HTTPVersion string `json:"httpVersion,omitempty"`
	Warp        string `json:"warp,omitempty"`
	Gateway     string `json:"gateway,omitempty"`
	RBI         string `json:"rbi,omitempty"`
	Kex         string `json:"kex,omitempty"`
	Timestamp   string `json:"timestamp,omitempty"`
}

type nsbCSVCompletePayload struct {
	Headers        []string   `json:"headers"`
	Rows           [][]string `json:"rows"`
	File           string     `json:"file"`
	Status         string     `json:"status"`
	Message        string     `json:"message"`
	QualifiedCount int        `json:"qualifiedCount"`
}

func (r *iptestResult) toNSBMessage(speedStr string) nsbScanMessage {
	return nsbScanMessage{
		IP:             r.ipAddr,
		OriginalInput:  r.originalInput,
		Port:           strconv.Itoa(r.port),
		TLS:            strconv.FormatBool(r.visitScheme == "https"),
		DC:             r.dataCenter,
		Loc:            r.locCode,
		Region:         r.region,
		City:           r.city,
		Latency:        r.latency,
		LossRate:       fmt.Sprintf("%.0f%%", r.lossRate*100),
		Speed:          speedStr,
		SpeedQualified: r.speedQualified,
		OutboundIP:     r.outboundIP,
		IPType:         r.ipType,
		ASNNumber:      r.asnNumber,
		ASNOrg:         r.asnOrg,
		VisitScheme:    r.visitScheme,
		TLSVersion:     r.tlsVersion,
		SNI:            r.sni,
		HTTPVersion:    r.httpVersion,
		Warp:           r.warp,
		Gateway:        r.gateway,
		RBI:            r.rbi,
		Kex:            r.kex,
		Timestamp:      r.timestamp,
	}
}

func (r *iptestResult) toNSBLiveMessage(speedStr string, compact bool) nsbScanMessage {
	if compact {
		return r.toCompactNSBMessage(speedStr)
	}
	return r.toNSBMessage(speedStr)
}

func (r *iptestResult) toCompactNSBMessage(speedStr string) nsbScanMessage {
	return nsbScanMessage{
		IP:             r.ipAddr,
		OriginalInput:  r.originalInput,
		Port:           strconv.Itoa(r.port),
		TLS:            strconv.FormatBool(r.visitScheme == "https"),
		DC:             r.dataCenter,
		Loc:            r.locCode,
		Region:         r.region,
		City:           r.city,
		Latency:        r.latency,
		LossRate:       fmt.Sprintf("%.0f%%", r.lossRate*100),
		Speed:          speedStr,
		SpeedQualified: r.speedQualified,
		OutboundIP:     r.outboundIP,
		IPType:         r.ipType,
		ASNNumber:      r.asnNumber,
		ASNOrg:         r.asnOrg,
	}
}

type csvHeaderPayload struct {
	Headers []string   `json:"headers"`
	Rows    [][]string `json:"rows"`
	File    string     `json:"file"`
}

type resetConfigResult struct {
	Success  bool     `json:"success"`
	Deleted  []string `json:"deleted"`
	Missing  []string `json:"missing"`
	Failed   []string `json:"failed"`
	Reminder string   `json:"reminder"`
}

// S6: 结果重放（v7 §5.1）。loggedRow 是任务结果日志的一行；taskResultLog
// 是每任务的只追加结果日志（单调序号空间，scan/test/nsb/speed 四类共享）。
type loggedRow struct {
	Seq  int64           // 本任务内单调递增，从 1 开始
	Type string          // "scan_result" | "test_result" | "nsb_scan_result" | "speed_test_result"
	Data json.RawMessage // 发送时的原始 JSON 载荷（含 seq 字段，见 §5.2）
}

type taskResultLog struct {
	mu        sync.Mutex
	taskID    string // 所属任务（task-N）
	epoch     string // 所属 backendEpoch（string；backendEpoch 本身即 string，见 session.go currentBackendEpoch）
	rows      []loggedRow
	nextSeq   int64 // 下一个待分配序号（从 1 开始）
	finalSeq  int64 // 任务结束时冻结的最大序号；运行中为 0（未冻结）
	frozen    bool  // 是否已冻结（endTask 置位；冻结后不再分配序号，见 §5.9）
	frozenAt  time.Time
	truncated bool  // 是否因超限停止追加（§5.1：超限后新行仍发送，只不再记入日志）
	bytes     int64 // rows 的近似字节数（用于容量判断）
}

// S6 容量与保留期（已批准初始参数，未实测最优，见计划 §8）。
const (
	maxResultLogRows   = 20000
	maxResultLogBytes  = 8 << 20 // 8 MiB
	resultLogRetention = 5 * time.Minute
)

// S7: 前台任务断线状态（v7.10 §9.1）。归属 appSession，由 taskMutex 保护。
// 五种状态：running（正常运行）→ disconnectPending（连接已断、延迟计时器
// 已武装）→ cancelling（计时器到期、已发 cancel）→ cancelled/completed（终态）。
// 不引入独立的状态机对象，不改 backgroundSnapshot.Phase 的展示语义。
type disconnectState int

const (
	stateRunning disconnectState = iota
	stateDisconnectPending
	stateCancelling
	stateCancelled
	stateCompleted
)

// S7: 看门狗快照（v7.10 §11.5.3）。三个字段均为墙钟 UnixNano（lastTickAt、
// suspendedTotal）或代次标识（generation）；快照整体经 atomic.Pointer
// 替换，永不原地修改。回调的决策函数只读墙钟，不读单调时钟（§11.5.2）。
type watchdogState struct {
	generation     uint64 // 所属 backend generation（快照创建时 atomic.LoadUint64(&backendGeneration)）
	lastTickAt     int64  // 上次看门狗 tick 的墙钟 UnixNano
	suspendedTotal int64  // 已探测到的挂起时长累计（纳秒，下界估计）
}

// watchdogSnapshot 是进程级唯一的看门狗快照指针。本批只实现快照数据结构、
// 初始化和代次重置所需的存储（resetBackendGeneration 内 Store 新快照），
// 不启动 ticker、CAS tick 循环或看门狗 goroutine（后续独立批次；启动位置
// 未决策，main.go 不改）。
var watchdogSnapshot atomic.Pointer[watchdogState]

func init() {
	// 快照初始值：进程启动墙钟、累计为零（§11.5.4）；main() 顶部的
	// resetBackendGeneration() 会再次重置（代次 bump 后）。
	watchdogSnapshot.Store(&watchdogState{generation: 0, lastTickAt: time.Now().UnixNano(), suspendedTotal: 0})
}

type appSession struct {
	ws *websocket.Conn
	// connectionID/taskID 仅用于第一阶段性能日志（区分旧连接与新连接），不参与任何业务判断。
	connectionID string
	taskID       string
	// R3: 连接创建时记录的后端代次（创建后不变），用于代次校验（见设计 §2.6/§2.8）。
	backendGeneration uint64
	// R3: 本 session 的任务所属代次（beginTask 内、持 globalTaskMutex 时置为当前代次，见 §2.4）。
	taskGeneration uint64
	// S7 C1: 任务创建时记录的 pageId（beginTask 内、与 taskID 同一 taskMutex
	// 持有内写入，v7.10 §10.3.2 步骤 0 的原子提交）。认领核验的权威归属字段
	// （§10.3.2 步骤 2）；由 taskMutex 保护。"" 表示创建时连接未注册（防御
	// 默认值：任务照常运行，但任何非空 pageId 的认领均回 not-owner）。
	taskPageId string
	// R3: 最后一次 Pong 到达时间（纳秒）与连接建立时间（纳秒），atomic.Int64，
	// 由 PongHandler Store、sweeper/恢复查询 Load，不参与锁顺序（见 §3.2）。
	lastPongAtNano  atomic.Int64
	connectedAtNano atomic.Int64
	emit            func(msgType string, data interface{})
	wsMutex         sync.Mutex
	// R3 实施期修正（T35(f)(C) 的竞态根因）：按连接共享的写锁（指针）。
	// wsMutex 是每 session 一把；follow/stop_task 换绑后两个 session 对象会
	// 先后持有同一条 *websocket.Conn（旧 session 的异步探测与 sender 仍在
	// 写），两把 wsMutex 管同一条连接时 gorilla 的并发写会 data race。
	// 因此实际写操作（WriteJSON/WriteControl）改由 wsWriteMu 串行化；
	// 换绑时新 session 继承旧 session 的 wsWriteMu（见 adoptConnWriteMu），
	// 锁属于连接、随连接转移。锁序：wsMutex → wsWriteMu（叶子锁，见 §12）。
	wsWriteMu      *sync.Mutex
	taskMutex      sync.Mutex
	isTaskRunning  bool
	taskCancel     context.CancelFunc
	backgroundTask bool
	// S7: 断线待定状态与计时器（v7.10 §9.1/§10A.2）。全部由 taskMutex 保护。
	// 每任务最多一个当前 *time.Timer（字段即当前计时器）；disconnectAt 为
	// 墙钟 UnixNano（§11.5.2：不得混用 time.Time 的 monotonic 时间）；
	// 武装身份（taskID/epoch/generation）与 S6 身份字段同源，供计时器回调
	// 的 V3/V4/V5 身份复核与 V6 计时器指针复核使用。
	disconnectState       disconnectState
	disconnectTimer       *time.Timer
	disconnectAt          int64
	disconnectArmedTaskID string
	disconnectArmedEpoch  string
	disconnectArmedGen    uint64
	backgroundSnapshot    backgroundTaskSnapshot
	// S6: 本任务的结果日志与发送协调锁（v7 §5.1/§5.2）。resultSendMu 串行
	// “分配序号 + 记日志 + 写 socket”，保证序号顺序 = 日志顺序 = 发送顺序。
	// resultLog 用 atomic.Pointer（五项修复问题 2 要求 5）：发布（beginTask）、
	// 读取（sendTaskResult/replayResults/endTask）、替换（beginTask）、清理
	// （clearCompletedBackgroundTaskSession/resetBackendGeneration）与重放快照
	// 均经原子 Load/Store，无数据竞争，且不参与锁顺序（原子操作不取锁）。
	resultLog            atomic.Pointer[taskResultLog]
	resultSendMu         sync.Mutex
	wsClosed             bool
	scanMutex            sync.Mutex
	scanResults          []ScanResult
	testMutex            sync.Mutex
	testResults          []TestResult
	nsbMutex             sync.Mutex
	nsbHeaders           []string
	nsbRows              [][]string
	nsbCompletePayload   *nsbCSVCompletePayload
	progressMutex        sync.Mutex
	progressState        map[string][2]int
	progressPrintTime    map[string]time.Time
	progressPrintPercent map[string]float64
}

// taskStartedIdentity 是 beginTask 在创建任务、提交任务身份的同一临界区内
// 原子取得的任务身份快照（S7 C1 O-1 定点修正）：taskId 与 backendEpoch 来自
// 同一次提交，不可变；task_started 只发送此快照，发送时不再分别重读 taskID
// 与当前 epoch 拼接——beginTask 返回与发送之间若发生 resetBackendGeneration，
// 旧 taskId 配新 backendEpoch 的混合身份不可能产生。零值表示“未创建任务”，
// 仅当 beginTask 返回 started==true 时有效。
type taskStartedIdentity struct {
	taskID       string
	backendEpoch string
}

type backgroundTaskSnapshot struct {
	// 第四阶段 B 批次：任务 ID（task-N），供前端校验恢复信号属于哪个任务。
	// 新增字段，旧客户端忽略。
	TaskID string `json:"taskId"`
	// R3: 快照所属的后端生命周期（发布时、与指针置位同一临界区写入，见 §10.4）。
	BackendEpoch string `json:"backendEpoch"`
	// R3: 该任务身份的版本（只在发布时写入，见 §6.2/§6.4）。
	TaskVersion    int64                  `json:"taskVersion"`
	Label          string                 `json:"label"`
	Mode           string                 `json:"mode"`
	Phase          string                 `json:"phase"`
	Message        string                 `json:"message"`
	Current        int                    `json:"current"`
	Total          int                    `json:"total"`
	Percent        float64                `json:"percent"`
	ResultCount    int                    `json:"resultCount"`
	SpeedCount     int                    `json:"speedCount"`
	ScanTotal      int                    `json:"scanTotal"`
	ScanSuccess    int                    `json:"scanSuccess"`
	ScanFailed     int                    `json:"scanFailed"`
	SpeedTotal     int                    `json:"speedTotal"`
	SpeedSuccess   int                    `json:"speedSuccess"`
	SpeedFailed    int                    `json:"speedFailed"`
	SpeedQualified int                    `json:"speedQualified"`
	TestCount      int                    `json:"testCount"`
	DCCount        int                    `json:"dcCount"`
	DCInfo         string                 `json:"dcInfo,omitempty"`
	Running        bool                   `json:"running"`
	StartedAt      time.Time              `json:"startedAt"`
	UpdatedAt      time.Time              `json:"updatedAt"`
	Params         map[string]interface{} `json:"params,omitempty"`
}

type taskStarter func(ctx context.Context, session *appSession)

var (
	locationMap   map[string]location
	locationMapMu sync.RWMutex

	upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

	configResetMutex sync.Mutex

	listenPort       int
	listenHost       string
	speedTestURL     string
	speedTestWorkers = 5
	debugMode        bool
	debugLevel       = "error"
	skipGeoCheck     bool
)

// ===== R3 (Phase 4 B S1/S5) 新增类型 =====
// 依据设计 v22.2 §2.8.2、§10.2、§11.1–§11.4、§7.3、§4.5。

// wsConnState 是 handleWebSocket 的连接级局部状态（见 §2.8.2/§13.2）。
// 与 appSession 分离：appSession 可被多个连接共享/重绑定，
// 本状态属于本连接，只被本连接的 handler 闭包与读循环引用。
type wsConnState struct {
	connInvalid atomic.Bool // 本连接的代次已失效
}

// ledgerEntry 是 publishLedger 的条目（见 §11.1）。
type ledgerEntry struct {
	state        string // dispatched | published | failed | superseded
	pageId       string
	ownerSession *appSession
	ownerConn    *websocket.Conn
	taskId       string
	version      int64
	snapshot     backgroundTaskSnapshot // 首次发布时在步骤 3 同一临界区捕获，published 后永不改写
	backendEpoch string
	generation   uint64
	createdAt    time.Time
	terminalAt   time.Time // 进入终态的时间（failed/superseded）；published 用 createdAt 作终态时钟起点
	refcount     int       // 当前绑定到该条目的 live pending 数（见 §11.5）
}

// ledger 状态常量。
const (
	ledgerStateDispatched = "dispatched"
	ledgerStatePublished  = "published"
	ledgerStateFailed     = "failed"
	ledgerStateSuperseded = "superseded"
)

// loopEntry 是 connLoopState 的条目（见 §11.4）。值类型，存于 map 中。
type loopEntry struct {
	state            string // idle | inHandler | exited
	pageId           string
	session          *appSession
	handlerName      string
	handlerStartedAt time.Time
	handlerDeadline  time.Time // check_proxy_country 为 startedAt+7s，其余为零值（未知）
	exitedAt         time.Time
	epoch            string
	generation       uint64
	ws               *websocket.Conn // 本连接的 socket，创建时记入、永不改写
}

// loop 状态常量。
const (
	loopStateIdle      = "idle"
	loopStateInHandler = "inHandler"
	loopStateExited    = "exited"
)

// pageSessionEntry 是 pageSessions 内层 map 的值（见 §2.6）。
// 每个 session 的登记记录带 epoch + generation。
type pageSessionEntry struct {
	epoch      string
	generation uint64
}

// pendingRecoveryEntry 是 pendingRecoveries 的条目（见 §11.2 唯一权威定义）。
// map 中保存的是结构体指针；recoveryId 只存在于 map key，不增加到字段中。
type pendingRecoveryEntry struct {
	ownerSession      *appSession
	ownerPageId       string
	ownerConn         *websocket.Conn
	sender            *recoverySender // 该连接的 sender，由调用方按 §4.5 懒启动后填入
	waitForPageId     string
	waitForPublishIds []string // 规范化列表（稳定去重后，见 §11.5.4）
	createdAt         time.Time
	serverDeadline    time.Time
	attempt           int
	epoch             string
	generation        uint64
}

// resolvedEntry 是 resolvedRecoveries 的条目（见 §11.3）。
type resolvedEntry struct {
	ownerSession *appSession
	ownerPageId  string
	ownerConn    *websocket.Conn
	signal       recoverySignal // 终态信号全文（含 signalSeq）
	resolvedAt   time.Time
	epoch        string
	generation   uint64
}

// publishResult 是 publishBackgroundTask 的唯一返回契约（见 §7.3）。
type publishResult struct {
	committed    bool
	outcome      string // committed | already-published | duplicate-in-flight | failed | generation-mismatch | page-mismatch | already-failed | already-superseded
	snapshot     backgroundTaskSnapshot
	hasSnapshot  bool
	backendEpoch string // 决议时在 backgroundTaskMutex 临界区内捕获的 epoch；generation-mismatch 时为空
}

// recoverySignal 是 sender 队列中的一条恢复信号（见 §4.5/§10.1）。
type recoverySignal struct {
	Type       string                 // background_task_found | background_task_missing | background_task_pending | recovery_rejected
	Data       map[string]interface{} // 信号内容（含 recoveryId、signalSeq 等）
	SignalSeq  int64                  // 本 sender 内单调递增；缓存命中重发时保留原值
	RecoveryID string                 // 所属 recoveryId（取消过滤与 pending 存在性检查用）
}

// recoverySender 是每连接一个的恢复信号 sender（见 §4.5）。
// 由 handleWebSocket 创建（局部变量，不进 session）。
type recoverySender struct {
	session    *appSession
	mu         sync.Mutex
	queue      []recoverySignal
	notify     chan struct{} // 容量 1，唤醒 sender；永不关闭
	running    bool
	done       chan struct{} // 关闭即退出信号（Defer C 关闭）
	exited     chan struct{} // sender 退出时关闭（供测试观测）
	exitedOnce sync.Once     // exited 只关闭一次：sender 可能被迟到的决议再次启动
	wg         sync.WaitGroup
	nextSeq    int64
}

// reconcilePanicGate 是 testReconcilePanicGate 的类型（见 §7.5.1/§16.3）。
type reconcilePanicGate struct {
	targetRecoveryId string
}
