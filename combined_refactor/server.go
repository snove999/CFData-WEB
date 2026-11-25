package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	wsWriteDeadline = 10 * time.Second
	wsReadDeadline  = 90 * time.Second
)

// wsPingInterval 是 ping 间隔（atomic.Int64，纳秒；供测试临时缩短以便
// 快速触发 ping 写失败路径；生产值 30s 不变，见 §5.6/§8.4）。
// 读写一律经 Load/Store（H-02 P1）；零值 0 会使 time.NewTicker panic，
// 故 init() 存入默认值。
var wsPingInterval atomic.Int64

func init() {
	wsPingInterval.Store(int64(30 * time.Second))
}

// readDeadline 返回生效的 ReadDeadline（测试可用 testReadDeadlineOverride 覆盖，见 §16.3/T41）。
func readDeadline() time.Duration {
	if d := testHookGet(&testReadDeadlineOverride); d != nil {
		return *d
	}
	return wsReadDeadline
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("WebSocket 升级失败:", err)
		return
	}

	// 第一阶段性能日志：为每次 WS 连接分配唯一标识，仅用于日志，不参与任何业务判断。
	connectionID := fmt.Sprintf("ws-%d", atomic.AddInt64(&wsConnectionSeq, 1))
	wsOpenAt := time.Now()
	perfLogLine("ws_open", fmt.Sprintf("conn=%s remote=%s", connectionID, r.RemoteAddr))
	session := &appSession{ws: ws, connectionID: connectionID, backendGeneration: atomic.LoadUint64(&backendGeneration), wsWriteMu: new(sync.Mutex)}
	session.connectedAtNano.Store(wsOpenAt.UnixNano())
	// 第二轮补齐（任务 1 续，-race 发现的真竞态）：ping goroutine 与
	// safeGo 闭包和读循环共享可变的 session 局部变量——follow/stop_task
	// 换绑时写 session，ping 每 tick 读 session，构成数据竞争（-race
	// 在 TestPingWriteFailureSkipsReboundConn 中复现，属 round 1 遗留）。
	// 读循环继续用 session 局部变量（单 goroutine，无竞争）；跨 goroutine
	// 的读取一律经 sessionPtr（atomic.Pointer，换绑 Store 与读取 Load
	// 之间建立 happens-before）。
	var sessionPtr atomic.Pointer[appSession]
	sessionPtr.Store(session)
	// R3 §13.2：创建时捕获原始 session，供 defer 清理用（永不用重定向后的局部变量）。
	connSession := session
	// R3 §2.8.2：连接级局部状态，在创建 session 之后、handlers map 之前创建。
	connState := &wsConnState{}
	// Defer A（最先注册，最后执行）：ws_close 日志 + 断线武装安全网 + 兜底 Close（见 §13.1；S7 §10A.2）。
	// S7: Defer A 不再对前台任务即时 cancelTaskSilently()；改为调用
	// connSession.armDisconnectTimer(ws)（幂等：读循环错误分支已武装时为
	// no-op）。取消只由计时器回调在窗口到期后执行。Defer A 仍用连接创建时
	// 捕获的原始 connSession，不用可能已换绑的局部 session。
	defer func() {
		// 测试门：Defer A 执行入口（与读循环错误分支的武装区分，见 T7-16/T7-26）。
		if g := testHookGet(&testDeferAEntryGate); g != nil {
			g.pass()
		}
		armed := connSession.armDisconnectTimer(ws)
		// taskID 可能被任务 goroutine 的 beginTask/endTask 同时修改，持锁复制后再打印。
		perfLogLine("ws_close", fmt.Sprintf("conn=%s task=%s disconnect_armed=%v elapsed_ms=%d", connSession.connectionID, connSession.taskIDSnapshot(), armed, time.Since(wsOpenAt).Milliseconds()))
		ws.Close()
	}()

	// R3 §16.3 testSetupGate：session 创建之后、connLoopState 登记之前阻塞设立，
	// 使 resetBackendGeneration 的步骤 2a 收集不到本连接（见 T38 A4/T35(c)）。
	// testSetupGateReached：先发到达信号（cap 1 非阻塞发送），测试收到信号
	// 后才调用 resetBackendGeneration——否则 session 可能在 reset 之后才
	// 创建、带着新代次出生，代次隔离测试的前提失效（见交付报告）。
	if rg := testHookGet(&testSetupGateReached); rg != nil {
		select {
		case rg <- struct{}{}:
		default:
		}
	}
	if gate := testHookGet(&testSetupGate); gate != nil {
		<-gate
	}
	// R3 §11.4：connectionID 分配后登记 connLoopState（state=idle，pageId 为空——注册前未知）。
	backgroundTaskMutex.Lock()
	connLoopState[connectionID] = loopEntry{state: loopStateIdle, session: session, epoch: currentBackendEpoch(), generation: session.backendGeneration, ws: ws}
	backgroundTaskMutex.Unlock()

	// R3 §4.5：每连接一个 sender（局部变量，不进 session）；ping/sender 的
	// 退出信号与等待句柄同样是本连接的局部状态。
	connSender := newRecoverySender(session)
	senderDone := connSender.done
	pingStop := make(chan struct{})
	pingDone := make(chan struct{})
	// Defer B（中间注册）：删除本连接的 pending/resolved（按 ownerConn + 代次双重匹配）
	// + 从 pageSessions 删除本 session（见 §13.1/§11.2）。在 Defer C 之后执行
	// （sender 已退出，无人再读取这些条目）。
	defer func() {
		backgroundTaskMutex.Lock()
		defer backgroundTaskMutex.Unlock()
		for recoveryId, pendingEntry := range pendingRecoveries {
			if pendingEntry.ownerConn == ws && pendingEntry.generation == connSession.backendGeneration {
				releaseWaiterLocked(recoveryId, pendingEntry)
			}
		}
		for recoveryId, entry := range resolvedRecoveries {
			if entry.ownerConn == ws && entry.generation == connSession.backendGeneration {
				delete(resolvedRecoveries, recoveryId)
			}
		}
		if le, ok := connLoopState[connectionID]; ok && le.pageId != "" && le.generation == connSession.backendGeneration {
			if sessions, ok := pageSessions[le.pageId]; ok {
				delete(sessions, connSession)
				if len(sessions) == 0 {
					delete(pageSessions, le.pageId)
				}
			}
		}
	}()
	// Defer C（最后注册，最先执行）：关 senderDone → ws.Close() → wg.Wait()
	// → 关 pingStop → 等 pingDone（5 秒超时，见 §13.1/§8.4）。
	// pingStop 的唯一关闭者是 Defer C；原独立 defer close(pingStop) 已删除（见 §8.4）。
	defer func() {
		close(senderDone)
		ws.Close()
		connSender.wg.Wait()
		close(pingStop)
		select {
		case <-pingDone:
		case <-time.After(5 * time.Second):
			recordProgramDebugError("ws_ping_done_timeout", connectionID)
		}
	}()

	ws.SetReadDeadline(time.Now().Add(readDeadline()))
	ws.SetPongHandler(func(string) error {
		// R3 §3.2：先在 wsMutex 内检查 wsClosed，再记 lastPongAtNano 并续期 ReadDeadline。
		session.wsMutex.Lock()
		closed := session.wsClosed
		session.wsMutex.Unlock()
		if closed {
			return nil
		}
		session.lastPongAtNano.Store(time.Now().UnixNano())
		ws.SetReadDeadline(time.Now().Add(readDeadline()))
		return nil
	})

	// R3 §8.4：ping goroutine 不再用 safeGo（safeGo 的 recover 会向 session 发
	// "error" 消息——在关闭期间该发送可能取 wsMutex，且 ping 的 panic 不应向客户端发消息）。
	// 改为专用 go 语句 + defer（只记日志，不发 WS 消息；pingDone 由本 defer 关闭）。
	go func() {
		defer func() {
			if r := recover(); r != nil {
				recordProgramDebugError("ws_ping_panic", fmt.Sprintf("%v\n%s", r, debug.Stack()))
			}
			close(pingDone)
		}()
		ticker := time.NewTicker(time.Duration(wsPingInterval.Load()))
		defer ticker.Stop()
		for {
			select {
			case <-pingStop:
				return
			case <-ticker.C:
				// 经 sessionPtr 读当前 owner（换绑后即新 session）；禁止
				// 直接读共享的 session 局部变量（数据竞争，见上）。
				sess := sessionPtr.Load()
				if sess == nil {
					return
				}
				sess.wsMutex.Lock()
				if sess.wsClosed {
					sess.wsMutex.Unlock()
					return
				}
				// Ping 写与消息写共用按连接共享的 wsWriteMu（见 types.go 注释）。
				// 惰性初始化（已持有 sess.wsMutex，见 writeWSMessageLocked）。
				if sess.wsWriteMu == nil {
					sess.wsWriteMu = new(sync.Mutex)
				}
				sess.wsWriteMu.Lock()
				err := ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(wsWriteDeadline))
				sess.wsWriteMu.Unlock()
				sess.wsMutex.Unlock()
				if err != nil {
					// 第二轮补齐（任务 1，修订 §5.6；用户 2026-11-12 授权）：
					// ping 写失败后重新加载当前 owner 并检查连接身份——只有
					// 当当前 owner 的 ws 仍是本次 ping 的 ws（即本连接仍是
					// session 的当前有效连接）时，才置 wsClosed=true。换绑后
					// owner 已指向新 session，旧连接的失败不得误伤新连接。
					// 重新加载而非复用 tick 时的 sess：换绑若发生在 tick 与
					// 失败之间，标记须落在当前 owner 上，不得落在已被换绑
					// 的旧 session 上。置位后后续发送（直接/定向）与后续
					// ping 均被 wsClosed 检查拦截。
					// 选项 A 保持：不 Close（读循环按序读完已缓冲的消息再退出，
					// socket 由 Defer C 关闭；写失败 Close 会丢弃缓冲消息）。
					if cur := sessionPtr.Load(); cur != nil {
						cur.wsMutex.Lock()
						if cur.ws == ws {
							cur.wsClosed = true
						}
						cur.wsMutex.Unlock()
					}
					return
				}
			}
		}
	}()

	// 第三阶段：最小 init_config 只含本地可得字段，不等待外网探测。
	// speedTestDefault 同步推导即可：auto 时为 "auto"，非 auto 时为配置值本身；
	// 具体测速源由 resolveStartupSpeedTestURL 在异步探测里解析，实际测速时按需选择。
	// 地区探测结果待异步消息 proxy_country_result 回填；此处不断言无警告。
	defaultSpeedURL := strings.TrimSpace(speedTestURL)
	if defaultSpeedURL == "" || isAutoSpeedURL(defaultSpeedURL) {
		defaultSpeedURL = autoSpeedURLValue
	}
	perfLogLine("ws_init_config", fmt.Sprintf("conn=%s elapsed_ms=%d geo=pending speed_url=pending", connectionID, time.Since(wsOpenAt).Milliseconds()))
	session.sendWSMessage("init_config", map[string]interface{}{
		// connectionId 为第一阶段新增的可选字段，仅供前端日志关联同一条连接；前端不得据此改变行为。
		"connectionId": connectionID,
		// R3 §10.1/§10.5：init_config 携带当前 backendEpoch，是前端记录 epoch 的唯一来源。
		// main() 顶部尚未调用 resetBackendGeneration 之前为空（epoch 未知，见 §2.9）。
		"backendEpoch": currentBackendEpoch(),
		// S6: 结果重放能力标记（v7 §8.1）。旧前端忽略未知字段；新前端缺失
		// 该标记时视为旧后端，不发 replay_results。
		"resultReplay": true,
		// P0: replay_complete requestId 回显能力标记（N-13）。本后端已实现
		// P0-1 回显，故为 true；旧前端忽略未知字段；新前端缺失该标记时
		// 视为旧后端（fail-closed），不发 replay_results。
		"replayCompleteEcho": true,
		"speedTestURL":       speedTestURL,
		"speedTestDefault":   defaultSpeedURL,
		"speedTestWorkers":   speedTestWorkers,
		"debug":              debugMode,
		"version":            appVersion,
		"releaseURL":         releaseLatestURL,
		"cfCountry":          "",
		"proxyWarning":       false,
		"geoCheckOK":         false,
		"skipGeoCheck":       skipGeoCheck,
		"speedURLStatus":     speedURLStatusPayload(),
		"proxyStatus":        map[string]interface{}{"configured": managementProxyConfigured(), "source": proxySourceLabel()},
	})
	// 第三阶段：地区探测与测速源解析改为异步，不阻塞 init_config。
	// 捕获发起探测的原始连接与会话；发送前用 sendWSMessageToConn 校验仍是同一连接，
	// 旧连接的结果绝不发给新连接。safeGo 传入 nil 会话：探测 goroutine 即使 panic
	// 也不向任何连接发送错误。两个探测都以 r.Context() 为父上下文，连接结束即取消，
	// 各自又有 7 秒 / 5 秒超时，不会泄漏也不会无限期挂起。
	probeConn, probeSession := ws, session
	probeStartedAt := time.Now()
	safeGo("ws-geo-probe", nil, func() {
		if skipGeoCheck {
			sent := probeSession.sendWSMessageToConn(probeConn, "proxy_country_result", map[string]interface{}{"cfCountry": "SKIPPED", "proxyWarning": false, "skipGeoCheck": true, "source": "init"})
			perfLogLine("ws_geo_probe_done", fmt.Sprintf("conn=%s elapsed_ms=%d cfCountry=SKIPPED sent=%v", connectionID, time.Since(probeStartedAt).Milliseconds(), sent))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 7*time.Second)
		cfCountry, cfCountryOK := detectCloudflareTraceCountry(ctx)
		cancel()
		sent := probeSession.sendWSMessageToConn(probeConn, "proxy_country_result", map[string]interface{}{"cfCountry": cfCountry, "proxyWarning": !cfCountryOK || shouldWarnProxyCountry(cfCountry), "geoCheckOK": cfCountryOK, "source": "init"})
		perfLogLine("ws_geo_probe_done", fmt.Sprintf("conn=%s elapsed_ms=%d cfCountry=%s geo_ok=%v sent=%v", connectionID, time.Since(probeStartedAt).Milliseconds(), cfCountry, cfCountryOK, sent))
	})
	safeGo("ws-speed-probe", nil, func() {
		resolvedSpeedURL, speedISP, speedISPErr := resolveStartupSpeedTestURL(r.Context(), speedTestURL)
		if speedISPErr != nil {
			recordDebugError("speed_isp_check", speedISPErr.Error())
		}
		if speedISPErr == nil {
			recordDebugByLevel("all", "speed_isp_check", fmt.Sprintf("asn=%d org=%s mobile=%v selected=%s", speedISP.ASN, speedISP.ASOrganization, isChinaMobileISP(speedISP), currentAutoSpeedURLDefault()))
		}
		sent := probeSession.sendWSMessageToConn(probeConn, "speed_url_status", speedURLStatusPayload())
		perfLogLine("ws_speed_probe_done", fmt.Sprintf("conn=%s elapsed_ms=%d resolved=%s err=%v sent=%v", connectionID, time.Since(probeStartedAt).Milliseconds(), resolvedSpeedURL, speedISPErr, sent))
	})
	// R3 §10.4/§2.8：connect-time found/missing 的指针、快照与 epoch 必须在
	// 同一个 backgroundTaskMutex 持有内捕获（禁止锁外读 epoch 再与锁内状态组合）；
	// 代次复核也在同一持有内：本连接的代次若已失效，不发送任何业务信号，
	// 读循环入口校验会随后发 backend_restarted 并退出（见 §2.8.2）。
	backgroundTaskMutex.Lock()
	connectGenOK := session.backendGeneration == atomic.LoadUint64(&backendGeneration)
	connectSnap, connectEpoch, connectVersion, connectFound := currentBackgroundTaskSnapshotLocked()
	backgroundTaskMutex.Unlock()
	if !connectGenOK {
		// 代次已失效（重置发生在 setup 期间）：不发送 connect-time 信号。
		perfLogLine("ws_background_task_stale", fmt.Sprintf("conn=%s", connectionID))
	} else if connectFound {
		perfLogLine("ws_background_task_found", fmt.Sprintf("conn=%s task=%s label=%s mode=%s running=%v", connectionID, connectSnap.TaskID, connectSnap.Label, connectSnap.Mode, connectSnap.Running))
		session.sendWSMessage("background_task_found", map[string]interface{}{"snapshot": connectSnap, "backendEpoch": connectEpoch, "taskVersion": connectVersion})
	} else {
		// 第四阶段 B 批次：无后台任务时也发明确信号，与 get_background_task_status
		// 的 missing 回复复用同一消息类型，前端据此结束重连恢复等待，不再无限等待。
		perfLogLine("ws_background_task_none", fmt.Sprintf("conn=%s", connectionID))
		session.sendWSMessage("background_task_missing", map[string]interface{}{"reason": "no-task", "backendEpoch": connectEpoch, "taskVersion": connectVersion})
	}
	// version-check 闭包运行在 safeGo goroutine：捕获当时的 session 值，
	// 不得直接读共享的 session 局部变量（换绑竞态，见 sessionPtr 注释）。
	vcSession := session
	safeGo("version-check", vcSession, func() {
		ctx, cancel := context.WithTimeout(r.Context(), 7*time.Second)
		defer cancel()
		info, err := getLatestRelease(ctx)
		perfLogLine("ws_version_info", fmt.Sprintf("conn=%s err=%v", connectionID, err))
		if err != nil {
			recordDebugError("version_check", err.Error())
			vcSession.sendWSMessage("version_info", map[string]interface{}{"version": appVersion, "releaseURL": releaseLatestURL, "error": err.Error()})
			return
		}
		vcSession.sendWSMessage("version_info", map[string]interface{}{"version": appVersion, "latest": info.TagName, "releaseURL": releaseLatestURL, "hasUpdate": versionIsOlder(appVersion, info.TagName)})
	})

	safeHandler := func(name string, fn func(json.RawMessage), data json.RawMessage) {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("handler %s panic: %v\n%s\n", name, r, debug.Stack())
				recordProgramDebugError("handler_panic", fmt.Sprintf("%s: %v\n%s", name, r, debug.Stack()))
				session.sendWSMessage("error", fmt.Sprintf("内部错误（%s），请重试；若持续发生请查看后端日志", name))
				session.cancelTaskSilently()
			}
		}()
		fn(data)
	}

	handlers := map[string]func(json.RawMessage){
		"start_task": func(data json.RawMessage) {
			var params startTaskRequest
			if err := json.Unmarshal(data, &params); err != nil {
				session.sendWSMessage("error", "start_task 参数解析失败")
				return
			}
			if params.Threads <= 0 {
				params.Threads = 100
			}
			if params.Port <= 0 {
				params.Port = 443
			}
			if params.Delay < 0 {
				params.Delay = 0
			}
			scanMode := params.ScanMode
			if scanMode == "" {
				scanMode = scanModeTCPing
			}
			autoSpeed := params.AutoSpeed && params.OfficialSpeedLimit > 0
			session.startTaskNamed("官方优选扫描", "official", map[string]interface{}{"ipType": params.IPType, "threads": params.Threads, "port": params.Port, "delay": params.Delay, "scanMode": scanMode, "autoSpeed": autoSpeed}, func(ctx context.Context, session *appSession) {
				runOfficialTask(ctx, session, params.IPType, params.Threads, params.Port, params.Delay, scanMode)
				if ctx.Err() != nil || !autoSpeed || !session.isBackgroundTask() {
					return
				}
				dc := strings.TrimSpace(params.OfficialTargetDC)
				if dc == "" {
					session.scanMutex.Lock()
					copy := append([]ScanResult(nil), session.scanResults...)
					session.scanMutex.Unlock()
					dc = pickBestDataCenter(copy)
				}
				if dc == "" || ctx.Err() != nil {
					return
				}
				runDetailedTest(ctx, session, dc, params.OfficialSpeedPort, params.Delay, scanMode)
				if ctx.Err() != nil || !session.isBackgroundTask() || params.OfficialSpeedLimit <= 0 {
					return
				}
				speedURL := strings.TrimSpace(params.OfficialSpeedURL)
				if speedURL == "" || isAutoSpeedURL(speedURL) {
					speedURL = speedTestURL
				}
				session.testMutex.Lock()
				results := append([]TestResult(nil), session.testResults...)
				session.testMutex.Unlock()
				if len(results) > 0 {
					runOfficialSpeedBatch(ctx, session, params.OfficialSpeedPort, speedURL, params.OfficialSpeedLimit, params.OfficialSpeedMin, results, false)
				}
			})
		},
		"start_test": func(data json.RawMessage) {
			var params startTestRequest
			if err := json.Unmarshal(data, &params); err != nil {
				session.sendWSMessage("error", "start_test 参数解析失败")
				return
			}
			if params.Port <= 0 {
				params.Port = 443
			}
			if params.Delay < 0 {
				params.Delay = 0
			}
			scanMode := params.ScanMode
			if scanMode == "" {
				scanMode = scanModeTCPing
			}
			session.startTaskNamed("官方详细测试", "official", map[string]interface{}{"dc": params.DC, "port": params.Port, "delay": params.Delay, "scanMode": scanMode}, func(ctx context.Context, session *appSession) {
				runDetailedTest(ctx, session, params.DC, params.Port, params.Delay, scanMode)
			})
		},
		"start_speed_test": func(data json.RawMessage) {
			var params startSpeedTestRequest
			if err := json.Unmarshal(data, &params); err != nil {
				session.sendWSMessage("error", "start_speed_test 参数解析失败")
				return
			}
			if params.Port <= 0 {
				params.Port = 443
			}
			session.startTaskNamed("单 IP 测速", "official", map[string]interface{}{"ip": params.IP, "port": params.Port, "url": params.URL}, func(ctx context.Context, session *appSession) {
				runSpeedTest(ctx, session, params.IP, params.Port, params.URL)
			})
		},
		"start_official_speed_batch": func(data json.RawMessage) {
			var params startOfficialSpeedBatchRequest
			if err := json.Unmarshal(data, &params); err != nil {
				session.sendWSMessage("error", "start_official_speed_batch 参数解析失败")
				return
			}
			if params.Port <= 0 {
				params.Port = 443
			}
			if params.SpeedLimit < 0 {
				params.SpeedLimit = 0
			}
			if params.SpeedMin <= 0 {
				params.SpeedMin = 0.1
			}
			if strings.TrimSpace(params.URL) == "" {
				params.URL = speedTestURL
			}
			session.startTaskNamed("官方批量测速", "official", map[string]interface{}{"port": params.Port, "url": params.URL, "speedLimit": params.SpeedLimit, "speedMin": params.SpeedMin, "skipTested": params.SkipTested}, func(ctx context.Context, session *appSession) {
				runOfficialSpeedBatch(ctx, session, params.Port, params.URL, params.SpeedLimit, params.SpeedMin, params.Results, params.SkipTested)
			})
		},
		"start_nsb_task": func(data json.RawMessage) {
			var params startNSBTaskRequest
			if err := json.Unmarshal(data, &params); err != nil {
				session.sendWSMessage("error", "start_nsb_task 参数解析失败")
				return
			}
			if params.MaxThreads <= 0 {
				params.MaxThreads = speedTestWorkers
			}
			if params.SpeedTest < 0 {
				params.SpeedTest = 0
			}
			if params.Delay < 0 {
				params.Delay = 0
			}
			if params.ResultLimit < 0 {
				params.ResultLimit = 0
			}
			if params.SpeedLimit < 0 {
				params.SpeedLimit = 0
			}
			if params.SpeedMin < 0 {
				params.SpeedMin = 0
			}
			if strings.TrimSpace(params.SpeedURL) == "" {
				params.SpeedURL = speedTestURL
			}
			if strings.TrimSpace(params.OutFile) == "" {
				params.OutFile = "ip.csv"
			}
			hasFileContent := strings.TrimSpace(params.FileContent) != ""
			hasSourceURL := strings.TrimSpace(params.SourceURL) != ""
			if hasFileContent == hasSourceURL {
				session.sendWSMessage("error", "请选择本地文件或网络URL（二选一）")
				return
			}
			if hasSourceURL {
				parsedURL, err := url.Parse(params.SourceURL)
				if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" {
					session.sendWSMessage("error", "网络URL必须是有效的 http/https 地址")
					return
				}
			}
			scanMode := params.ScanMode
			if scanMode == "" {
				scanMode = scanModeTCPing
			}
			session.startTaskNamed("非标优选", "nsb", map[string]interface{}{"fileName": params.FileName, "sourceURL": params.SourceURL, "outFile": params.OutFile, "maxThreads": params.MaxThreads, "fallbackPort": params.FallbackPort, "speedTest": params.SpeedTest, "speedURL": params.SpeedURL, "enableTLS": params.EnableTLS, "delay": params.Delay, "resultLimit": params.ResultLimit, "dc": params.DC, "speedMin": params.SpeedMin, "speedLimit": params.SpeedLimit, "compact": params.Compact, "scanMode": scanMode}, func(ctx context.Context, session *appSession) {
				fileName := params.FileName
				fileContent := params.FileContent
				if hasSourceURL {
					session.sendWSMessage("log", "正在获取非标网络输入: "+params.SourceURL)
					content, err := getURLContentViaProxy(ctx, params.SourceURL)
					if err != nil {
						if ctx.Err() != nil {
							return
						}
						session.sendWSMessage("error", "获取非标网络输入失败: "+err.Error())
						return
					}
					fileName = params.SourceURL
					fileContent = content
				}
				runNSBTask(ctx, session, fileName, fileContent, params.OutFile, params.MaxThreads, params.FallbackPort, params.SpeedTest, params.SpeedURL, params.EnableTLS, params.Delay, params.ResultLimit, params.DC, params.SpeedMin, params.SpeedLimit, params.Compact, scanMode, params.RelinkHost)
			})
		},
		"start_nsb_reallink": func(data json.RawMessage) {
			var params startNSBReallinkRequest
			if err := json.Unmarshal(data, &params); err != nil {
				session.sendWSMessage("error", "start_nsb_reallink 参数解析失败")
				return
			}
			params.RelinkHost = strings.TrimSpace(params.RelinkHost)
			if params.RelinkHost == "" {
				session.sendWSMessage("error", "链路复验未配置探针域名（relinkhost）")
				return
			}
			if params.SpeedTest < 0 {
				params.SpeedTest = 0
			}
			if params.SpeedLimit < 0 {
				params.SpeedLimit = 0
			}
			if params.SpeedMin < 0 {
				params.SpeedMin = 0
			}
			if strings.TrimSpace(params.SpeedURL) == "" {
				params.SpeedURL = speedTestURL
			}
			if strings.TrimSpace(params.OutFile) == "" {
				params.OutFile = "ip.csv"
			}
			if len(params.Results) == 0 {
				session.sendWSMessage("error", "没有可复验的非标结果")
				return
			}
			scanMode := params.ScanMode
			if scanMode == "" {
				scanMode = scanModeTCPing
			}
			session.startTaskNamed("链路复验", "nsb", map[string]interface{}{"outFile": params.OutFile, "speedTest": params.SpeedTest, "speedLimit": params.SpeedLimit}, func(ctx context.Context, session *appSession) {
				runNSBReallinkTask(ctx, session, params.Results, params.RelinkHost, params.SpeedTest, params.SpeedURL, params.EnableTLS, params.SpeedMin, params.SpeedLimit, params.Compact, scanMode, params.OutFile)
			})
		},
		"start_nsb_speed_batch": func(data json.RawMessage) {
			var params startNSBSpeedBatchRequest
			if err := json.Unmarshal(data, &params); err != nil {
				session.sendWSMessage("error", "start_nsb_speed_batch 参数解析失败")
				return
			}
			if params.SpeedTest < 0 {
				params.SpeedTest = 0
			}
			if params.SpeedLimit < 0 {
				params.SpeedLimit = 0
			}
			if params.SpeedMin <= 0 {
				params.SpeedMin = 0.1
			}
			if strings.TrimSpace(params.SpeedURL) == "" {
				params.SpeedURL = speedTestURL
			}
			if len(params.Results) == 0 {
				session.sendWSMessage("error", "没有可测速的非标结果")
				return
			}
			session.startTaskNamed("非标批量测速", "nsb", map[string]interface{}{"speedTest": params.SpeedTest, "speedURL": params.SpeedURL, "enableTLS": params.EnableTLS, "speedMin": params.SpeedMin, "speedLimit": params.SpeedLimit, "skipTested": params.SkipTested, "compact": params.Compact}, func(ctx context.Context, session *appSession) {
				runNSBSpeedBatch(ctx, session, params.Results, params.SpeedTest, params.SpeedURL, params.EnableTLS, params.SpeedMin, params.SpeedLimit, params.SkipTested, params.Compact)
			})
		},
		"stop_task": func(data json.RawMessage) {
			if backgroundSession := currentBackgroundTaskSession(); backgroundSession != nil {
				if session != backgroundSession {
					// 先转移连接写锁，再换绑（见 adoptConnWriteMu）：转移后旧
					// session 的探测与新 session 的写共用同一把锁。
					session.adoptConnWriteMu(backgroundSession)
					backgroundSession.attachWebSocket(ws)
					session = backgroundSession
					sessionPtr.Store(backgroundSession)
				}
				backgroundSession.stopTask()
				return
			}
			session.stopTask()
		},
		// R3 §10.5：register_page 是连接首发消息（绕过前端门控），后端登记页面并回 ack。
		"register_page": func(data json.RawMessage) {
			// R3 §16.3 testRegisterGate：register_page handler 内阻塞（T32）。
			if gate := testHookGet(&testRegisterGate); gate != nil {
				<-gate
			}
			var req struct {
				PageId string `json:"pageId"`
			}
			if err := json.Unmarshal(data, &req); err != nil || strings.TrimSpace(req.PageId) == "" {
				// pageId 格式非法：防御性分支，回 register_page_rejected（不登记、不回 ack）。
				session.sendWSMessageToConn(ws, "register_page_rejected", map[string]interface{}{"pageId": req.PageId, "reason": "invalid-page-id"})
				return
			}
			pageId := strings.TrimSpace(req.PageId)
			// R3 §2.8 第 4 项：临界区内代次复核（取锁后、写入 pageSessions 之前）。
			backgroundTaskMutex.Lock()
			if session.backendGeneration != atomic.LoadUint64(&backendGeneration) {
				backgroundTaskMutex.Unlock()
				connState.connInvalid.Store(true)
				return
			}
			epoch := currentBackendEpoch()
			// 同一连接重复注册另一 pageId：先删除旧登记。
			if le, ok := connLoopState[connectionID]; ok && le.pageId != "" && le.pageId != pageId {
				if sessions, ok := pageSessions[le.pageId]; ok {
					delete(sessions, session)
					if len(sessions) == 0 {
						delete(pageSessions, le.pageId)
					}
				}
			}
			if _, ok := pageSessions[pageId]; !ok {
				pageSessions[pageId] = make(map[*appSession]pageSessionEntry)
			}
			pageSessions[pageId][session] = pageSessionEntry{epoch: epoch, generation: session.backendGeneration}
			if le, ok := connLoopState[connectionID]; ok {
				le.pageId = pageId
				le.epoch = epoch
				le.generation = session.backendGeneration
				connLoopState[connectionID] = le
			}
			backgroundTaskMutex.Unlock()
			// R3 §16.3 testRegisterAckGate：回 register_page_ack 之前阻塞（T33）。
			if gate := testHookGet(&testRegisterAckGate); gate != nil {
				<-gate
			}
			session.sendWSMessageToConn(ws, "register_page_ack", map[string]interface{}{"pageId": pageId, "backendEpoch": epoch})
		},
		"run_in_background": func(data json.RawMessage) {
			// R3 §7.6/§7.8：专属 defer/recover 是本 handler 的第一项可执行操作——
			// 注册先于参数解析，覆盖本 handler 的全部执行路径；safeHandler 永不接管
			// run_in_background 的 panic。
			defer func() {
				if r := recover(); r != nil {
					// 三步顺序：取消先行（见 §7.8.2）；每步各自带第二层 guard（见 §7.8.6）。
					func() {
						defer func() {
							if r2 := recover(); r2 != nil {
								fmt.Println("[run_in_background] recover cancel step failed; panic swallowed")
							}
						}()
						session.cancelTaskSilently()
					}()
					func() {
						defer func() {
							if r2 := recover(); r2 != nil {
								fmt.Println("[run_in_background] recover log step failed; panic swallowed")
							}
						}()
						recordProgramDebugError("handler_panic", fmt.Sprintf("run_in_background: %v\n%s", r, debug.Stack()))
					}()
					func() {
						defer func() {
							if r2 := recover(); r2 != nil {
								fmt.Println("[run_in_background] recover send step failed; panic swallowed")
							}
						}()
						// testPublishRecoverSendGate（测试钩子，生产恒 nil，见 §16.3）
						// 位于发送步骤、sendWSMessageToConn 调用之前、锁外。
						if gate := testHookGet(&testPublishRecoverSendGate); gate != nil {
							<-gate
						}
						if !session.sendWSMessageToConn(ws, "error", "内部错误（run_in_background），请重试；若持续发生请查看后端日志") {
							ws.Close()
						}
					}()
				}
			}()
			// 失败发送规则（见 §7.6）：本 handler 的任何一次响应发送返回 false，
			// 立即关闭本次请求的原始连接 ws，不再发送后续响应，直接返回。
			send := func(ok bool) bool {
				if !ok {
					ws.Close()
					return false
				}
				return true
			}
			// 解析 publishId 与 pageId（前端新协议，见 §10）。
			// data 为空时按 {} 处理；解析失败时发 "error" 后返回——解析失败不是发布决议。
			if len(data) == 0 {
				data = []byte("{}")
			}
			var req struct {
				PublishId string `json:"publishId"`
				PageId    string `json:"pageId"`
			}
			if err := json.Unmarshal(data, &req); err != nil {
				send(session.sendWSMessageToConn(ws, "error", "请求格式错误"))
				return
			}
			publishId, pageId := req.PublishId, req.PageId
			if publishId == "" {
				// 旧客户端兼容：生成服务端 publishId，但该 publish 不可被恢复查询引用
				// （无 pageId 归属，前端无法声明它为意图——见 §10 的兼容性规则）。
				publishId = fmt.Sprintf("pub-server-%s-%d", session.connectionID, atomic.AddInt64(&serverPublishSeq, 1))
			}
			res := publishBackgroundTask(session, publishId, pageId, ws)
			// testPublishReturnGate（见 §16.3）：publishBackgroundTask 返回后、switch 发送之前。
			if gate := testHookGet(&testPublishReturnGate); gate != nil {
				<-gate
			}
			switch res.outcome {
			case "committed", "already-published":
				// 幂等响应使用 publishBackgroundTask 返回的原始快照（见 §7.3/§7.6）：
				// 永不使用 session.backgroundSummary()——当前 session 不一定是原始发布 session。
				if !send(session.sendWSMessageToConn(ws, "background_task_enabled", map[string]interface{}{
					"publishId":    publishId,
					"taskId":       res.snapshot.TaskID,
					"backendEpoch": res.snapshot.BackendEpoch,
					"taskVersion":  res.snapshot.TaskVersion,
					"snapshot":     res.snapshot,
				})) {
					return
				}
				if !send(session.sendWSMessageToConn(ws, "log", "当前任务已转入后台运行")) {
					return
				}
			case "duplicate-in-flight":
				// 不发任何信号：在途的另一次发布会发出 enabled 或 publish_failed。
			case "generation-mismatch":
				// 连接生命周期失效，不是普通发布失败：不发送 background_task_publish_failed；
				// handler 在此分支置本连接的 connState.connInvalid（见 §7.6），
				// 读循环在本次分发后发送 backend_restarted 一次并退出。
				connState.connInvalid.Store(true)
				return
			case "page-mismatch":
				if !send(session.sendWSMessageToConn(ws, "background_task_publish_failed", map[string]interface{}{"publishId": publishId, "backendEpoch": res.backendEpoch, "reason": "page-mismatch"})) {
					return
				}
			default: // "failed"、"already-failed"、"already-superseded"
				if !send(session.sendWSMessageToConn(ws, "background_task_publish_failed", map[string]interface{}{"publishId": publishId, "backendEpoch": res.backendEpoch, "reason": res.outcome})) {
					return
				}
			}
		},
		// R3 §10.6：恢复查询（B 的恢复查询，带未确认意图）。
		"recovery_query": func(data json.RawMessage) {
			var req struct {
				PageId            string   `json:"pageId"`
				UnackedPublishIds []string `json:"unackedPublishIds"`
				RecoveryId        string   `json:"recoveryId"`
				Attempt           int      `json:"attempt"`
			}
			if err := json.Unmarshal(data, &req); err != nil || strings.TrimSpace(req.PageId) == "" || strings.TrimSpace(req.RecoveryId) == "" {
				session.sendWSMessageToConn(ws, "error", "请求格式错误")
				return
			}
			handleRecoveryQuery(session, connState, connSender, ws, strings.TrimSpace(req.PageId), req.UnackedPublishIds, strings.TrimSpace(req.RecoveryId), req.Attempt)
		},
		// R3 §10.6.1/§11.2：同一 recoveryId 的重查（幂等，命中缓存原样返回）。
		"recovery_requery": func(data json.RawMessage) {
			var req struct {
				RecoveryId string `json:"recoveryId"`
				Attempt    int    `json:"attempt"`
			}
			if err := json.Unmarshal(data, &req); err != nil || strings.TrimSpace(req.RecoveryId) == "" {
				session.sendWSMessageToConn(ws, "error", "请求格式错误")
				return
			}
			// requery 消息不带 pageId/unacked：pageId 取本连接已注册的 pageId，
			// 无条目时按指针决议视为新查询（见 §11.3）。
			handleRecoveryQuery(session, connState, connSender, ws, "", nil, strings.TrimSpace(req.RecoveryId), req.Attempt)
		},
		// R3 §11.2/§13.6：取消某次恢复（删除 pending，不缓存）。
		"recovery_cancel": func(data json.RawMessage) {
			var req struct {
				RecoveryId string `json:"recoveryId"`
			}
			if err := json.Unmarshal(data, &req); err != nil || strings.TrimSpace(req.RecoveryId) == "" {
				return
			}
			backgroundTaskMutex.Lock()
			defer backgroundTaskMutex.Unlock()
			entry, ok := pendingRecoveries[req.RecoveryId]
			if !ok {
				return
			}
			// §2.8 第 3 项：待取消条目的 generation 必须等于当前代次，否则忽略；
			// 归属校验：只取消本连接自己的 pending。
			if entry.generation != atomic.LoadUint64(&backendGeneration) {
				return
			}
			if entry.ownerConn != ws {
				return
			}
			releaseWaiterLocked(req.RecoveryId, entry)
		},
		"follow_background_task": func(data json.RawMessage) {
			backgroundSession := currentBackgroundTaskSession()
			if backgroundSession == nil {
				session.sendWSMessage("error", "当前没有可跟随的后台任务")
				return
			}
			// 先转移连接写锁，再换绑（见 adoptConnWriteMu）。
			if session != backgroundSession {
				session.adoptConnWriteMu(backgroundSession)
			}
			backgroundSession.attachWebSocket(ws)
			session = backgroundSession
			sessionPtr.Store(backgroundSession)
			snapshot := backgroundSession.backgroundSummary()
			backgroundSession.sendWSMessage("background_task_following", snapshot)

			if !snapshot.Running && snapshot.Mode == "official" {
				backgroundSession.scanMutex.Lock()
				scanResults := append([]ScanResult(nil), backgroundSession.scanResults...)
				backgroundSession.scanMutex.Unlock()
				if len(scanResults) > 0 {
					backgroundSession.sendWSMessage("scan_complete_wait_dc", buildDCList(scanResults))
				}
				backgroundSession.testMutex.Lock()
				results := append([]TestResult(nil), backgroundSession.testResults...)
				backgroundSession.testMutex.Unlock()
				if len(results) > 0 {
					for _, res := range results {
						backgroundSession.sendWSMessage("test_result", res)
					}
					backgroundSession.sendWSMessage("test_complete", results)
				}
			}

			if !snapshot.Running && snapshot.Phase == "完成" {
				backgroundSession.nsbMutex.Lock()
				payload := backgroundSession.nsbCompletePayload
				backgroundSession.nsbMutex.Unlock()
				if payload != nil {
					backgroundSession.sendWSMessage("nsb_csv_complete", *payload)
				}
			}
		},
		"replay_results": func(data json.RawMessage) {
			// S6: 重放请求（v7 §5.4）。requestId 仅用于关联与日志，后端不保存、
			// 不查询、不去重（选 B，v7 §5.4.5）；每次请求客户端生成新 requestId。
			var req struct {
				TaskID       string `json:"taskId"`
				BackendEpoch string `json:"backendEpoch"`
				LastSeq      int64  `json:"lastSeq"`
				RequestID    string `json:"requestId"`
			}
			if err := json.Unmarshal(data, &req); err != nil {
				sendReplayRejected(connectionID, map[string]interface{}{"reason": "task-not-found", "requestId": ""})
				return
			}
			// S7 C2 A-1：后台优先 + taskId 不匹配落查（C2 报告 §8.2 A-1；D-3）。
			// 后台会话存在且 taskId 匹配 → 沿用现有后台路径；后台不存在或
			// taskId 不匹配 → 落到 findReplayCandidate 找前台候选（不得因
			// 一个不匹配的后台任务提前拒绝有效的前台重放请求）。
			sess := currentBackgroundTaskSession()
			if sess == nil || sess.taskIDSnapshot() != req.TaskID {
				// 查找层：同一 backgroundTaskMutex 持有内读请求者 pageId
				// （F-C2-3：pageId 取自请求连接自己的 loopEntry；条目缺失
				// 或 pageId 为空 → fail closed，回 task-not-found）。
				// findReplayCandidate 返回时 taskMutex 已释放；本处释放
				// backgroundTaskMutex 后再调 replayResults（§7.1 约束 1）。
				backgroundTaskMutex.Lock()
				pageID := ""
				if le, ok := connLoopState[connectionID]; ok {
					pageID = le.pageId
				}
				var verdict replayVerdict
				if pageID == "" {
					verdict = replayVerdict{reason: "task-not-found"}
				} else {
					verdict = findReplayCandidate(replayRequest{TaskID: req.TaskID, BackendEpoch: req.BackendEpoch, PageID: pageID}, ws)
				}
				backgroundTaskMutex.Unlock()
				if verdict.sess == nil {
					sendReplayRejected(connectionID, map[string]interface{}{"taskId": req.TaskID, "backendEpoch": req.BackendEpoch, "requestId": req.RequestID, "reason": verdict.reason})
					return
				}
				sess = verdict.sess
			}
			completed, reason := sess.replayResults(ws, req.TaskID, req.BackendEpoch, req.LastSeq, req.RequestID)
			if !completed && reason != "" {
				// replay_rejected 回给请求连接（经请求连接的原始会话发送，
				// 见 sendReplayRejected；不触发快照更新）。
				sendReplayRejected(connectionID, map[string]interface{}{"taskId": req.TaskID, "backendEpoch": req.BackendEpoch, "requestId": req.RequestID, "reason": reason})
			}
			// completed=true：replay_complete 已由 replayResults 发送。
			// completed=false 且 reason 为空：写失败，不发任何消息（客户端重请求）。
		},
		// S7 C1: 前台任务认领（v7.10 §10.1/§10.2/§10.3）。认领协议：
		// 验证身份及状态 → 原子认领 → timer 处理 → 连接换绑 → 释放锁 → resume_accepted。
		// 锁序：backgroundTaskMutex → taskMutex → wsMutex；锁内不发送（R-1）；
		// 提交点后只做重定向 + 发送（R-2，避免 safeHandler 误取消已认领任务）。
		"resume_foreground_task": func(data json.RawMessage) {
			// 测试门（T7-21/T7-34）：handler 入口、取 backgroundTaskMutex 之前暂停。
			if g := testHookGet(&testClaimEntryGate); g != nil {
				g.pass()
			}
			var raw struct {
				TaskID       string `json:"taskId"`
				BackendEpoch string `json:"backendEpoch"`
				PageID       string `json:"pageId"`
			}
			if err := json.Unmarshal(data, &raw); err != nil || strings.TrimSpace(raw.TaskID) == "" || strings.TrimSpace(raw.PageID) == "" || strings.TrimSpace(raw.BackendEpoch) == "" {
				session.sendWSMessageToConn(ws, "resume_rejected", map[string]interface{}{"reason": "task-not-found"})
				return
			}
			req := claimRequest{TaskID: strings.TrimSpace(raw.TaskID), BackendEpoch: strings.TrimSpace(raw.BackendEpoch), PageID: strings.TrimSpace(raw.PageID)}
			// C-2 预检 + 认领查找：同一 backgroundTaskMutex 持有内，先核验请求者
			// 自己的 session，再枚举 connLoopState。findClaimCandidate 在 claim
			// 时返回持有 taskMutex 的 session（线性化点在同一持有内完成）。
			backgroundTaskMutex.Lock()
			verdict := findClaimCandidate(req, ws, session)
			switch verdict.action {
			case claimActionClaim:
				sess := verdict.sess
				// 测试门（T7-18/T7-20/T7-21）：核验通过后、状态转换之前暂停
				// （持有 backgroundTaskMutex + sess.taskMutex）。
				if g := testHookGet(&testClaimCommitGate); g != nil {
					g.pass()
				}
				// 原子认领（同一持有内）：状态转换 + 停表 + 清武装字段 + 连接换绑。
				// 换绑：新连接继承旧 session 的写锁（adoptConnWriteMu；sess ==
				// session 时跳过——调用方须保证 newOwner != s），再 attachWebSocket
				// （置 s.ws、清 wsClosed：新连接刚投递了认领帧，读方向已证明存活；
				// 自认领时清掉 ping 写失败留下的陈旧 wsClosed，否则 resume_accepted
				// 将被 sendWSMessageToConn 的 wsClosed 检查拦截）。
				sess.disconnectState = stateRunning
				sess.stopDisconnectTimerLocked()
				sess.disconnectArmedTaskID = ""
				sess.disconnectArmedEpoch = ""
				sess.disconnectArmedGen = 0
				sess.disconnectAt = 0
				if sess != session {
					session.adoptConnWriteMu(sess)
				}
				sess.attachWebSocket(ws)
				sess.taskMutex.Unlock()
				backgroundTaskMutex.Unlock()
				// 重定向局部变量 + sessionPtr（follow/stop_task 同款模式；
				// sess == session 时为 no-op）。随后锁外发 resume_accepted
				// （R-1；sendWSMessageToConn 在发送时复核归属，换绑窗口内被
				// 抢占则静默丢弃，前端按无应答重试认领）。
				session = sess
				sessionPtr.Store(sess)
				sess.sendWSMessageToConn(ws, "resume_accepted", map[string]interface{}{"taskId": req.TaskID, "backendEpoch": req.BackendEpoch})
			case claimActionIdempotent:
				// C-1 自认领幂等：任务已是 running 且 s.ws == 请求连接；不转换、
				// 不换绑、不改状态，直接回 resume_accepted（第一次应答丢失后的重试）。
				backgroundTaskMutex.Unlock()
				verdict.sess.sendWSMessageToConn(ws, "resume_accepted", map[string]interface{}{"taskId": req.TaskID, "backendEpoch": req.BackendEpoch})
			default:
				reason := verdict.reason
				backgroundTaskMutex.Unlock()
				session.sendWSMessageToConn(ws, "resume_rejected", map[string]interface{}{"taskId": req.TaskID, "backendEpoch": req.BackendEpoch, "reason": reason})
			}
		},
		"get_background_task_status": func(data json.RawMessage) {
			// R3 §2.8：读路径的代次复核——在取锁后、读之前再次验证代次；
			// 不匹配时置本连接的 connInvalid，不读状态、不返回快照、不发送任何业务响应。
			backgroundTaskMutex.Lock()
			if session.backendGeneration != atomic.LoadUint64(&backendGeneration) {
				backgroundTaskMutex.Unlock()
				connState.connInvalid.Store(true)
				return
			}
			snap, epoch, version, ok := currentBackgroundTaskSnapshotLocked()
			backgroundTaskMutex.Unlock()
			if !ok {
				session.sendWSMessage("background_task_missing", map[string]interface{}{"reason": "no-task", "backendEpoch": epoch, "taskVersion": version})
				return
			}
			session.sendWSMessage("background_task_update", map[string]interface{}{"snapshot": snap, "backendEpoch": snap.BackendEpoch, "taskVersion": snap.TaskVersion})
		},
		"compact_ipv4": func(data json.RawMessage) {
			session.startTaskNamed("IPv4 地址库精简", "official", nil, func(ctx context.Context, session *appSession) {
				runCompactIPv4Task(ctx, session)
			})
		},
		"reset_all_config": func(data json.RawMessage) {
			resetAllConfigFiles(session)
		},
		"get_speed_url_status": func(data json.RawMessage) {
			session.sendWSMessage("speed_url_status", speedURLStatusPayload())
		},
		"heartbeat": func(data json.RawMessage) {
			// 第四阶段 A 批次：应用层心跳确认，只回给发起该心跳的当前连接。
			// 经 sendWSMessageToConn 发送，不经过 sendWSMessage 的后台任务快照更新，
			// 不触发任何业务副作用；连接已换绑或已关闭时静默丢弃。
			session.sendWSMessageToConn(ws, "heartbeat_ack", map[string]interface{}{"t": time.Now().UnixMilli()})
		},
		"get_management_proxy": func(data json.RawMessage) {
			session.sendWSMessage("proxy_status", map[string]interface{}{"configured": managementProxyConfigured(), "label": proxyStatusLabel(), "source": proxySourceLabel()})
		},
		"set_management_proxy": func(data json.RawMessage) {
			var params struct {
				Value string `json:"value"`
			}
			if err := json.Unmarshal(data, &params); err != nil {
				session.sendWSMessage("error", "set_management_proxy 参数解析失败")
				return
			}
			value := strings.TrimSpace(params.Value)
			if err := setManagementProxy(value); err != nil {
				session.sendWSMessage("error", "代理设置无效: "+err.Error())
				return
			}
			status := map[string]interface{}{"configured": managementProxyConfigured(), "label": proxyStatusLabel(), "source": proxySourceLabel()}
			saveErr := updateProxyInConfigFile(defaultCLIConfigPath(), managementProxyRaw)
			if saveErr != nil {
				status["saved"] = false
				status["message"] = "代理已生效但写入配置文件失败，重启后失效: " + saveErr.Error()
			} else {
				status["saved"] = true
			}
			if value == "" {
				if saveErr != nil {
					status["message"] = "代理已清除但写入配置文件失败，重启后旧代理将重新生效: " + saveErr.Error()
				} else {
					status["message"] = "代理已清除（回退系统环境变量代理）"
				}
			}
			session.sendWSMessage("proxy_updated", status)
		},
		"get_config": func(data json.RawMessage) {
			cfgPath := defaultCLIConfigPath()
			raw, err := os.ReadFile(cfgPath)
			if err != nil {
				session.sendWSMessage("error", "读取配置文件失败: "+err.Error())
				return
			}
			var parsed interface{}
			if err := json.Unmarshal(raw, &parsed); err != nil {
				session.sendWSMessage("error", "解析配置文件失败: "+err.Error())
				return
			}
			session.sendWSMessage("config_data", parsed)
		},
		"check_proxy_country": func(data json.RawMessage) {
			// R3 §16.3 testSlowHandlerGate：handler 入口阻塞（T34/T35/T36b 的原始竞态复现）。
			if gate := testHookGet(&testSlowHandlerGate); gate != nil {
				<-gate
			}
			if skipGeoCheck {
				session.sendWSMessage("proxy_country_result", map[string]interface{}{"cfCountry": "SKIPPED", "proxyWarning": false, "skipGeoCheck": true})
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), 7*time.Second)
			country, countryOK := detectCloudflareTraceCountry(ctx)
			cancel()
			session.sendWSMessage("proxy_country_result", map[string]interface{}{"cfCountry": country, "proxyWarning": !countryOK || shouldWarnProxyCountry(country), "geoCheckOK": countryOK})
		},
		"github_upload": func(data json.RawMessage) {
			// 上传闭包运行在 safeGo goroutine：捕获当时的 session 值，
			// 不得直接读共享的 session 局部变量（换绑竞态，见 sessionPtr 注释）。
			upSession := session
			var params githubUploadRequest
			if err := json.Unmarshal(data, &params); err != nil {
				session.sendWSMessage("error", "github_upload 参数解析失败")
				return
			}
			safeGo("github-upload", upSession, func() {
				downloadURL, err := uploadGitHubContentWithRetry(r.Context(), params, func(attempt, total int, err error) {
					if params.Silent {
						return
					}
					if err == nil {
						upSession.sendWSMessage("github_upload_status", map[string]interface{}{"attempt": attempt, "total": total, "message": fmt.Sprintf("第 %d/%d 次上传中", attempt, total)})
						return
					}
					upSession.sendWSMessage("github_upload_status", map[string]interface{}{"attempt": attempt, "total": total, "message": fmt.Sprintf("第 %d/%d 次上传失败，准备重试: %s", attempt, total, err.Error())})
				})
				if err != nil {
					upSession.sendWSMessage("github_upload_error", map[string]interface{}{"path": params.Path, "message": "上传 GitHub 失败: " + err.Error() + "（可在设置菜单配置代理后重试）", "silent": params.Silent})
					return
				}
				upSession.sendWSMessage("github_upload_result", map[string]interface{}{"path": params.Path, "rawURL": downloadURL, "silent": params.Silent})
			})
		},
		"edgetunnel_upload": func(data json.RawMessage) {
			// 上传闭包运行在 safeGo goroutine：捕获当时的 session 值，
			// 不得直接读共享的 session 局部变量（换绑竞态，见 sessionPtr 注释）。
			upSession := session
			var params edgetunnelUploadRequest
			if err := json.Unmarshal(data, &params); err != nil {
				session.sendWSMessage("error", "edgetunnel_upload 参数解析失败")
				return
			}
			safeGo("edgetunnel-upload", upSession, func() {
				err := uploadToEdgetunnel(r.Context(), params, func(msg string) {
					if !params.Silent {
						upSession.sendWSMessage("edgetunnel_upload_status", map[string]interface{}{"message": msg})
					}
				})
				if err != nil {
					upSession.sendWSMessage("edgetunnel_upload_error", map[string]interface{}{"message": "edgetunnel 上传失败: " + err.Error() + "（可在设置菜单配置代理后重试）", "silent": params.Silent})
					return
				}
				upSession.sendWSMessage("edgetunnel_upload_result", map[string]interface{}{"message": "上传成功", "silent": params.Silent})
			})
		},
	}

	for {
		_, msg, err := ws.ReadMessage()
		if err != nil {
			recordDebugNotice("websocket_read", err.Error())
			perfLogLine("ws_read_error", fmt.Sprintf("conn=%s task=%s err=%v", connectionID, session.taskIDSnapshot(), err))
			// 第三阶段：连接已断，立即标记为已关闭，使随后到达的初始化异步探测
			// 结果在 sendWSMessageToConn 里直接丢弃，不再向已死连接尝试写入。
			// R3 §5.7：仅当本连接仍是 session 的当前连接时才置 wsClosed
			// （session 可能已换绑，误置会伤害新连接）。
			session.wsMutex.Lock()
			if session.ws == ws {
				session.wsClosed = true
			}
			session.wsMutex.Unlock()
			// S7: 读循环错误分支武装断线计时器（v7.10 §10A.2 主执行点）：在
			// wsClosed 标记之后、connLoopState 登记之前。helper 内部做归属检查
			// 与状态转换（同一 taskMutex 持有内）。顺序：标记死亡 → 武装 →
			// 登记退出 → break。ping 写失败分支不武装（写失败是单方向证据，
			// 真正的断线武装仍由读循环错误分支负责；半开连接按 C4 等待读检测）。
			session.armDisconnectTimer(ws)
			// R3 §11.4：loopState=exited 与置 wsClosed 在同一分支（不是 defer，
			// 避免 break→defer 的窗口）。
			backgroundTaskMutex.Lock()
			if le, ok := connLoopState[connectionID]; ok {
				le.state = loopStateExited
				le.exitedAt = time.Now()
				connLoopState[connectionID] = le
			}
			backgroundTaskMutex.Unlock()
			break
		}

		// R3 §13.3：入口校验（读循环统一执行，分发前）——失败时置本连接的
		// connInvalid、不分发、不取锁、不读状态；随后的检查点发送 backend_restarted 并退出。
		if session.backendGeneration != atomic.LoadUint64(&backendGeneration) {
			connState.connInvalid.Store(true)
		} else {
			var request wsRequest
			if err := json.Unmarshal(msg, &request); err != nil {
				session.sendWSMessage("error", "请求格式错误")
				// 代次失效检查点（见下）：JSON 错误不经过 handler，不置位，继续读。
				if connState.connInvalid.Load() {
					session.sendWSMessageToConn(ws, "backend_restarted", map[string]interface{}{"backendEpoch": currentBackendEpoch()})
					break
				}
				continue
			}
			handler, ok := handlers[request.Type]
			if !ok {
				session.sendWSMessage("error", "未知请求类型")
				if connState.connInvalid.Load() {
					session.sendWSMessageToConn(ws, "backend_restarted", map[string]interface{}{"backendEpoch": currentBackendEpoch()})
					break
				}
				continue
			}
			// R3 §16.3 testRegisterReadGate：读循环读到 register_page 之前阻塞（T36a）。
			if request.Type == "register_page" {
				if gate := testHookGet(&testRegisterReadGate); gate != nil {
					<-gate
				}
			}
			// R3 §11.4：分发前记 loopState=inHandler（handler 名、开始时间、deadline；
			// check_proxy_country 为 7s，其余为零值）；handler 返回后记 idle。
			backgroundTaskMutex.Lock()
			if le, ok := connLoopState[connectionID]; ok {
				le.state = loopStateInHandler
				le.handlerName = request.Type
				le.handlerStartedAt = time.Now()
				if request.Type == "check_proxy_country" {
					le.handlerDeadline = le.handlerStartedAt.Add(7 * time.Second)
				} else {
					le.handlerDeadline = time.Time{}
				}
				connLoopState[connectionID] = le
			}
			backgroundTaskMutex.Unlock()
			safeHandler(request.Type, handler, request.Data)
			backgroundTaskMutex.Lock()
			if le, ok := connLoopState[connectionID]; ok && le.state == loopStateInHandler {
				le.state = loopStateIdle
				le.handlerName = ""
				connLoopState[connectionID] = le
			}
			backgroundTaskMutex.Unlock()
		}
		// R3 §13.3：代次失效检查点（读循环中唯一的检查点）——
		// 无论本次分发是否执行、handler 是否 panic（safeHandler 已 recover；
		// run_in_background 的 panic 由 handler 内 recover 先行吞没），
		// 只要本连接的 connInvalid 被置位，就发送一次 backend_restarted 并退出。
		// backend_restarted 的唯一发送点是读循环（见 §2.8.2）；发送失败仍 break，不重试。
		if connState.connInvalid.Load() {
			session.sendWSMessageToConn(ws, "backend_restarted", map[string]interface{}{"backendEpoch": currentBackendEpoch()})
			break
		}
	}
}

// handleRecoveryQuery 处理 recovery_query 与 recovery_requery（见 §10.6/§10.6.1）。
// pageId 为空时取本连接已注册的 pageId（requery 消息不带 pageId）。
// unacked 为空时按指针决议（requery 无条目时视为新查询，见 §11.3）。
// 检查顺序（同一 backgroundTaskMutex 持有内）：
// (0) 代次复核 → (1) resolved 缓存命中 → (2) pending 存在（终态优先）
// → (3) 无 pending、无缓存 → A/B/C 全新决议。
func handleRecoveryQuery(session *appSession, connState *wsConnState, sender *recoverySender, conn *websocket.Conn, pageId string, unacked []string, recoveryId string, attempt int) {
	backgroundTaskMutex.Lock()
	defer backgroundTaskMutex.Unlock()
	// (0) 代次复核：不匹配时置 connInvalid，不回 "error"、不执行决策表、
	// 不创建 pending、不发送任何业务响应；读循环发送一次 backend_restarted 并退出。
	if session.backendGeneration != atomic.LoadUint64(&backendGeneration) {
		connState.connInvalid.Store(true)
		return
	}
	if pageId == "" {
		if le, ok := connLoopState[session.connectionID]; ok {
			pageId = le.pageId
		}
	}
	// (1) resolved 缓存命中 → 原样返回缓存的信号（先校验 owner）。
	if cached, ok := resolvedRecoveries[recoveryId]; ok {
		if cached.ownerConn != conn || cached.ownerPageId != pageId {
			sender.enqueue(recoverySignal{Type: "recovery_rejected", Data: map[string]interface{}{"recoveryId": recoveryId, "reason": "not-owner"}, RecoveryID: recoveryId})
			return
		}
		sender.enqueue(cached.signal)
		return
	}
	// (2) pending 存在 → 先按 B 阶段重算该 pending 全部等待意图的结局。
	if pendingEntry, ok := pendingRecoveries[recoveryId]; ok {
		if pendingEntry.ownerConn != conn || pendingEntry.ownerPageId != pageId {
			sender.enqueue(recoverySignal{Type: "recovery_rejected", Data: map[string]interface{}{"recoveryId": recoveryId, "reason": "not-owner"}, RecoveryID: recoveryId})
			return
		}
		sig, terminal, deadline := decideRecoveryLocked(pendingEntry, recoveryId)
		if terminal {
			// 全部意图已终态：以终态决议该 pending（见 §10.6.1）。
			resolvePendingLocked(recoveryId, pendingEntry)
			return
		}
		// 仍有任一意图未终态：返回新 pending（新 serverDeadline、新 signalSeq，
		// 不创建重复条目、不调用 createPendingRecoveryLocked）。
		pendingEntry.serverDeadline = deadline
		pendingEntry.attempt = attempt
		sender.enqueue(sig)
		return
	}
	// (3) 无 pending、无缓存 → 按 A/B/C 阶段全新决议。
	// 先构造一个临时 pending 做决议（不写入 map），再按结论分流。
	tmp := &pendingRecoveryEntry{ownerSession: session, ownerPageId: pageId, ownerConn: conn, waitForPageId: pageId, waitForPublishIds: normalizeWaitForPublishIds(unacked), epoch: currentBackendEpoch(), generation: session.backendGeneration}
	sig, terminal, deadline := decideRecoveryLocked(tmp, recoveryId)
	if terminal {
		// 终态决议：写 resolved 缓存 + 入队信号（不创建 pending）。
		sig = sender.assignSeq(sig)
		evictResolvedIfNeededLocked()
		resolvedRecoveries[recoveryId] = resolvedEntry{ownerSession: session, ownerPageId: pageId, ownerConn: conn, signal: sig, resolvedAt: time.Now(), epoch: currentBackendEpoch(), generation: session.backendGeneration}
		sender.enqueue(sig)
		return
	}
	// pending 结论：容量检查（见 §8.5），再经 createPendingRecoveryLocked 创建。
	if len(pendingRecoveries) >= 1000 {
		recoveryQueueRejects.Add(1)
		sender.enqueue(recoverySignal{Type: "recovery_rejected", Data: map[string]interface{}{"recoveryId": recoveryId, "reason": "pending-capacity", "serverDeadline": deadline}, RecoveryID: recoveryId})
		return
	}
	if sender.queueLen() >= 16 {
		recoveryQueueRejects.Add(1)
		sender.enqueue(recoverySignal{Type: "recovery_rejected", Data: map[string]interface{}{"recoveryId": recoveryId, "reason": "queue-full", "serverDeadline": deadline}, RecoveryID: recoveryId})
		return
	}
	entry := createPendingRecoveryLocked(session, recoveryId, pageId, unacked, conn, deadline, attempt)
	// 同一持有内启动或复用 ownerConn 的 sender（§4.5 懒启动协议），填入条目的 sender 字段。
	entry.sender = sender
	sender.enqueue(sig)
}

const githubUploadMaxAttempts = 3

func uploadGitHubContentWithRetry(ctx context.Context, params githubUploadRequest, onAttempt func(attempt, total int, err error)) (string, error) {
	var lastErr error
	for attempt := 1; attempt <= githubUploadMaxAttempts; attempt++ {
		if onAttempt != nil {
			onAttempt(attempt, githubUploadMaxAttempts, nil)
		}
		downloadURL, err := uploadGitHubContent(ctx, params)
		if err == nil {
			return downloadURL, nil
		}
		lastErr = err
		recordDebugError("github_upload_attempt", fmt.Sprintf("attempt=%d/%d path=%s err=%v", attempt, githubUploadMaxAttempts, params.Path, err))
		if onAttempt != nil {
			onAttempt(attempt, githubUploadMaxAttempts, err)
		}
		if attempt < githubUploadMaxAttempts {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(time.Duration(attempt) * time.Second):
			}
		}
	}
	return "", lastErr
}

func uploadGitHubContent(ctx context.Context, params githubUploadRequest) (string, error) {
	params.Token = strings.TrimSpace(params.Token)
	params.Owner = strings.TrimSpace(params.Owner)
	params.Repo = strings.TrimSpace(params.Repo)
	params.Branch = strings.TrimSpace(params.Branch)
	params.Path = strings.Trim(strings.TrimSpace(params.Path), "/")
	params.Message = strings.TrimSpace(params.Message)
	if params.Token == "" || params.Owner == "" || params.Repo == "" || params.Path == "" || strings.TrimSpace(params.Content) == "" {
		return "", fmt.Errorf("token、仓库、路径和内容不能为空")
	}
	if params.Branch == "" {
		params.Branch = "main"
	}
	if params.Message == "" {
		params.Message = "update cfdata results"
	}

	apiURL := fmt.Sprintf("https://api.github.com/repos/%s/%s/contents/%s", url.PathEscape(params.Owner), url.PathEscape(params.Repo), escapeGitHubContentPath(params.Path))
	sha, err := getGitHubContentSHA(ctx, apiURL, params.Token, params.Branch)
	if err != nil {
		return "", err
	}
	payload := map[string]string{
		"message": params.Message,
		"content": base64.StdEncoding.EncodeToString([]byte(params.Content)),
		"branch":  params.Branch,
	}
	if sha != "" {
		payload["sha"] = sha
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, apiURL, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	setGitHubHeaders(req, params.Token)
	resp, err := managementHTTPClient(0).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("GitHub API 返回 %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	io.Copy(io.Discard, resp.Body)
	return githubRawURL(params), nil
}

func githubRawURL(params githubUploadRequest) string {
	branch := strings.TrimSpace(params.Branch)
	if branch == "" {
		branch = "main"
	}
	path := strings.Trim(strings.TrimSpace(params.Path), "/")
	return fmt.Sprintf("https://raw.githubusercontent.com/%s/%s/refs/heads/%s/%s", url.PathEscape(strings.TrimSpace(params.Owner)), url.PathEscape(strings.TrimSpace(params.Repo)), url.PathEscape(branch), escapeGitHubContentPath(path))
}

func getGitHubContentSHA(ctx context.Context, apiURL, token, branch string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL+"?ref="+url.QueryEscape(branch), nil)
	if err != nil {
		return "", err
	}
	setGitHubHeaders(req, token)
	resp, err := managementHTTPClient(0).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return "", nil
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("查询 GitHub 文件失败 %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	var payload struct {
		SHA string `json:"sha"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", err
	}
	return payload.SHA, nil
}

func setGitHubHeaders(req *http.Request, token string) {
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "cfdata")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
}

func buildDCList(scanResults []ScanResult) []DataCenterInfo {
	dcMap := make(map[string]*DataCenterInfo)
	for _, res := range scanResults {
		if _, ok := dcMap[res.DataCenter]; !ok {
			dcMap[res.DataCenter] = &DataCenterInfo{
				DataCenter: res.DataCenter,
				DCCountry:  res.DCCountry,
				City:       res.City,
				IPCount:    0,
				MinLatency: 999999,
			}
		}
		info := dcMap[res.DataCenter]
		info.IPCount++
		if lat := int(res.TCPDuration / time.Millisecond); lat < info.MinLatency {
			info.MinLatency = lat
		}
	}
	dcList := make([]DataCenterInfo, 0, len(dcMap))
	for _, info := range dcMap {
		dcList = append(dcList, *info)
	}
	return dcList
}

func escapeGitHubContentPath(path string) string {
	parts := strings.Split(path, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}

func normalizeEdgetunnelHost(host string) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return host
	}
	if !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
		host = "https://" + host
	}
	return strings.TrimRight(host, "/")
}

func edgetunnelLogin(ctx context.Context, host, password string) (*http.Cookie, error) {
	loginURL := host + "/login"
	body := strings.NewReader("password=" + url.QueryEscape(password))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, loginURL, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := managementHTTPClient(0).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("登录失败 %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	contentType := resp.Header.Get("Content-Type")
	if strings.Contains(contentType, "text/html") {
		return nil, fmt.Errorf("密码错误")
	}
	cookies := resp.Cookies()
	for _, c := range cookies {
		if c.Name != "" {
			return c, nil
		}
	}
	return nil, fmt.Errorf("登录成功但未获取到 cookie")
}

func edgetunnelRead(ctx context.Context, host string, cookie *http.Cookie) (string, error) {
	readURL := fmt.Sprintf("%s/admin/ADD.txt?_t=%d", host, time.Now().Unix())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, readURL, nil)
	if err != nil {
		return "", err
	}
	req.AddCookie(cookie)
	req.Header.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	resp, err := managementHTTPClient(0).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func edgetunnelWrite(ctx context.Context, host string, cookie *http.Cookie, content string) error {
	writeURL := host + "/admin/ADD.txt"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, writeURL, strings.NewReader(content))
	if err != nil {
		return err
	}
	req.AddCookie(cookie)
	req.Header.Set("Content-Type", "text/plain")
	resp, err := managementHTTPClient(0).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("上传失败 %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	var result struct {
		Success bool `json:"success"`
	}
	if err := json.Unmarshal(data, &result); err != nil || !result.Success {
		return fmt.Errorf("上传返回异常: %s", strings.TrimSpace(string(data)))
	}
	return nil
}

func uploadToEdgetunnel(ctx context.Context, params edgetunnelUploadRequest, onStatus func(string)) error {
	params.Host = normalizeEdgetunnelHost(params.Host)
	params.Password = strings.TrimSpace(params.Password)
	params.Content = strings.TrimSpace(params.Content)
	if params.Host == "" {
		return fmt.Errorf("host 不能为空")
	}
	if params.Password == "" {
		return fmt.Errorf("密码不能为空")
	}
	if params.Content == "" {
		return fmt.Errorf("上传内容不能为空")
	}
	onStatus("正在登录...")
	cookie, err := edgetunnelLogin(ctx, params.Host, params.Password)
	if err != nil {
		return fmt.Errorf("登录失败: %w", err)
	}
	if params.Mode == "append" {
		onStatus("正在读取远程文件...")
		remote, err := edgetunnelRead(ctx, params.Host, cookie)
		if err != nil {
			onStatus("读取远程文件失败，将覆盖上传")
		} else if remote != "" && remote != "null" {
			params.Content = remote + "\n" + params.Content
		}
	}
	onStatus("正在上传...")
	if err := edgetunnelWrite(ctx, params.Host, cookie, params.Content); err != nil {
		return err
	}
	return nil
}
