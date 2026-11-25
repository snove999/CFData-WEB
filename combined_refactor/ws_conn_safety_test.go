package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// 第三阶段审查修正的可重复竞态测试：
//  1. sendWSMessageToConn 在并发换绑下的身份校验（emit/ws/wsClosed 均在同一锁内读取）；
//  2. safeGo 传入 nil session 时 panic 不会二次崩溃；
//  3. 测速源异步解析的全局状态在并发与取消下仍合法。
// 仓库此前无测试文件；本文件是本次审查后保留的唯一测试文件。

func TestSendWSMessageToConnConcurrentRebind(t *testing.T) {
	serverConns := make(chan *websocket.Conn, 2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		serverConns <- c
	}))
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"

	dial := func() *websocket.Conn {
		t.Helper()
		c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		return c
	}
	clientA := dial()
	defer clientA.Close()
	connA := <-serverConns
	clientB := dial()
	defer clientB.Close()
	connB := <-serverConns

	s := &appSession{ws: connA, connectionID: "ws-test"}
	stop := make(chan struct{})
	senderDone := make(chan struct{})
	// 发送方：持续向旧连接发送，换绑后应全部被丢弃，不得发到新连接。
	go func() {
		defer close(senderDone)
		for {
			select {
			case <-stop:
				return
			default:
				s.sendWSMessageToConn(connA, "ping", map[string]interface{}{"v": 1})
			}
		}
	}()
	// 换绑方：反复在两个连接之间换绑，与发送方并发。
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if i%2 == 0 {
				s.attachWebSocket(connB)
			} else {
				s.attachWebSocket(connA)
			}
		}
	}()
	wg.Wait()
	close(stop)
	<-senderDone

	// 换绑结束后固定到 connB：向旧连接发送必须失败，向当前连接发送必须成功。
	s.attachWebSocket(connB)
	if s.sendWSMessageToConn(connA, "stale", nil) {
		t.Fatalf("send to stale connA returned true")
	}
	if !s.sendWSMessageToConn(connB, "live", map[string]interface{}{"v": 1}) {
		t.Fatalf("send to current connB returned false")
	}
}

// emit 读写同步关系：写入端（newCLISession）与 sendWSMessageToConn 的读取端
// 使用同一把 wsMutex。生产上 emit 只在会话返回前赋值一次（CLI 会话），
// WS 会话恒为 nil，sendWSMessageToConn 不可能接触到正在被赋值的 CLI 会话；
// 本测试用并发的加锁写 + 加锁读验证该同步协议本身。
// 注意：sendWSMessage / sendWSMessageDirect 在锁外读 emit，本测试不让它们与
// emit 写并发——它们的安全性来自另一条保证：emit 在会话发布后不再改变。
func TestEmitReadWriteSync(t *testing.T) {
	serverConns := make(chan *websocket.Conn, 2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		serverConns <- c
	}))
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"

	dial := func() *websocket.Conn {
		t.Helper()
		c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		return c
	}
	clientA := dial()
	defer clientA.Close()
	connA := <-serverConns
	clientB := dial()
	defer clientB.Close()
	connB := <-serverConns

	s := &appSession{ws: connA, connectionID: "ws-test"}
	setEmit := func(fn func(string, interface{})) {
		s.wsMutex.Lock()
		s.emit = fn
		s.wsMutex.Unlock()
	}

	// 并发阶段：加锁的 emit 写与 sendWSMessageToConn / attachWebSocket 并发，
	// -race 下应无数据竞争。
	stop := make(chan struct{})
	var settersWg sync.WaitGroup
	for i := 0; i < 4; i++ {
		settersWg.Add(1)
		go func() {
			defer settersWg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					setEmit(func(string, interface{}) {})
					setEmit(nil)
				}
			}
		}()
	}
	var senderWg sync.WaitGroup
	senderWg.Add(1)
	go func() {
		defer senderWg.Done()
		for i := 0; i < 200; i++ {
			s.sendWSMessageToConn(connA, "ping", nil)
			if i%2 == 0 {
				s.attachWebSocket(connB)
			} else {
				s.attachWebSocket(connA)
			}
		}
	}()
	senderWg.Wait()
	close(stop)
	settersWg.Wait()

	// 确定性阶段：emit 已设置时 sendWSMessageToConn 必须拒绝，
	// 直接发送走 emit；清除 emit 后 WS 发送恢复正常。
	emitCalls := make(chan string, 4)
	setEmit(func(msgType string, data interface{}) { emitCalls <- msgType })
	s.attachWebSocket(connB)
	if s.sendWSMessageToConn(connB, "x", nil) {
		t.Fatalf("sendWSMessageToConn with emit set returned true")
	}
	s.sendWSMessageDirect("emit_msg", nil)
	select {
	case got := <-emitCalls:
		if got != "emit_msg" {
			t.Fatalf("emit got %q, want emit_msg", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("emit was not called")
	}
	setEmit(nil)
	if !s.sendWSMessageToConn(connB, "live", map[string]interface{}{"v": 1}) {
		t.Fatalf("sendWSMessageToConn after clearing emit returned false")
	}
	_ = clientB.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := clientB.ReadMessage(); err != nil {
		t.Fatalf("clientB read: %v", err)
	}
}

// 第四阶段 A 批次：应用层心跳。连接建立后发 heartbeat 应收到 heartbeat_ack；
// 未知类型只回 error，不会断开连接，随后心跳仍可正常确认。
func TestHeartbeatAck(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handleWebSocket)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"

	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	readUntil := func(want string, timeout time.Duration) map[string]interface{} {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for {
			_ = c.SetReadDeadline(deadline)
			_, raw, err := c.ReadMessage()
			if err != nil {
				t.Fatalf("read waiting for %s: %v", want, err)
			}
			var msg map[string]interface{}
			if err := json.Unmarshal(raw, &msg); err != nil {
				continue
			}
			typ, _ := msg["type"].(string)
			if typ == want {
				return msg
			}
		}
	}
	writeMsg := func(msgType string) {
		t.Helper()
		if err := c.WriteJSON(map[string]interface{}{"type": msgType, "data": map[string]interface{}{}}); err != nil {
			t.Fatalf("write %s: %v", msgType, err)
		}
	}

	// 先等 init_config，确认连接与初始化流程正常。
	readUntil("init_config", 10*time.Second)

	// 心跳应得到确认。
	writeMsg("heartbeat")
	ack := readUntil("heartbeat_ack", 5*time.Second)
	if _, ok := ack["data"].(map[string]interface{})["t"]; !ok {
		t.Fatalf("heartbeat_ack missing t field: %v", ack)
	}

	// 未知类型只回 error，连接保持可用，随后心跳仍可确认。
	writeMsg("no_such_type_xyz")
	readUntil("error", 5*time.Second)
	writeMsg("heartbeat")
	readUntil("heartbeat_ack", 5*time.Second)
}

// 第四阶段 B 批次：重连恢复信号。无后台任务时新连接应在 init_config 之后
// 收到 background_task_missing；有后台任务时应收到带 taskId 的 found。
func TestBackgroundTaskRecoverySignals(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handleWebSocket)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"

	readUntil := func(c *websocket.Conn, want string, timeout time.Duration) map[string]interface{} {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for {
			_ = c.SetReadDeadline(deadline)
			_, raw, err := c.ReadMessage()
			if err != nil {
				t.Fatalf("read waiting for %s: %v", want, err)
			}
			var msg map[string]interface{}
			if err := json.Unmarshal(raw, &msg); err != nil {
				continue
			}
			typ, _ := msg["type"].(string)
			if typ == want {
				return msg
			}
		}
	}

	// 无后台任务：init_config 后应收到 missing。
	c1, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial c1: %v", err)
	}
	readUntil(c1, "init_config", 10*time.Second)
	readUntil(c1, "background_task_missing", 5*time.Second)
	_ = c1.Close()

	// 注册一个后台任务：新连接应收到带 taskId 的 found。
	bg := &appSession{connectionID: "ws-test-bg", backendGeneration: atomic.LoadUint64(&backendGeneration)}
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, ok := bg.beginTask(cancel, "test task", "official", nil); !ok {
		t.Fatal("beginTask failed")
	}
	// 失败路径的安全网：t.Fatalf 会跳过函数末尾的清理，defer 保证全局会话不残留、
	// 不污染后续测试。
	t.Cleanup(func() {
		bg.endTask()
		clearCompletedBackgroundTaskSession()
	})
	if !bg.enableBackgroundTask() {
		t.Fatal("enableBackgroundTask failed")
	}
	wantID := bg.taskIDSnapshot()
	if wantID == "" {
		t.Fatal("background task has empty taskID")
	}
	c2, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial c2: %v", err)
	}
	defer c2.Close()
	readUntil(c2, "init_config", 10*time.Second)
	found := readUntil(c2, "background_task_found", 5*time.Second)
	data, _ := found["data"].(map[string]interface{})
	// R3 §10.1：found 的数据为 {snapshot, backendEpoch, taskVersion, ...}，
	// taskId 在 snapshot 内。
	snap, _ := data["snapshot"].(map[string]interface{})
	gotID, _ := snap["taskId"].(string)
	if gotID != wantID {
		t.Fatalf("found taskId = %q, want %q (data=%v)", gotID, wantID, data)
	}
	// 清理全局后台会话，避免影响其他测试。
	bg.endTask()
	clearCompletedBackgroundTaskSession()
	if currentBackgroundTaskSession() != nil {
		t.Fatal("background session not cleared after cleanup")
	}
}

// 第四阶段 B 批次修正（问题 4）：转入后台的发布必须是原子的。
// 原实现先在 taskMutex 下置位 backgroundTask=true，解锁后再注册全局会话，
// 中间存在窗口：并发查询 currentBackgroundTaskSession() 会得到 nil，
// 新连接随之误发 background_task_missing。
// 新实现先取 backgroundTaskMutex 再取 taskMutex，置位与注册在同一临界区内完成。
// 本测试用持锁的方式确定性地验证这一顺序：测试先持有 backgroundTaskMutex，
// 再并发调用 enableBackgroundTask——新实现会阻塞在取锁处，flag 保持 false；
// 旧实现会先置位 flag 再阻塞在注册处，flag 已为 true。50ms 的等待只是给
// goroutine 走到阻塞点的时间，断言本身不依赖时序。
// 释放锁后 enable 完成，查询必须立即看到已发布的会话。
func TestEnableBackgroundTaskPublishesAtomically(t *testing.T) {
	s := &appSession{connectionID: "ws-publish-race", backendGeneration: atomic.LoadUint64(&backendGeneration)}
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, ok := s.beginTask(cancel, "publish race task", "official", nil); !ok {
		t.Fatal("beginTask failed")
	}
	defer func() {
		s.endTask()
		clearCompletedBackgroundTaskSession()
	}()
	// 先持有全局互斥锁：模拟一次正在进行的查询/注册临界区。
	backgroundTaskMutex.Lock()
	done := make(chan struct{})
	go func() {
		s.enableBackgroundTask()
		close(done)
	}()
	// 给 goroutine 走到阻塞点的时间（新实现阻塞在 backgroundTaskMutex；
	// 旧实现先置位 flag 再阻塞在注册处）。
	time.Sleep(50 * time.Millisecond)
	s.taskMutex.Lock()
	flag := s.backgroundTask
	s.taskMutex.Unlock()
	backgroundTaskMutex.Unlock()
	if flag {
		t.Fatal("backgroundTask flag set before the session was published (race window: flag visible while query returns nil)")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("enableBackgroundTask did not return after the mutex was released")
	}
	if got := currentBackgroundTaskSession(); got != s {
		t.Fatalf("published session not visible to queries: got %v, want %p", got, s)
	}
}

// 第四阶段 B 批次修正（问题 3）：经 WS run_in_background 触发的后台任务发布，
// 必须对随后建立的新连接可见（新连接收到 found 而非 missing）。
// 依据：唯一的发布入口是 run_in_background 处理器（server.go），
// 它经 safeHandler 同步调用（仅 recover 包装，不开 goroutine），
// 因此发布总在旧连接读循环退出前完成；新连接的读循环又在 found/missing
// 发送后才启动，不可能在查询前发布；页面重连至少等待 500ms 退避，
// 远大于同步处理器完成的时间。CLI 从不发送 run_in_background，
// App.swift 也不自动转入后台。本测试用真实 WS 走完整路径：
// 在连接 A 上 start_task（beginTask 同步置位运行态，扫描在 goroutine 跑），
// 随即 run_in_background（同步发布），再拨新连接 B，断言 B 收到带相同 taskId 的 found。
// 扫描本身做真实网络 I/O，测试不等待它；结束后用 stop_task 取消，轮询到任务结束再清理全局会话。
func TestRunInBackgroundVisibleToNextConnection(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handleWebSocket)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"

	readUntil := func(c *websocket.Conn, want string, timeout time.Duration) map[string]interface{} {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for {
			_ = c.SetReadDeadline(deadline)
			_, raw, err := c.ReadMessage()
			if err != nil {
				t.Fatalf("read waiting for %s: %v", want, err)
			}
			var msg map[string]interface{}
			if err := json.Unmarshal(raw, &msg); err != nil {
				continue
			}
			typ, _ := msg["type"].(string)
			if typ == want {
				return msg
			}
		}
	}
	send := func(c *websocket.Conn, typ string, data map[string]interface{}) {
		t.Helper()
		raw, _ := json.Marshal(map[string]interface{}{"type": typ, "data": data})
		if err := c.WriteMessage(websocket.TextMessage, raw); err != nil {
			t.Fatalf("write %s: %v", typ, err)
		}
	}

	cA, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial A: %v", err)
	}
	defer cA.Close()
	readUntil(cA, "init_config", 10*time.Second)
	readUntil(cA, "background_task_missing", 5*time.Second)

	// 在 A 上启动任务：beginTask 同步完成，任务即进入运行态；随即转入后台。
	send(cA, "start_task", map[string]interface{}{"threads": 1, "port": 443, "delay": 0, "scanMode": "tcping", "ipType": 4})
	send(cA, "run_in_background", map[string]interface{}{})
	enabled := readUntil(cA, "background_task_enabled", 10*time.Second)
	enabledData, _ := enabled["data"].(map[string]interface{})
	wantID, _ := enabledData["taskId"].(string)
	if wantID == "" {
		t.Fatal("enabled snapshot has empty taskId")
	}
	// 失败路径的安全网：defer 在任何返回路径（含 Fatalf）都停止任务并清理全局会话。
	// defer 内不用 t.Fatalf（写失败时直接忽略，避免二次 Goexit 截断清理）。
	defer func() {
		raw, _ := json.Marshal(map[string]interface{}{"type": "stop_task", "data": map[string]interface{}{}})
		_ = cA.WriteMessage(websocket.TextMessage, raw)
		deadline := time.Now().Add(30 * time.Second)
		for {
			sess := currentBackgroundTaskSession()
			if sess == nil || !sess.backgroundSummary().Running {
				break
			}
			if time.Now().After(deadline) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		clearCompletedBackgroundTaskSession()
	}()

	// 新连接 B 必须看到已发布的后台任务（found），而非 missing。
	cB, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial B: %v", err)
	}
	defer cB.Close()
	readUntil(cB, "init_config", 10*time.Second)
	found := readUntil(cB, "background_task_found", 5*time.Second)
	foundData, _ := found["data"].(map[string]interface{})
	// R3 §10.1：found 的数据为 {snapshot, backendEpoch, taskVersion, ...}，
	// taskId 在 snapshot 内。
	foundSnap, _ := foundData["snapshot"].(map[string]interface{})
	gotID, _ := foundSnap["taskId"].(string)
	if gotID != wantID {
		t.Fatalf("new connection found taskId = %q, want %q (the WS-triggered publish must be visible)", gotID, wantID)
	}

	// 清理在 publish 成功后注册的 defer 中完成（含停止任务、轮询结束、清除全局会话）；
	// 此处只在 defer 执行前做一次同步等待，使断言时任务已结束。
	deadline := time.Now().Add(30 * time.Second)
	for {
		sess := currentBackgroundTaskSession()
		if sess == nil || !sess.backgroundSummary().Running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background task did not end after stop_task")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 第四阶段 B 批次修正（问题 R3）：读循环是单线程顺序分发，
// 一个被阻塞的 handler 会延迟其后所有消息的处理（即使后者不需要该锁）。
// 本测试用 backgroundTaskMutex 确定性地阻塞 run_in_background 的 enable，
// 证明随后发送的 heartbeat 在锁释放前收不到回复、释放后才收到。
// 结合代码审计（check_proxy_country 是唯一的长耗时同步 handler，
// 最长阻塞读循环 7 秒；而客户端重连退避首档仅 500ms），
// 这证明了 A/B 竞态的机制：A 的 run_in_background 尚未被处理时，
// 客户端可关闭 A 并建立 B，B 查询到 nil 并发送 missing，A 随后才完成发布。
// 该竞态不是“查询与发送之间的微秒间隙”，而是慢 handler 把发布推迟到了 B 的查询之后，
// 因此把 WS 写操作放进全局任务锁既不能修复它（发布在发送完成后才开始，
// 锁只串行化“已开始”的操作），又会损害可用性（写操作最长阻塞 10 秒写超时，
// 其间所有后台任务查询/发布都被阻塞）。
func TestReadLoopSequentialBlockedHandlerDelaysSubsequentMessages(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handleWebSocket)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"

	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	// 用一个 reader goroutine 把收到的消息类型发到 channel，测试用 select + time.After
	// 做超时判断，避免在连接上反复设置读 deadline（超时后再次读取会立即返回）。
	types := make(chan string, 64)
	go func() {
		for {
			_, raw, err := c.ReadMessage()
			if err != nil {
				close(types)
				return
			}
			var msg map[string]interface{}
			if err := json.Unmarshal(raw, &msg); err != nil {
				continue
			}
			if typ, ok := msg["type"].(string); ok {
				types <- typ
			}
		}
	}()
	waitFor := func(want string, timeout time.Duration) bool {
		t.Helper()
		deadline := time.After(timeout)
		for {
			select {
			case typ, ok := <-types:
				if !ok {
					return false
				}
				if typ == want {
					return true
				}
			case <-deadline:
				return false
			}
		}
	}
	send := func(typ string) {
		t.Helper()
		raw, _ := json.Marshal(map[string]interface{}{"type": typ, "data": map[string]interface{}{}})
		if err := c.WriteMessage(websocket.TextMessage, raw); err != nil {
			t.Fatalf("write %s: %v", typ, err)
		}
	}

	if !waitFor("init_config", 10*time.Second) {
		t.Fatal("init_config not received")
	}
	if !waitFor("background_task_missing", 5*time.Second) {
		t.Fatal("background_task_missing not received")
	}

	// 阻塞 enable：run_in_background 的 handler 会在 backgroundTaskMutex 上等待，
	// 读循环随之停住（它按顺序同步分发消息）。
	backgroundTaskMutex.Lock()
	released := false
	defer func() {
		if !released {
			backgroundTaskMutex.Unlock()
		}
	}()
	send("run_in_background")
	// heartbeat 不需要 backgroundTaskMutex；若读循环未被阻塞，它应立即回复。
	send("heartbeat")
	if waitFor("heartbeat_ack", 500*time.Millisecond) {
		t.Fatal("heartbeat_ack arrived while the read loop was blocked")
	}

	// 释放锁：被阻塞的 enable 完成（无运行任务，返回 false 并发 error），
	// 读循环继续，heartbeat_ack 随即到达。
	backgroundTaskMutex.Unlock()
	released = true
	if !waitFor("heartbeat_ack", 5*time.Second) {
		t.Fatal("heartbeat_ack did not arrive after release")
	}
}

// safeGo 传入 nil session 时：panic 被 recover 捕获，不解引用 nil，
// 不发生二次 panic，进程继续运行；正常函数同样能执行完。
func TestSafeGoNilSession(t *testing.T) {
	done := make(chan struct{})
	safeGo("test-nil-panic", nil, func() { panic("boom") })
	safeGo("test-nil-ok", nil, func() { close(done) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("safeGo with nil session did not run")
	}
}

// 多个连接的异步探测并发调用解析：全局默认值始终是合法候选之一，
// 且 -race 下无数据竞争；已取消的上下文不改变全局默认值。
func TestResolveStartupSpeedTestURLConcurrent(t *testing.T) {
	setAutoSpeedURLDefault(cloudflareSpeedURL)
	valid := map[string]bool{
		cloudflareSpeedURL:      true,
		cmSpeedURL:              true,
		mobileDedicatedSpeedURL: true,
		autoSpeedURLValue:       true,
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = resolveStartupSpeedTestURL(context.Background(), autoSpeedURLValue)
		}()
	}
	wg.Wait()
	if got := currentAutoSpeedURLDefault(); !valid[got] {
		t.Fatalf("concurrent resolve left invalid default %q", got)
	}

	// 已取消的上下文：探测直接失败，全局默认值保持调用前的值。
	setAutoSpeedURLDefault(cmSpeedURL)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := resolveStartupSpeedTestURL(ctx, autoSpeedURLValue); err == nil {
		t.Fatalf("cancelled context: expected error, got nil")
	}
	if got := currentAutoSpeedURLDefault(); got != cmSpeedURL {
		t.Fatalf("cancelled probe changed default to %q, want %q", got, cmSpeedURL)
	}
	setAutoSpeedURLDefault(cloudflareSpeedURL)
}

// ===== R3 (Phase 4 B S1/S5) 新增测试 =====
// 依据设计 v22.2 §16.2。全部测试串行执行（不使用 t.Parallel()），
// 每个测试以 resetBackendGeneration 隔离全局状态（见 §1.6 前置条件）。
// 对五张受保护 map 的并发读取一律按 §1.6 加锁快照规则：
// 取 backgroundTaskMutex → 锁内复制快照 → 立即释放锁 → 锁外断言与等待。

func resetTestHooks() {
	testHookSet(&testSlowHandlerGate, nil)
	testHookSet(&testRegisterGate, nil)
	testHookSet(&testRegisterAckGate, nil)
	testHookSet(&testRegisterReadGate, nil)
	testHookSet(&testReadDeadlineOverride, nil)
	testHookSet(&testWriteTimestampLog, nil)
	testHookSet(&testRecoverySendGate, nil)
	testHookSet(&testQueueLenGate, nil)
	testHookSet(&testPublishPanicGate, nil)
	testHookSet(&testPublishPanicGate2, nil)
	testHookSet(&testPublishDeferGate, nil)
	testHookSet(&testSetupGate, nil)
	testHookSet(&testSetupGateReached, nil)
	testHookSet(&testPublishLockGate, nil)
	testHookSet(&testPublishReturnGate, nil)
	testHookSet(&testPublishRecoverSendGate, nil)
	testHookSet(&testRollbackPanicGate, nil)
	testHookSet(&testReconcilePanicGate, nil)
	// S7 测试门与注入（Batch 1；发布屏障修正新增后三项）：生产声明、测试赋值；
	// 每测试结束时清零，防止门/注入泄漏到后续测试（门内阻塞的生产 goroutine
	// 由各测试在 cleanup 中关闭 release 释放，见各测试的 sync.Once 释放）。
	testHookSet(&testDisconnectArmGate, nil)
	testHookSet(&testDeferAEntryGate, nil)
	testHookSet(&testTimerCallbackGate, nil)
	testHookSet(&testTimerCallbackEntryGate, nil)
	testHookSet(&testTimerCallbackPhaseGate, nil)
	testHookSet(&testTimerRearmHook, nil)
	testHookSet(&testWatchdogSnapshotOverride, nil)
	testHookSet(&testWatchdogTickGate, nil)
	testHookSet(&testStopWatchdogGate, nil)
	testHookSet(&testTimerPublishGate, nil)
	testHookSet(&testTimerCallbackStartedHook, nil)
	testHookSet(&testTimerCallbackLoadedHook, nil)
	// S7 C1 测试门与 hook（认领协议）：生产声明、测试赋值；每测试结束时清零。
	testHookSet(&testBeginTaskCommitGate, nil)
	testHookSet(&testClaimEntryGate, nil)
	testHookSet(&testClaimCommitGate, nil)
	testHookSet(&testArmCountHook, nil)
	// S7 C1 O-1 测试门与 hook（task_started 身份快照）：生产声明、测试赋值；
	// 每测试结束时清零。
	testHookSet(&testTaskStartedGate, nil)
	testHookSet(&testTaskStartedDoneHook, nil)
	testHookSet(&testTaskStartedPayloadHook, nil)
	// S7 C1 O-1 第二轮测试门（taskStartedIdentityValid 竞态）。
	testHookSet(&testTaskStartedValidGate, nil)
	testHookSet(&testResetEpochGate, nil)
}

// r3Setup 隔离本测试的全局状态：先清钩子、再调 resetBackendGeneration
// （清空五张表与指针、递增代次、生成新 epoch）；结束时再清一次钩子。
func r3Setup(t *testing.T) {
	t.Helper()
	resetTestHooks()
	resetBackendGeneration()
	t.Cleanup(resetTestHooks)
}

// r3Server 启动真实 handleWebSocket 的 httptest 服务器。
func r3Server(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handleWebSocket)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
}

// r3Client 是带后台读 goroutine 的 WS 测试客户端。
type r3Client struct {
	t            *testing.T
	conn         *websocket.Conn
	rawConn      net.Conn // NetDial 捕获的底层 TCP 连接（RST 测试用）
	msgs         chan map[string]interface{}
	closed       chan struct{}
	initEpoch    string
	connectionID string
	pageId       string
}

func r3Dial(t *testing.T, wsURL string) *r3Client {
	t.Helper()
	var rawConn net.Conn
	dialer := websocket.Dialer{
		NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := net.Dial(network, addr)
			if err == nil {
				rawConn = c
			}
			return c, err
		},
	}
	c, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	cl := &r3Client{t: t, conn: c, rawConn: rawConn, msgs: make(chan map[string]interface{}, 256), closed: make(chan struct{})}
	t.Cleanup(func() { cl.Close() })
	go func() {
		defer close(cl.closed)
		for {
			_, raw, err := c.ReadMessage()
			if err != nil {
				return
			}
			var msg map[string]interface{}
			if err := json.Unmarshal(raw, &msg); err != nil {
				continue
			}
			cl.msgs <- msg
		}
	}()
	return cl
}

func (c *r3Client) Close() {
	_ = c.conn.Close()
}

// CloseWrite 只关闭写方向（发送 TCP FIN），保留读方向（半关闭）。
// 用于 T34/T35：让服务端读循环按序读完已缓冲的 P 再看到 EOF，
// 而不是让并发写失败路径抢先 Close 服务端 socket 导致 P 丢失
// （根因见 T34：handler/探测写失败 → target.Close() → 读循环
// 在读到 P 之前退出，任务被当作前台任务取消）。
func (c *r3Client) CloseWrite() {
	if tc, ok := c.rawConn.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
	} else {
		c.t.Fatalf("rawConn is not *net.TCPConn")
	}
}

// send 发送一条 WS 消息。
func (c *r3Client) send(typ string, data map[string]interface{}) {
	c.t.Helper()
	raw, _ := json.Marshal(map[string]interface{}{"type": typ, "data": data})
	if err := c.conn.WriteMessage(websocket.TextMessage, raw); err != nil {
		c.t.Fatalf("write %s: %v", typ, err)
	}
}

// waitFor 等待指定类型的下一条消息（跳过其他类型），超时即失败。
func (c *r3Client) waitFor(typ string, timeout time.Duration) map[string]interface{} {
	c.t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case msg, ok := <-c.msgs:
			if !ok {
				c.t.Fatalf("connection closed while waiting for %s", typ)
			}
			if t, _ := msg["type"].(string); t == typ {
				return msg
			}
		case <-deadline:
			c.t.Fatalf("timeout waiting for %s", typ)
		}
	}
}

// waitForTimeout 与 waitFor 相同，但超时返回 false 而不是失败（用于否定断言）。
func (c *r3Client) waitForTimeout(typ string, timeout time.Duration) (map[string]interface{}, bool) {
	deadline := time.After(timeout)
	for {
		select {
		case msg, ok := <-c.msgs:
			if !ok {
				return nil, false
			}
			if t, _ := msg["type"].(string); t == typ {
				return msg, true
			}
		case <-deadline:
			return nil, false
		}
	}
}

// waitForAll 等待一组类型的消息各至少一条（不要求顺序），超时即失败并
// 指明缺失的类型。用于探测消息：三条探测的到达顺序不确定，逐条 waitFor
// 会丢弃先到的其他探测消息（见 TestPingWriteFailureMarksCurrentConnClosed）。
func (c *r3Client) waitForAll(timeout time.Duration, types ...string) {
	c.t.Helper()
	pending := make(map[string]bool, len(types))
	for _, typ := range types {
		pending[typ] = true
	}
	deadline := time.After(timeout)
	for len(pending) > 0 {
		select {
		case msg, ok := <-c.msgs:
			if !ok {
				c.t.Fatalf("connection closed while waiting for %v", types)
			}
			if t, _ := msg["type"].(string); pending[t] {
				delete(pending, t)
			}
		case <-deadline:
			missing := make([]string, 0, len(pending))
			for typ := range pending {
				missing = append(missing, typ)
			}
			c.t.Fatalf("timeout waiting for probe messages, missing: %v", missing)
		}
	}
}

// waitForAny 等待多个类型中任意一个的下一条消息（跳过其他类型），返回
// 实际收到的类型与消息；超时即失败。用于分支判定：重查回复可能是 found
// 或 missing，调用方按实际类型分支断言（见 TestT36b_RSTExitBranch）。
func (c *r3Client) waitForAny(timeout time.Duration, types ...string) (string, map[string]interface{}) {
	c.t.Helper()
	want := make(map[string]bool, len(types))
	for _, typ := range types {
		want[typ] = true
	}
	deadline := time.After(timeout)
	for {
		select {
		case msg, ok := <-c.msgs:
			if !ok {
				c.t.Fatalf("connection closed while waiting for any of %v", types)
			}
			if t, _ := msg["type"].(string); want[t] {
				return t, msg
			}
		case <-deadline:
			c.t.Fatalf("timeout waiting for any of %v", types)
		}
	}
}

// waitForAbsent 断言在 duration 内收不到指定类型的消息（收到即失败；连接关闭则通过）。
func (c *r3Client) waitForAbsent(typ string, duration time.Duration) {
	c.t.Helper()
	deadline := time.After(duration)
	for {
		select {
		case msg, ok := <-c.msgs:
			if !ok {
				return
			}
			if t, _ := msg["type"].(string); t == typ {
				c.t.Fatalf("unexpectedly received %s: %v", typ, msg)
			}
		case <-deadline:
			return
		}
	}
}

// countType 在 duration 内统计收到指定类型消息的条数。
func (c *r3Client) countType(typ string, duration time.Duration) int {
	n := 0
	deadline := time.After(duration)
	for {
		select {
		case msg, ok := <-c.msgs:
			if !ok {
				return n
			}
			if t, _ := msg["type"].(string); t == typ {
				n++
			}
		case <-deadline:
			return n
		}
	}
}

// drainConnectTimeMissing 在 readInit 后排空连接时 missing：服务端在 init_config
// 后立即按指针状态发送连接时信号（指针为空时为 missing）；随后 recovery_query
// 的 missing 必须与它区分开，否则 waitFor 会先拿到连接时信号（无 outcomes）。
// 仅用于指针为空因而连接时信号必为 missing 的场景。
func (c *r3Client) drainConnectTimeMissing(timeout time.Duration) {
	c.t.Helper()
	_, _ = c.waitForTimeout("background_task_missing", timeout)
}

// readInit 等待 init_config，记录 epoch 与 connectionId。
func (c *r3Client) readInit(timeout time.Duration) {
	c.t.Helper()
	msg := c.waitFor("init_config", timeout)
	data, _ := msg["data"].(map[string]interface{})
	c.initEpoch, _ = data["backendEpoch"].(string)
	c.connectionID, _ = data["connectionId"].(string)
	if c.initEpoch == "" {
		c.t.Fatal("init_config missing backendEpoch (resetBackendGeneration not called?)")
	}
	if c.connectionID == "" {
		c.t.Fatal("init_config missing connectionId")
	}
}

// register 发送 register_page 并等待 ack，校验 pageId 与 epoch。
func (c *r3Client) register(pageId string) {
	c.t.Helper()
	c.pageId = pageId
	c.send("register_page", map[string]interface{}{"pageId": pageId})
	ack := c.waitFor("register_page_ack", 5*time.Second)
	data, _ := ack["data"].(map[string]interface{})
	if got, _ := data["pageId"].(string); got != pageId {
		c.t.Fatalf("ack pageId = %q, want %q", got, pageId)
	}
	if got, _ := data["backendEpoch"].(string); got != c.initEpoch {
		c.t.Fatalf("ack epoch = %q, want init_config epoch %q", got, c.initEpoch)
	}
}

// startRealTask 在本连接上启动一个真实任务（start_task），并轮询到服务端 session 进入运行态。
func (c *r3Client) startRealTask() *appSession {
	c.t.Helper()
	c.send("start_task", map[string]interface{}{"threads": 1, "port": 443, "delay": 0, "scanMode": "tcping", "ipType": 4})
	return waitSessionRunning(c.t, c.connectionID, 10*time.Second)
}

// recoveryQuery 发送 recovery_query。
func (c *r3Client) recoveryQuery(pageId string, unacked []string, recoveryId string, attempt int) {
	c.t.Helper()
	c.send("recovery_query", map[string]interface{}{"pageId": pageId, "unackedPublishIds": unacked, "recoveryId": recoveryId, "attempt": attempt})
}

// --- 加锁快照读取（§1.6 规则：取锁—复制—释放—锁外断言） ---

func snapshotLedger(t *testing.T, publishId string) (ledgerEntry, bool) {
	t.Helper()
	backgroundTaskMutex.Lock()
	e, ok := publishLedger[publishId]
	backgroundTaskMutex.Unlock()
	return e, ok
}

func snapshotLoopState(t *testing.T, connID string) (loopEntry, bool) {
	t.Helper()
	backgroundTaskMutex.Lock()
	e, ok := connLoopState[connID]
	backgroundTaskMutex.Unlock()
	return e, ok
}

func snapshotPending(t *testing.T, recoveryId string) (exists bool, waitFor []string, serverDeadline time.Time) {
	t.Helper()
	backgroundTaskMutex.Lock()
	defer backgroundTaskMutex.Unlock()
	p, ok := pendingRecoveries[recoveryId]
	if !ok {
		return false, nil, time.Time{}
	}
	cp := append([]string(nil), p.waitForPublishIds...)
	return true, cp, p.serverDeadline
}

func snapshotResolved(t *testing.T, recoveryId string) (resolvedEntry, bool) {
	t.Helper()
	backgroundTaskMutex.Lock()
	e, ok := resolvedRecoveries[recoveryId]
	backgroundTaskMutex.Unlock()
	return e, ok
}

func snapshotServerSession(t *testing.T, connID string) *appSession {
	t.Helper()
	backgroundTaskMutex.Lock()
	defer backgroundTaskMutex.Unlock()
	if e, ok := connLoopState[connID]; ok {
		return e.session
	}
	return nil
}

func snapshotLedgerRefcount(t *testing.T, publishId string) (int, bool) {
	t.Helper()
	e, ok := snapshotLedger(t, publishId)
	if !ok {
		return 0, false
	}
	return e.refcount, true
}

// pollUntil 每 interval 轮询 cond（锁外等待），超时即失败。
func pollUntil(t *testing.T, what string, timeout, interval time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(interval)
	}
}

// pollLedgerState 轮询 ledger[pid].state == want（§1.6 加锁快照规则）。
func pollLedgerState(t *testing.T, publishId, want string, timeout time.Duration) {
	t.Helper()
	pollUntil(t, "ledger["+publishId+"].state=="+want, timeout, 50*time.Millisecond, func() bool {
		e, ok := snapshotLedger(t, publishId)
		return ok && e.state == want
	})
}

// pollLoopExited 轮询 connLoopState[connID].state == exited。
func pollLoopExited(t *testing.T, connID string, timeout time.Duration) {
	t.Helper()
	pollUntil(t, "connLoopState["+connID+"]==exited", timeout, 50*time.Millisecond, func() bool {
		e, ok := snapshotLoopState(t, connID)
		return ok && e.state == loopStateExited
	})
}

// waitSessionRunning 轮询服务端 session 进入运行态，返回该 session。
func waitSessionRunning(t *testing.T, connID string, timeout time.Duration) *appSession {
	t.Helper()
	var sess *appSession
	pollUntil(t, "session "+connID+" running", timeout, 50*time.Millisecond, func() bool {
		sess = snapshotServerSession(t, connID)
		if sess == nil {
			return false
		}
		sess.taskMutex.Lock()
		defer sess.taskMutex.Unlock()
		return sess.isTaskRunning
	})
	return sess
}

// waitSessionStopped 轮询服务端 session 退出运行态。
func waitSessionStopped(t *testing.T, sess *appSession, timeout time.Duration) {
	t.Helper()
	pollUntil(t, "session stopped", timeout, 100*time.Millisecond, func() bool {
		sess.taskMutex.Lock()
		defer sess.taskMutex.Unlock()
		return !sess.isTaskRunning
	})
}

// r3RunningSession 创建一个带运行任务的测试 session（无网络 I/O，供直接调用测试用）。
func r3RunningSession(t *testing.T, connID string) (*appSession, context.CancelFunc) {
	t.Helper()
	s := &appSession{connectionID: connID, backendGeneration: atomic.LoadUint64(&backendGeneration)}
	_, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		s.endTask()
		clearCompletedBackgroundTaskSession()
	})
	if _, ok := s.beginTask(cancel, "r3 test task", "official", nil); !ok {
		t.Fatal("beginTask failed")
	}
	return s, cancel
}

// msgData 取消息 data 字段的 map 形式。
func msgData(msg map[string]interface{}) map[string]interface{} {
	data, _ := msg["data"].(map[string]interface{})
	return data
}

// outcomesOf 取 found/missing/pending 信号的 publishOutcomes 列表。
func outcomesOf(msg map[string]interface{}) []map[string]interface{} {
	data := msgData(msg)
	raw, _ := data["publishOutcomes"].([]interface{})
	out := make([]map[string]interface{}, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]interface{}); ok {
			out = append(out, m)
		}
	}
	return out
}

// outcomeOf 找 publishOutcomes 中指定 publishId 的结局。
func outcomeOf(msg map[string]interface{}, publishId string) (string, bool) {
	for _, o := range outcomesOf(msg) {
		if id, _ := o["publishId"].(string); id == publishId {
			outcome, _ := o["outcome"].(string)
			return outcome, true
		}
	}
	return "", false
}

// --- T32/T33 ---

// T32——pageId 注册先于慢 handler 的正常路径（后端部分）：
// init_config → register_page → register_page_ack（ack 的 epoch 与 init_config 一致），
// pageSessions 含该 session；connect-time 信号在 ack 前到达时被暂存（前端行为，
// 由 Node harness test_register_gate.js 覆盖）。
func TestT32_RegisterPageOrder(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	c := r3Dial(t, wsURL)
	c.readInit(10 * time.Second)
	// connect-time 信号（无任务时为 missing）在 ack 前到达。
	c.waitFor("background_task_missing", 5*time.Second)
	c.register("test-t32-page")
	// pageSessions 含该 session。
	backgroundTaskMutex.Lock()
	sessions, ok := pageSessions["test-t32-page"]
	backgroundTaskMutex.Unlock()
	if !ok || len(sessions) == 0 {
		t.Fatalf("pageSessions missing test-t32-page after register")
	}
	// 注册后业务消息正常响应。
	c.send("heartbeat", map[string]interface{}{})
	c.waitFor("heartbeat_ack", 5*time.Second)
}

// T33——注册尚未确认时 ack 被延迟（后端部分）：
// testRegisterAckGate 延迟 ack 发送；释放后 ack 到达，epoch 与 init_config 一致。
func TestT33_RegisterAckGateDelaysAck(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	testHookSet(&testRegisterAckGate, make(chan struct{}))
	defer func() { testHookSet(&testRegisterAckGate, nil) }()
	c := r3Dial(t, wsURL)
	c.readInit(10 * time.Second)
	c.send("register_page", map[string]interface{}{"pageId": "test-t33-page"})
	if _, ok := c.waitForTimeout("register_page_ack", 500*time.Millisecond); ok {
		t.Fatal("register_page_ack arrived while testRegisterAckGate was armed")
	}
	close(testHookGet(&testRegisterAckGate))
	ack := c.waitFor("register_page_ack", 5*time.Second)
	data := msgData(ack)
	if got, _ := data["backendEpoch"].(string); got != c.initEpoch {
		t.Fatalf("ack epoch = %q, want %q", got, c.initEpoch)
	}
}

// T34——A 慢 handler 阻塞、P 尚未进入 handler、B 重连查询时必须得到 pending。
func TestT34_SlowHandlerPending(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t34-page"
	// A 注册并启动任务。
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.waitFor("background_task_missing", 5*time.Second)
	cA.register(pageId)
	cA.startRealTask()
	// 阻塞 check_proxy_country handler。
	testHookSet(&testSlowHandlerGate, make(chan struct{}))
	defer func() { testHookSet(&testSlowHandlerGate, nil) }()
	cA.send("check_proxy_country", map[string]interface{}{})
	// P 在 ack 后发出，进入 A 的 TCP 缓冲，未进入 handler。
	publishId := "pub-" + pageId + "-1"
	cA.send("run_in_background", map[string]interface{}{"publishId": publishId, "pageId": pageId})
	// 确认 P 尚未进入 ledger（handler 仍阻塞）。
	time.Sleep(200 * time.Millisecond)
	if _, ok := snapshotLedger(t, publishId); ok {
		t.Fatal("ledger already has P while slow handler is blocked")
	}
	// 全关闭 A（close 帧 + TCP 关闭，即真实浏览器断开路径），重连为 B
	// （同一 pageId），B 注册后查询。选项 A 已修复生产代码：写失败
	// 只置 wsClosed、不 Close 服务端 socket，读循环按序读完已缓冲的 P
	// 再退出，因此全关闭下 T34 仍应通过（见 T34 根因与选项 A 决定）。
	cA.Close()
	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.waitFor("background_task_missing", 5*time.Second)
	cB.register(pageId)
	recoveryId := "rec-" + pageId + "-2-0"
	cB.recoveryQuery(pageId, []string{publishId}, recoveryId, 0)
	pending := cB.waitFor("background_task_pending", 5*time.Second)
	if outcome, ok := outcomeOf(pending, publishId); !ok || outcome != "pending" {
		t.Fatalf("pending publishOutcomes for P = %q (present=%v), want pending", outcome, ok)
	}
	if exists, _, _ := snapshotPending(t, recoveryId); !exists {
		t.Fatal("pendingRecoveries missing B's recoveryId")
	}
	// 释放 gate：A 的 handler 返回 → 读循环实际处理 P → 发布 → B 得 found。
	close(testHookGet(&testSlowHandlerGate))
	testHookSet(&testSlowHandlerGate, nil)
	// 显式确认读循环已处理 P（ledger 已发布），再等 B 的 found。
	pollUntil(t, "read loop processes P after gate release", 10*time.Second, 50*time.Millisecond, func() bool {
		e, ok := snapshotLedger(t, publishId)
		return ok && e.state == ledgerStatePublished
	})
	found := cB.waitFor("background_task_found", 15*time.Second)
	if outcome, ok := outcomeOf(found, publishId); !ok || outcome != "published" {
		t.Fatalf("found publishOutcomes for P = %q (present=%v), want published", outcome, ok)
	}
	// 任务未被意外取消：发布后仍在运行。
	sessA := snapshotServerSession(t, cA.connectionID)
	if sessA == nil {
		t.Fatal("no server session for A")
	}
	sessA.taskMutex.Lock()
	running := sessA.isTaskRunning
	sessA.taskMutex.Unlock()
	if !running {
		t.Fatal("A's background task stopped after publish, want it to keep running")
	}
	if exists, _, _ := snapshotPending(t, recoveryId); exists {
		t.Fatal("pending still present after found")
	}
	// P 已发布、任务已转后台，此时再全关闭 A：读循环退出时不再取消任务。
	cA.Close()
}

// TestPingWriteFailureMarksCurrentConnClosed：ping 写失败后，连接身份仍为
// 当前连接时置 wsClosed=true（第二轮补齐任务 1，修订 §5.6）；
// 置位后后续发送被拦截、ping 停止；不 Close（选项 A）。
// 归因控制：先等三条异步探测写全部完成，此后本连接上唯一的写者是
// ping goroutine；handler 被 gate 阻塞，读循环无法先行观测断开。
func TestPingWriteFailureMarksCurrentConnClosed(t *testing.T) {
	r3Setup(t)
	setTestPingInterval(t, 100*time.Millisecond)
	_, wsURL := r3Server(t)
	pageId := "test-ping-fail-page"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	waitFirstPingTick(t, cA.connectionID, 10*time.Second)
	cA.waitFor("background_task_missing", 5*time.Second)
	// 三条探测到达顺序不确定，用 waitForAll 一次等齐（逐条 waitFor 会丢弃
	// 先到的其他探测消息，导致后一条等满超时）。
	cA.waitForAll(20*time.Second, "proxy_country_result", "speed_url_status", "version_info")
	cA.register(pageId)
	// 稳态基线：连接已建立、探测 HTTP 连接池已预热后再计数——
	// 探测用的共享 http.Transport 空闲 keep-alive goroutine（http2/
	// persistConn 读写循环）会合法存活到空闲超时，不是泄漏；
	// 基线须包含它们（同 T42 的做法），只断言本连接的 goroutine 退出。
	time.Sleep(100 * time.Millisecond)
	baseGoroutines := runtime.NumGoroutine()
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	// 阻塞 check_proxy_country handler：读循环停在 handler 内，
	// 既不能读到 RST 而退出，也不会有 handler 响应写。
	testHookSet(&testSlowHandlerGate, make(chan struct{}))
	defer func() { testHookSet(&testSlowHandlerGate, nil) }()
	cA.send("check_proxy_country", map[string]interface{}{})
	// RST 断开：服务端下一次 ping 写必失败（ECONNRESET）。
	cA.CloseRST()
	// 轮询到 ping 写失败置 wsClosed（tick 100ms，预算 10s）。
	pollUntil(t, "ping write failure marks wsClosed", 10*time.Second, 20*time.Millisecond, func() bool {
		_, closed := sess.wsSnapshot()
		return closed
	})
	// 置位后后续发送被拦截：定向发送返回 false。
	conn, _ := sess.wsSnapshot()
	if conn == nil {
		t.Fatal("session ws is nil after RST")
	}
	if sess.sendWSMessageToConn(conn, "heartbeat_ack", map[string]interface{}{"t": 0}) {
		t.Fatal("sendWSMessageToConn succeeded after ping marked wsClosed, want false")
	}
	// 再等 5 个 tick：标记保持，不被清除、不复活 ping。
	time.Sleep(500 * time.Millisecond)
	if _, closed := sess.wsSnapshot(); !closed {
		t.Fatal("wsClosed cleared after ping failure, want it to stay true")
	}
	// 释放 gate：读循环读到 RST 错误后退出（选项 A：socket 由 Defer C 关闭）。
	close(testHookGet(&testSlowHandlerGate))
	testHookSet(&testSlowHandlerGate, nil)
	pollLoopExited(t, cA.connectionID, 10*time.Second)
	time.Sleep(500 * time.Millisecond)
	if got := runtime.NumGoroutine(); got > baseGoroutines+2 {
		t.Fatalf("goroutine count = %d, baseline %d (leak > 2)", got, baseGoroutines)
	}
}

// TestPingWriteFailureSkipsReboundConn：旧连接 ping 写失败时，session 已
// 换绑到新连接（follow），不得把旧连接的失败写入新连接（任务 1）。
// 断言：backgroundSession.wsClosed 保持 false；B 仍能正常收发；
// A 的读循环退出时也不误置（§5.7 同一规则）。
// 本测试在“无条件置位”的朴素修法下失败（阴性对照，见交付报告），
// 在身份重查修法下通过。
func TestPingWriteFailureSkipsReboundConn(t *testing.T) {
	r3Setup(t)
	setTestPingInterval(t, 100*time.Millisecond)
	_, wsURL := r3Server(t)
	pageId := "test-ping-rebind-page"
	pubId := "pub-" + pageId + "-1"

	// A 注册、启动任务、发布到后台。
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	waitFirstPingTick(t, cA.connectionID, 10*time.Second)
	cA.waitFor("background_task_missing", 5*time.Second)
	cA.register(pageId)
	cA.startRealTask()
	cA.send("run_in_background", map[string]interface{}{"publishId": pubId, "pageId": pageId})
	cA.waitFor("background_task_enabled", 10*time.Second)
	// 阻塞 A 的 handler：A 的读循环停在 handler 内，无法先行观测断开。
	testHookSet(&testSlowHandlerGate, make(chan struct{}))
	defer func() { testHookSet(&testSlowHandlerGate, nil) }()
	cA.send("check_proxy_country", map[string]interface{}{})
	// B 跟随：backgroundSession（A 的 session）换绑到 B 的连接。
	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	// 首 tick 观测须在 follow 之前：follow 会把本连接的局部 session
	// 重定向为 backgroundSession（server.go:802），此后 pong 记在
	// backgroundSession 上，snapshotServerSession(cB) 不再看到它。
	waitFirstPingTick(t, cB.connectionID, 10*time.Second)
	cB.register(pageId)
	cB.send("follow_background_task", map[string]interface{}{})
	cB.waitFor("background_task_following", 10*time.Second)
	bgSess := snapshotServerSession(t, cA.connectionID)
	if bgSess == nil {
		t.Fatal("no server session for A (backgroundSession)")
	}
	if _, closed := bgSess.wsSnapshot(); closed {
		t.Fatal("backgroundSession wsClosed already true before RST")
	}
	// RST 断开 A 的旧连接：A 帧的 ping 下一次写必失败，但 session.ws
	// 已是 B 的连接，身份重查必须跳过置位。
	cA.CloseRST()
	// 等 5 个 tick（ping 失败后 goroutine 即返回；若误置会在此窗口内发生）。
	time.Sleep(500 * time.Millisecond)
	if _, closed := bgSess.wsSnapshot(); closed {
		t.Fatal("ping failure on old conn marked wsClosed on rebound session, want false")
	}
	// 新连接仍能正常收发：B 查询后台任务状态得 update。
	cB.send("get_background_task_status", map[string]interface{}{})
	upd := cB.waitFor("background_task_update", 10*time.Second)
	if snap, _ := msgData(upd)["snapshot"].(map[string]interface{}); snap == nil {
		t.Fatalf("background_task_update missing snapshot: %v", msgData(upd))
	}
	// 释放 gate：A 的读循环读到 RST 错误后退出；退出分支同样不得误置
	// 已换绑 session 的 wsClosed（§5.7）。
	close(testHookGet(&testSlowHandlerGate))
	testHookSet(&testSlowHandlerGate, nil)
	pollLoopExited(t, cA.connectionID, 10*time.Second)
	if _, closed := bgSess.wsSnapshot(); closed {
		t.Fatal("read-loop exit marked wsClosed on rebound session, want false")
	}
	// B 仍能正常收发。
	cB.send("get_background_task_status", map[string]interface{}{})
	cB.waitFor("background_task_update", 10*time.Second)
	// 收尾：关闭 B 并等其服务端循环退出，避免 cleanup 恢复写与 B 的
	// ping goroutine 启动读竞争（-race）。关 B 不会武装断线计时器：
	// cB 自身的连接 session 不持有任务，armDisconnectTimer 的状态检查
	// （session.go:1079）提前返回；故无须先停任务。
	cB.Close()
	pollLoopExited(t, cB.connectionID, 10*time.Second)
}

// TestSweeperLifecycle：stateSweeper 的启动/停止/重启唯一性与 tick→扫描
// 接线（第二轮补齐任务 5，§2.10）。
// 断言：启动后 sweeperLive==1；重复启动被拒绝（Live 恒为 1）；短周期
// tick 实际触发扫描（孤儿 dispatched → failed，等待者得 missing）；
// 停止后 Live==0 且 done 关闭；重启后 Live 回 1；代次重置后新代次的
// 活条目不被旧 sweeper 清理、过期条目仍按 TTL 清理。
func TestSweeperLifecycle(t *testing.T) {
	r3Setup(t)
	if got := sweeperLive.Load(); got != 0 {
		t.Fatalf("sweeperLive = %d at test start, want 0", got)
	}
	origTick := sweeperTickInterval
	sweeperTickInterval = 100 * time.Millisecond
	t.Cleanup(func() { sweeperTickInterval = origTick })

	// --- 启动 1 + 重复启动拒绝 ---
	stop1, done1, started1 := startStateSweeper()
	if !started1 {
		t.Fatal("first startStateSweeper returned started=false")
	}
	if got := sweeperLive.Load(); got != 1 {
		t.Fatalf("sweeperLive = %d after first start, want 1", got)
	}
	if _, _, startedAgain := startStateSweeper(); startedAgain {
		t.Fatal("duplicate startStateSweeper returned started=true, want false")
	}
	if got := sweeperLive.Load(); got != 1 {
		t.Fatalf("sweeperLive = %d after duplicate start attempt, want 1", got)
	}

	// --- tick 实际触发扫描：真实 pending + 直接种孤儿 dispatched ---
	_, wsURL := r3Server(t)
	pageId := "test-sweeper-page"
	pubId := "pub-test-sweeper-page-1"
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.drainConnectTimeMissing(500 * time.Millisecond)
	cB.register(pageId)
	cB.recoveryQuery(pageId, []string{pubId}, "rec-sweeper", 0)
	cB.waitFor("background_task_pending", 10*time.Second)
	sessA := snapshotServerSession(t, cA.connectionID)
	if sessA == nil {
		t.Fatal("no server session for A")
	}
	backgroundTaskMutex.Lock()
	publishLedger[pubId] = ledgerEntry{
		state: ledgerStateDispatched, pageId: pageId, ownerSession: sessA,
		backendEpoch: currentBackendEpoch(),
		generation:   atomic.LoadUint64(&backendGeneration),
		createdAt:    time.Now().Add(-61 * time.Second),
		refcount:     1,
	}
	backgroundTaskMutex.Unlock()
	// 一个 tick（100ms）后孤儿应转为 failed，等待者收到 missing(publish-failed)。
	pollUntil(t, "sweeper tick converts orphan to failed", 10*time.Second, 50*time.Millisecond, func() bool {
		e, ok := snapshotLedger(t, pubId)
		return ok && e.state == ledgerStateFailed
	})
	miss := cB.waitFor("background_task_missing", 10*time.Second)
	if reason, _ := msgData(miss)["reason"].(string); reason != "publish-failed" {
		t.Fatalf("waiter missing reason = %q, want publish-failed", reason)
	}
	if out, ok := outcomeOf(miss, pubId); !ok || out != "failed" {
		t.Fatalf("waiter missing outcome = %q, want failed: %v", out, outcomesOf(miss))
	}
	if n, ok := snapshotLedgerRefcount(t, pubId); !ok || n != 0 {
		t.Fatalf("ledger[%s].refcount = %d, want 0 after sweep", pubId, n)
	}

	// --- 停止 1 ---
	close(stop1)
	select {
	case <-done1:
	case <-time.After(5 * time.Second):
		t.Fatal("first sweeper did not exit within 5s of stop")
	}
	if got := sweeperLive.Load(); got != 0 {
		t.Fatalf("sweeperLive = %d after stop, want 0", got)
	}

	// --- 重启 + 跨代次隔离 ---
	stop2, done2, started2 := startStateSweeper()
	if !started2 {
		t.Fatal("restart startStateSweeper returned started=false")
	}
	if got := sweeperLive.Load(); got != 1 {
		t.Fatalf("sweeperLive = %d after restart, want 1", got)
	}
	// 代次重置：旧 sweeper 仍在运行（reset 不停 sweeper，停它的是 Main() 的
	// defer）；新代次的活条目不得被清理，过期条目仍按 TTL 清理。
	// 注意：reset 步骤 3a 会关闭 A/B 的旧连接，本测试此后不再用它们发消息。
	resetBackendGeneration()
	gen := atomic.LoadUint64(&backendGeneration)
	backgroundTaskMutex.Lock()
	resolvedRecoveries["rec-sweeper-live"] = resolvedEntry{resolvedAt: time.Now(), epoch: currentBackendEpoch(), generation: gen}
	resolvedRecoveries["rec-sweeper-expired"] = resolvedEntry{resolvedAt: time.Now().Add(-6 * time.Minute), epoch: currentBackendEpoch(), generation: gen}
	backgroundTaskMutex.Unlock()
	pollUntil(t, "sweeper tick removes expired new-gen entry", 10*time.Second, 50*time.Millisecond, func() bool {
		_, ok := snapshotResolved(t, "rec-sweeper-expired")
		return !ok
	})
	if _, ok := snapshotResolved(t, "rec-sweeper-live"); !ok {
		t.Fatal("live new-gen resolved entry was deleted by the sweeper, want it kept")
	}
	// 停止 2。
	close(stop2)
	select {
	case <-done2:
	case <-time.After(5 * time.Second):
		t.Fatal("restarted sweeper did not exit within 5s of stop")
	}
	if got := sweeperLive.Load(); got != 0 {
		t.Fatalf("sweeperLive = %d after second stop, want 0", got)
	}
}

// TestRuntimeCancelLatency 实测 resetBackendGeneration 取消孤儿任务的
// 真实退出延迟（见简报 §6）：源码上 ctx.Err()/DialContext/
// NewRequestWithContext 的存在 ≠ 已验证的取消延迟，必须实测。
// 两种任务类型：
// (a) 真实 WS 扫描任务（start_task/tcping，含真实网络 I/O）；
// (b) 直接 ctx 绑定任务（goroutine 只等 ctx.Done()，隔离网络因素，
// session 需先登记进 connLoopState，否则 reset 不把它当孤儿收集）。
// 断言：取消延迟 < 10s（任务逐项上限 10s、拨号 5s，取消路径应远快于
// 逐项超时）；实测值见 t.Logf 输出，写入交付报告。
func TestRuntimeCancelLatency(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)

	// (a) 真实 WS 扫描任务。
	pageId := "test-cancel-page"
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.waitFor("background_task_missing", 5*time.Second)
	cA.register(pageId)
	sess := cA.startRealTask()
	oldGen := sess.taskGeneration
	t0 := time.Now()
	resetBackendGeneration()
	resetTook := time.Since(t0)
	// 轮询到任务真正退出（isTaskRunning == false），计时含 reset 调用本身。
	// 用 5ms 细轮询而不用 waitSessionStopped（100ms 间隔会把测量值
	// 抬到 100ms 量级，掩盖真实延迟）。
	{
		deadline := time.Now().Add(30 * time.Second)
		for {
			sess.taskMutex.Lock()
			stopped := !sess.isTaskRunning
			sess.taskMutex.Unlock()
			if stopped {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("timeout waiting for ws-scan session to stop")
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	latency := time.Since(t0)
	t.Logf("cancel-latency ws-scan: reset-call=%v cancel-latency=%v", resetTook, latency)
	if latency > 10*time.Second {
		t.Fatalf("ws-scan cancel latency %v exceeds 10s", latency)
	}
	// 旧代次计数已释放：endTask 时 gen < current，条目删除。
	globalTaskMutex.Lock()
	_, stillCounted := runningTasksByGeneration[oldGen]
	globalTaskMutex.Unlock()
	if stillCounted {
		t.Fatalf("old generation %d still counted after task end", oldGen)
	}
	// reset 步骤 3a 已关闭 A 的旧连接；客户端读到关闭即证明关闭已送达。
	// （注意：reset 步骤 2c 已重建 connLoopState 为空表，A 的条目直接
	// 消失而非 exited，故此处不断言 loopState。）
	select {
	case <-cA.closed:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for client A to observe server close")
	}
	cA.Close()

	// (b) 直接 ctx 绑定任务（无网络 I/O）。
	s2 := &appSession{connectionID: "test-cancel-direct", backendGeneration: atomic.LoadUint64(&backendGeneration)}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); s2.endTask() }) // endTask 幂等：已结束时不再递减。
	if _, ok := s2.beginTask(cancel, "cancel latency direct", "official", nil); !ok {
		t.Fatal("beginTask failed for direct task")
	}
	// 登记进 connLoopState（ws 为 nil，reset 收集孤儿时跳过关 socket，
	// 只收 taskCancel；见 resetBackendGeneration 步骤 2a）。
	backgroundTaskMutex.Lock()
	connLoopState[s2.connectionID] = loopEntry{state: loopStateIdle, session: s2, epoch: currentBackendEpoch(), generation: s2.backendGeneration}
	backgroundTaskMutex.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		s2.endTask()
	}()
	oldGen2 := s2.taskGeneration
	t1 := time.Now()
	resetBackendGeneration()
	resetTook2 := time.Since(t1)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for direct task to end after reset")
	}
	latency2 := time.Since(t1)
	t.Logf("cancel-latency direct-ctx: reset-call=%v cancel-latency=%v", resetTook2, latency2)
	if latency2 > 10*time.Second {
		t.Fatalf("direct-ctx cancel latency %v exceeds 10s", latency2)
	}
	globalTaskMutex.Lock()
	_, stillCounted2 := runningTasksByGeneration[oldGen2]
	globalTaskMutex.Unlock()
	if stillCounted2 {
		t.Fatalf("old generation %d still counted after direct task end", oldGen2)
	}
}

// ---------------------------------------------------------------------------
// T35：已发布幂等与连接隔离（见 §7.6/§10.6/§16.2）。
// ---------------------------------------------------------------------------

// snapshotBackgroundSessionWS 返回当前 backgroundTaskSession 的 ws（加锁快照）。
func snapshotBackgroundSessionWS(t *testing.T) *websocket.Conn {
	t.Helper()
	backgroundTaskMutex.Lock()
	bs := backgroundTaskSession
	backgroundTaskMutex.Unlock()
	if bs == nil {
		return nil
	}
	conn, _ := bs.wsSnapshot()
	return conn
}

// waitForClose 等待客户端观察到服务端关闭（测试本身绝不主动关闭该连接）。
func waitForClose(t *testing.T, c *r3Client, timeout time.Duration) {
	t.Helper()
	select {
	case <-c.closed:
	case <-time.After(timeout):
		t.Fatalf("client %s did not observe server close within %v", c.connectionID, timeout)
	}
}

// dialBlockedAcrossReset 在 testSetupGate 处阻塞一个连接的设立，跨越一次
// resetBackendGeneration，再释放 gate；返回该旧代次连接（已读 init_config）。
// 调用方随后可再建新代次连接对照。gate 的清理由 resetTestHooks 兜底。
func dialBlockedAcrossReset(t *testing.T, wsURL string) *r3Client {
	t.Helper()
	testHookSet(&testSetupGate, make(chan struct{}))
	testHookSet(&testSetupGateReached, make(chan struct{}, 1))
	c := r3Dial(t, wsURL)
	// 等服务端 goroutine 抵达 gate（session 已创建）再 reset：r3Dial 返回
	// 只保证客户端握手完成，服务端 session 可能尚未创建；若先 reset，session
	// 会带着新代次出生，代次隔离的前提即失效（-race 下已偶发 backend_restarted 超时）。
	select {
	case <-testHookGet(&testSetupGateReached):
	case <-time.After(10 * time.Second):
		t.Fatal("server did not reach testSetupGate")
	}
	resetBackendGeneration()
	close(testHookGet(&testSetupGate))
	testHookSet(&testSetupGate, nil)
	testHookSet(&testSetupGateReached, nil)
	c.readInit(10 * time.Second)
	// 守卫：session 的代次必须严格小于当前代次（旧代次），否则测试前提已失效。
	if sess := snapshotServerSession(t, c.connectionID); sess.backendGeneration == atomic.LoadUint64(&backendGeneration) {
		t.Fatalf("session generation %d == current generation, want old generation", sess.backendGeneration)
	}
	return c
}

// startPonger 启动一个 1 秒间隔的显式 Pong 发送 goroutine（用于短 ReadDeadline
// 下保持连接存活，见 T37/T41）；返回 stop 函数，测试结束前调用。
func startPonger(t *testing.T, c *r3Client) func() {
	t.Helper()
	stop := make(chan struct{})
	var once sync.Once
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if err := c.conn.WriteControl(websocket.PongMessage, nil, time.Now().Add(2*time.Second)); err != nil {
					return
				}
			}
		}
	}()
	return func() { once.Do(func() { close(stop) }) }
}

// enabledFields 取 background_task_enabled 响应 data 中的关键字段。
func enabledFields(msg map[string]interface{}) (publishId, taskId, epoch string, version float64, snapTaskId, snapEpoch string, snapVersion float64) {
	data := msgData(msg)
	publishId, _ = data["publishId"].(string)
	taskId, _ = data["taskId"].(string)
	epoch, _ = data["backendEpoch"].(string)
	version, _ = data["taskVersion"].(float64)
	snap, _ := data["snapshot"].(map[string]interface{})
	snapTaskId, _ = snap["taskId"].(string)
	snapEpoch, _ = snap["backendEpoch"].(string)
	snapVersion, _ = snap["taskVersion"].(float64)
	return
}

// TestT35_FoundAndIdempotentResend：B 先查询 P（pending），A 发布 P 后 B 得
// found；同一连接再次发送 P → already-published，A 收到字段逐项相等的
// background_task_enabled，taskIdentityVersion 不变，B 不再收到重复 found。
func TestT35_FoundAndIdempotentResend(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t35-resend-page"
	pubId := "pub-test-t35-resend-page-1"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cA.startRealTask()

	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register(pageId)
	cB.recoveryQuery(pageId, []string{pubId}, "rec-t35-resend", 0)
	pend := cB.waitFor("background_task_pending", 10*time.Second)
	if _, ok := outcomeOf(pend, pubId); !ok {
		t.Fatalf("pending outcomes missing %s: %v", pubId, outcomesOf(pend))
	}

	// A 发布 P：A 得 enabled #1，B 得 found（决议完成）。
	cA.send("run_in_background", map[string]interface{}{"publishId": pubId, "pageId": pageId})
	en1 := cA.waitFor("background_task_enabled", 10*time.Second)
	found := cB.waitFor("background_task_found", 10*time.Second)
	if _, ok := outcomeOf(found, pubId); !ok {
		t.Fatalf("found outcomes missing %s: %v", pubId, outcomesOf(found))
	}
	backgroundTaskMutex.Lock()
	verBefore := taskIdentityVersion
	backgroundTaskMutex.Unlock()

	// 同一连接再次发送 P。
	cA.send("run_in_background", map[string]interface{}{"publishId": pubId, "pageId": pageId})
	en2 := cA.waitFor("background_task_enabled", 10*time.Second)
	p1, t1, e1, v1, st1, se1, sv1 := enabledFields(en1)
	p2, t2, e2, v2, st2, se2, sv2 := enabledFields(en2)
	if p1 != p2 || t1 != t2 || e1 != e2 || v1 != v2 || st1 != st2 || se1 != se2 || sv1 != sv2 {
		t.Fatalf("already-published enabled differs:\n#1 %v\n#2 %v", msgData(en1), msgData(en2))
	}
	backgroundTaskMutex.Lock()
	verAfter := taskIdentityVersion
	backgroundTaskMutex.Unlock()
	if verBefore != verAfter {
		t.Fatalf("taskIdentityVersion changed on replay: %d -> %d", verBefore, verAfter)
	}
	e, ok := snapshotLedger(t, pubId)
	if !ok || e.state != ledgerStatePublished {
		t.Fatalf("ledger[%s].state = %s, want published", pubId, e.state)
	}
	// B 不再收到重复 found。
	if n := cB.countType("background_task_found", 500*time.Millisecond); n != 0 {
		t.Fatalf("B received %d duplicate found after replay, want 0", n)
	}
}

// TestT35_IdempotentReplayYPollution：B 在重放 P 之前先在 B 上开始新任务 Y
// 并发布 Q；重放 P 时 X 已被 Y 取代（§6 supersede），返回
// publish_failed{reason: already-superseded} 且不带任何快照——
// 设计 T35(6) 的 already-published 预期与 §6 矛盾（见重放处注释与交付报告）。
// 断言：version 不变，ledger[Q] 不受影响，X 的存储快照仍为原始快照。
func TestT35_IdempotentReplayYPollution(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t35-pollution-page"
	pubX := "pub-test-t35-pollution-page-1"
	pubY := "pub-test-t35-pollution-page-2"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cA.startRealTask()
	// A 发布 X。
	cA.send("run_in_background", map[string]interface{}{"publishId": pubX, "pageId": pageId})
	enX := cA.waitFor("background_task_enabled", 10*time.Second)
	_, xTaskId, _, xVer, _, _, _ := enabledFields(enX)
	if xTaskId == "" || xVer == 0 {
		t.Fatalf("X enabled fields unexpected: taskId=%q version=%v", xTaskId, xVer)
	}
	cA.Close()

	// B 连接并 follow X，随后停止 X、启动 Y、发布 Q。
	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register(pageId)
	cB.send("follow_background_task", map[string]interface{}{})
	cB.waitFor("background_task_following", 10*time.Second)
	cB.send("stop_task", map[string]interface{}{})
	// X 的任务跑在 A 的 session 对象上（B 已 rebind）；等它真正结束再启动 Y。
	sessX := snapshotServerSession(t, cA.connectionID)
	pollUntil(t, "X task stopped", 10*time.Second, 50*time.Millisecond, func() bool {
		sessX.taskMutex.Lock()
		defer sessX.taskMutex.Unlock()
		return !sessX.isTaskRunning
	})
	cB.send("start_task", map[string]interface{}{"threads": 1, "port": 443, "delay": 0, "scanMode": "tcping", "ipType": 4})
	pollUntil(t, "Y task running", 10*time.Second, 50*time.Millisecond, func() bool {
		sessX.taskMutex.Lock()
		defer sessX.taskMutex.Unlock()
		return sessX.isTaskRunning
	})
	cB.send("run_in_background", map[string]interface{}{"publishId": pubY, "pageId": pageId})
	enY := cB.waitFor("background_task_enabled", 10*time.Second)
	// 重放前的版本快照（Q 的发布已把版本推到 v3；重放本身不得再改变它）。
	backgroundTaskMutex.Lock()
	verBeforeReplay := taskIdentityVersion
	backgroundTaskMutex.Unlock()
	_, yTaskId, _, yVer, _, _, _ := enabledFields(enY)
	if yTaskId == "" || yTaskId == xTaskId {
		t.Fatalf("Y enabled taskId unexpected: %q (X=%q)", yTaskId, xTaskId)
	}
	if yVer <= xVer {
		t.Fatalf("Y version %v not greater than X version %v", yVer, xVer)
	}

	// B 重放 P（X 的 publishId）：X 已被 Y 取代——Y 的 beginTask 清除指针时
	// 按 §6 把 X 的 published 条目标记为 superseded，因此重放返回
	// publish_failed{reason: already-superseded}，而非 already-published。
	// 设计 T35(6) 的 already-published 预期与 §6 的 supersede 规则矛盾：
	// X 已结束且被 Y 取代，再把 X 的快照当作“已发布”返回会把已结束的旧任务
	// 当成当前任务；superseded 才是正确语义（T47 亦依赖此规则）。
	// “无 Y 污染”性质仍然成立且更强：失败响应不带任何快照。
	cB.send("run_in_background", map[string]interface{}{"publishId": pubX, "pageId": pageId})
	pf := cB.waitFor("background_task_publish_failed", 10*time.Second)
	pfData := msgData(pf)
	if got, _ := pfData["publishId"].(string); got != pubX {
		t.Fatalf("publish_failed publishId = %q, want %q", got, pubX)
	}
	if got, _ := pfData["reason"].(string); got != "already-superseded" {
		t.Fatalf("publish_failed reason = %q, want already-superseded", got)
	}
	if _, hasSnap := pfData["snapshot"]; hasSnap {
		t.Fatalf("publish_failed unexpectedly carries a snapshot: %v", pfData)
	}
	backgroundTaskMutex.Lock()
	verAfterReplay := taskIdentityVersion
	ledX := publishLedger[pubX]
	ledY := publishLedger[pubY]
	backgroundTaskMutex.Unlock()
	if verAfterReplay != verBeforeReplay {
		t.Fatalf("taskIdentityVersion changed on replay: %d -> %d", verBeforeReplay, verAfterReplay)
	}
	if ledX.state != ledgerStateSuperseded || ledX.taskId != xTaskId || ledX.version != int64(xVer) {
		t.Fatalf("ledger[X] unexpected after replay: %+v (want superseded, taskId=%q version=%v)", ledX, xTaskId, xVer)
	}
	// X 的存储快照仍为原始快照（未被重放改写）。
	if ledX.snapshot.TaskID != xTaskId || ledX.snapshot.TaskVersion != int64(xVer) {
		t.Fatalf("ledger[X] snapshot changed on replay: %+v", ledX.snapshot)
	}
	if ledY.state != ledgerStatePublished || ledY.taskId != yTaskId || ledY.version != int64(yVer) {
		t.Fatalf("ledger[Y] unexpected: %+v", ledY)
	}
}

// TestT35_RebindReplay：A 发布后断开，B follow 同一后台 session 后重放 P，
// B 得 X 的原始快照（与 A 的 enabled 逐项相等），版本不变。
func TestT35_RebindReplay(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t35-rebind-page"
	pubId := "pub-test-t35-rebind-page-1"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cA.startRealTask()
	cA.send("run_in_background", map[string]interface{}{"publishId": pubId, "pageId": pageId})
	enA := cA.waitFor("background_task_enabled", 10*time.Second)
	cA.Close()

	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register(pageId)
	cB.send("follow_background_task", map[string]interface{}{})
	cB.waitFor("background_task_following", 10*time.Second)
	cB.send("run_in_background", map[string]interface{}{"publishId": pubId, "pageId": pageId})
	enB := cB.waitFor("background_task_enabled", 10*time.Second)
	p1, t1, e1, v1, st1, se1, sv1 := enabledFields(enA)
	p2, t2, e2, v2, st2, se2, sv2 := enabledFields(enB)
	if p1 != p2 || t1 != t2 || e1 != e2 || v1 != v2 || st1 != st2 || se1 != se2 || sv1 != sv2 {
		t.Fatalf("rebind replay enabled differs:\nA %v\nB %v", msgData(enA), msgData(enB))
	}
}

// TestT35_LateResponseDroppedAndServerClose：A 的 P2 在慢 handler 后才被读循环
// 处理，此时 session 已 rebind 给 B；P2 的响应被丢弃（B 收不到 enabled 也
// 收不到 publish_failed），服务端按 §7.6 关闭 A 的原始连接；B 的状态查询
// 正常；A 重连后 recovery_query 按 ledger 决议 P2 为 found/published。
func TestT35_LateResponseDroppedAndServerClose(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t35-late-page"
	pub1 := "pub-test-t35-late-page-1"
	pub2 := "pub-test-t35-late-page-2"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cA.startRealTask()
	cA.send("run_in_background", map[string]interface{}{"publishId": pub1, "pageId": pageId})
	cA.waitFor("background_task_enabled", 10*time.Second)

	testHookSet(&testSlowHandlerGate, make(chan struct{}))
	cA.send("check_proxy_country", map[string]interface{}{})
	cA.send("run_in_background", map[string]interface{}{"publishId": pub2, "pageId": pageId})
	time.Sleep(200 * time.Millisecond)

	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register(pageId)
	cB.send("follow_background_task", map[string]interface{}{})
	cB.waitFor("background_task_following", 10*time.Second)
	// 断言 backgroundSession.ws 已是 B 的连接（与 B 的 loopEntry.ws 同一对象）。
	le, ok := snapshotLoopState(t, cB.connectionID)
	if !ok {
		t.Fatalf("no loopState entry for B %s", cB.connectionID)
	}
	if got := snapshotBackgroundSessionWS(t); got != le.ws {
		t.Fatalf("backgroundSession.ws != B's ws before releasing gate")
	}

	close(testHookGet(&testSlowHandlerGate))
	testHookSet(&testSlowHandlerGate, nil)
	// (d) A 观察到服务端关闭——测试全程不主动关闭 A。
	waitForClose(t, cA, 5*time.Second)
	// (b) B 收不到 P2 的任何响应。
	if n := cB.countType("background_task_enabled", 1*time.Second); n != 0 {
		t.Fatalf("B received %d enabled for P2, want 0", n)
	}
	if n := cB.countType("background_task_publish_failed", 1*time.Second); n != 0 {
		t.Fatalf("B received %d publish_failed for P2, want 0", n)
	}
	// P2 已按 ledger 提交（任务仍在运行，发布成功）。
	if e, ok := snapshotLedger(t, pub2); !ok || e.state != ledgerStatePublished {
		t.Fatalf("ledger[%s].state = %s, want published", pub2, e.state)
	}
	// (d) B 的 get_background_task_status 在同一窗口内正常响应。
	cB.send("get_background_task_status", map[string]interface{}{})
	upd := cB.waitFor("background_task_update", 5*time.Second)
	data := msgData(upd)
	snap, _ := data["snapshot"].(map[string]interface{})
	if tid, _ := snap["taskId"].(string); tid == "" {
		t.Fatalf("B status update has empty snapshot taskId: %v", data)
	}
	// A 重连后 recovery_query 按 ledger 决议 P2。
	cA2 := r3Dial(t, wsURL)
	cA2.readInit(10 * time.Second)
	cA2.register(pageId)
	cA2.recoveryQuery(pageId, []string{pub2}, "rec-t35-late", 0)
	found := cA2.waitFor("background_task_found", 10*time.Second)
	if out, ok := outcomeOf(found, pub2); !ok || out != "published" {
		t.Fatalf("requery P2 outcome = %q, want published: %v", out, outcomesOf(found))
	}
}

// TestT35_ConnStateIsolation：两条独立 session、正反两个方向——旧代次连接在
// gate 释放后发业务消息只得一次 backend_restarted 随后关闭，新代次连接
// 不受影响（心跳与状态查询正常）。
func TestT35_ConnStateIsolation(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t35-isolation-page"

	// 正向：A 为旧代次（设立被 gate 挡住后跨越 reset），B 为新代次正常连接。
	oldA := dialBlockedAcrossReset(t, wsURL)
	newB := r3Dial(t, wsURL)
	newB.readInit(10 * time.Second)
	newB.register(pageId)
	oldA.send("heartbeat", map[string]interface{}{})
	restarted := oldA.waitFor("backend_restarted", 5*time.Second)
	_ = restarted
	if n := oldA.countType("backend_restarted", 500*time.Millisecond); n != 0 {
		t.Fatalf("old A received %d extra backend_restarted, want exactly 1", n)
	}
	waitForClose(t, oldA, 5*time.Second)
	newB.send("heartbeat", map[string]interface{}{})
	newB.waitFor("heartbeat_ack", 5*time.Second)
	newB.send("get_background_task_status", map[string]interface{}{})
	newB.waitFor("background_task_missing", 5*time.Second)

	// 反向：B 为旧代次，A 为新代次正常连接。
	oldB := dialBlockedAcrossReset(t, wsURL)
	newA := r3Dial(t, wsURL)
	newA.readInit(10 * time.Second)
	newA.register(pageId)
	oldB.send("heartbeat", map[string]interface{}{})
	oldB.waitFor("backend_restarted", 5*time.Second)
	if n := oldB.countType("backend_restarted", 500*time.Millisecond); n != 0 {
		t.Fatalf("old B received %d extra backend_restarted, want exactly 1", n)
	}
	waitForClose(t, oldB, 5*time.Second)
	newA.send("heartbeat", map[string]interface{}{})
	newA.waitFor("heartbeat_ack", 5*time.Second)
	newA.send("get_background_task_status", map[string]interface{}{})
	newA.waitFor("background_task_missing", 5*time.Second)
}

// TestT35_PublishReturnGateEpoch：(i) 直接调用 publishBackgroundTask 返回的
// res.backendEpoch == E1，reset 后同一返回值仍为 E1（值语义）；
// (ii) e2e：B 用错 pageId 重放 P_A，handler 在 testPublishReturnGate 处阻塞，
// 测试触发 reset 后释放 gate——响应发送失败被丢弃，新连接收不到该 publishId
// 的 publish_failed，recovery_query 将其决议为 intent-unknown。
func TestT35_PublishReturnGateEpoch(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)

	// (i) 单元部分。
	s, _ := r3RunningSession(t, "test-t35e-unit")
	res := publishBackgroundTask(s, "pub-t35e-page-1", "t35e-page", nil)
	if res.outcome != "committed" || !res.committed {
		t.Fatalf("unit publish outcome = %q, want committed", res.outcome)
	}
	e1 := res.backendEpoch
	if e1 == "" {
		t.Fatal("unit publish returned empty backendEpoch")
	}
	if e1 != currentBackendEpoch() {
		t.Fatalf("unit publish backendEpoch %q != current %q", e1, currentBackendEpoch())
	}
	resetBackendGeneration()
	if res.backendEpoch != e1 {
		t.Fatalf("returned backendEpoch changed across reset: %q -> %q", e1, res.backendEpoch)
	}

	// (ii) e2e 部分。
	pageA := "test-t35e-page-a"
	pageB := "test-t35e-page-b"
	pubA := "pub-test-t35e-page-a-1"
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageA)
	cA.startRealTask()
	cA.send("run_in_background", map[string]interface{}{"publishId": pubA, "pageId": pageA})
	cA.waitFor("background_task_enabled", 10*time.Second)

	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register(pageB)
	testHookSet(&testPublishReturnGate, make(chan struct{}))
	cB.send("run_in_background", map[string]interface{}{"publishId": pubA, "pageId": pageB})
	time.Sleep(500 * time.Millisecond) // 等 handler 抵达 gate（阻塞后确定性停留）。
	resetBackendGeneration()
	close(testHookGet(&testPublishReturnGate))
	testHookSet(&testPublishReturnGate, nil)
	waitForClose(t, cB, 5*time.Second)

	// 新连接 C：不注册 pageA，直接查询 P_A → intent-unknown。
	cC := r3Dial(t, wsURL)
	cC.readInit(10 * time.Second)
	cC.drainConnectTimeMissing(500 * time.Millisecond)
	cC.recoveryQuery(pageA, []string{pubA}, "rec-t35e", 0)
	miss := cC.waitFor("background_task_missing", 10*time.Second)
	if out, ok := outcomeOf(miss, pubA); !ok || out != "intent-unknown" {
		t.Fatalf("requery P_A outcome = %q, want intent-unknown: %v", out, outcomesOf(miss))
	}
	if n := cC.countType("background_task_publish_failed", 500*time.Millisecond); n != 0 {
		t.Fatalf("C received %d publish_failed for P_A, want 0", n)
	}
	cC.send("get_background_task_status", map[string]interface{}{})
	cC.waitFor("background_task_missing", 5*time.Second)
}

// TestT35_PanicPreCommit：提交前 panic（testPublishPanicGate2 在步骤 2→3 之间），
// B 尚未连接。断言：A 恰好一条 "error"（文本含 run_in_background）；
// backgroundTaskSession 为 nil；ledger[P] 为 failed；A 的连接不关闭；
// 无第二条 "error"；随后 B 查询 P 得 missing(publish-failed)。
func TestT35_PanicPreCommit(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t35f-a-page"
	pubId := "pub-test-t35f-a-page-1"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cA.startRealTask()
	testHookSet(&testPublishPanicGate2, make(chan struct{}))
	cA.send("run_in_background", map[string]interface{}{"publishId": pubId, "pageId": pageId})
	errMsg := cA.waitFor("error", 10*time.Second)
	// "error" 消息的 data 本身就是字符串（见 handler 的 sendWSMessageToConn 调用）。
	txt, _ := errMsg["data"].(string)
	if !strings.Contains(txt, "run_in_background") {
		t.Fatalf("error text = %q, want it to contain run_in_background", txt)
	}
	if n := cA.countType("error", 500*time.Millisecond); n != 0 {
		t.Fatalf("A received %d extra error messages, want exactly 1", n)
	}
	// A 的连接不关闭（发送成功，无失败关闭）：500ms 内 closed 不应触发。
	select {
	case <-cA.closed:
		t.Fatal("A's connection closed after pre-commit panic, want it to stay open")
	case <-time.After(500 * time.Millisecond):
	}
	backgroundTaskMutex.Lock()
	bs := backgroundTaskSession
	led := publishLedger[pubId]
	backgroundTaskMutex.Unlock()
	if bs != nil {
		t.Fatalf("backgroundTaskSession = %v, want nil after rollback", bs)
	}
	if led.state != ledgerStateFailed {
		t.Fatalf("ledger[%s].state = %s, want failed", pubId, led.state)
	}
	// 随后 B 查询 P → missing(publish-failed)。
	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.drainConnectTimeMissing(500 * time.Millisecond)
	cB.register(pageId)
	cB.recoveryQuery(pageId, []string{pubId}, "rec-t35f-a", 0)
	miss := cB.waitFor("background_task_missing", 10*time.Second)
	if out, ok := outcomeOf(miss, pubId); !ok || out != "failed" {
		t.Fatalf("B requery P outcome = %q, want failed: %v", out, outcomesOf(miss))
	}
}

// TestT35_PanicRecoverOrder1：提交后 panic（testPublishPanicGate 在步骤 3→4 之间），
// recover 的发送步骤被 testPublishRecoverSendGate 挡住；顺序一：先释放 gate，
// A 收到恰好一条 "error"，随后 B follow + 查询得 found（恰好一次），B 未收到 "error"。
func TestT35_PanicRecoverOrder1(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t35f-b1-page"
	pubId := "pub-test-t35f-b1-page-1"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cA.startRealTask()
	testHookSet(&testPublishPanicGate, make(chan struct{}))
	testHookSet(&testPublishRecoverSendGate, make(chan struct{}))
	cA.send("run_in_background", map[string]interface{}{"publishId": pubId, "pageId": pageId})
	// 等 ledger 提交（panic 前已 commit；锁已释放的正面证据是 B 的 follow 能完成）。
	pollLedgerState(t, pubId, ledgerStatePublished, 10*time.Second)
	// 先释放 recover 发送 gate → A 收到恰好一条 "error"。
	close(testHookGet(&testPublishRecoverSendGate))
	testHookSet(&testPublishRecoverSendGate, nil)
	errMsg := cA.waitFor("error", 10*time.Second)
	// "error" 消息的 data 本身就是字符串（见 handler 的 sendWSMessageToConn 调用）。
	txt, _ := errMsg["data"].(string)
	if !strings.Contains(txt, "run_in_background") {
		t.Fatalf("error text = %q, want it to contain run_in_background", txt)
	}
	if n := cA.countType("error", 500*time.Millisecond); n != 0 {
		t.Fatalf("A received %d extra error messages, want exactly 1", n)
	}
	// 随后 B follow + 查询：found 恰好一次，B 未收到 "error"。
	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register(pageId)
	cB.send("follow_background_task", map[string]interface{}{})
	cB.waitFor("background_task_following", 10*time.Second)
	cB.recoveryQuery(pageId, []string{pubId}, "rec-t35f-b1", 0)
	found := cB.waitFor("background_task_found", 10*time.Second)
	if out, ok := outcomeOf(found, pubId); !ok || out != "published" {
		t.Fatalf("B requery P outcome = %q, want published: %v", out, outcomesOf(found))
	}
	if n := cB.countType("background_task_found", 500*time.Millisecond); n != 0 {
		t.Fatalf("B received %d extra found, want exactly 1", n)
	}
	if n := cB.countType("error", 500*time.Millisecond); n != 0 {
		t.Fatalf("B received %d error messages, want 0", n)
	}
}

// TestT35_PanicRecoverOrder2：与顺序一同一构造，但 B 先 follow + 查询（此时
// recover 的发送仍被 gate 挡住），再释放 gate——A 的发送失败（session 已换绑），
// 服务端关闭 A 的原始连接；B 的 found 恰好一次且不受影响。
func TestT35_PanicRecoverOrder2(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t35f-b2-page"
	pubId := "pub-test-t35f-b2-page-1"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cA.startRealTask()
	testHookSet(&testPublishPanicGate, make(chan struct{}))
	testHookSet(&testPublishRecoverSendGate, make(chan struct{}))
	cA.send("run_in_background", map[string]interface{}{"publishId": pubId, "pageId": pageId})
	pollLedgerState(t, pubId, ledgerStatePublished, 10*time.Second)

	// B 先 follow + 查询（gate 仍挡住 A 的 recover 发送）。
	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register(pageId)
	cB.send("follow_background_task", map[string]interface{}{})
	cB.waitFor("background_task_following", 10*time.Second)
	le, ok := snapshotLoopState(t, cB.connectionID)
	if !ok {
		t.Fatalf("no loopState entry for B %s", cB.connectionID)
	}
	if got := snapshotBackgroundSessionWS(t); got != le.ws {
		t.Fatalf("backgroundSession.ws != B's ws before releasing recover gate")
	}
	cB.recoveryQuery(pageId, []string{pubId}, "rec-t35f-b2", 0)
	found := cB.waitFor("background_task_found", 10*time.Second)
	if out, ok := outcomeOf(found, pubId); !ok || out != "published" {
		t.Fatalf("B requery P outcome = %q, want published: %v", out, outcomesOf(found))
	}
	// 再释放 gate：A 的 recover 发送失败 → 服务端关闭 A。
	close(testHookGet(&testPublishRecoverSendGate))
	testHookSet(&testPublishRecoverSendGate, nil)
	waitForClose(t, cA, 5*time.Second)
	if n := cA.countType("error", 500*time.Millisecond); n != 0 {
		t.Fatalf("A received %d error messages, want 0 (send failed)", n)
	}
	if n := cB.countType("background_task_found", 500*time.Millisecond); n != 0 {
		t.Fatalf("B received %d extra found, want exactly 1", n)
	}
	if n := cB.countType("error", 500*time.Millisecond); n != 0 {
		t.Fatalf("B received %d error messages, want 0", n)
	}
	cB.send("get_background_task_status", map[string]interface{}{})
	cB.waitFor("background_task_update", 5*time.Second)
}

// TestT35_PanicReboundOldHandler：A 的 session 已被 B rebind，A 的旧 handler
// 在处理 P2 时 panic（testPublishPanicGate2）→ rollback 执行（ledger[P2]=failed，
// 指针不再指向 P2 的任务）；B 未收到 A 的 "error"；A 的连接被服务端关闭；
// B 的 get_background_task_status 正常响应（指针已清空 → missing/no-task）。
func TestT35_PanicReboundOldHandler(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t35f-c-page"
	pub1 := "pub-test-t35f-c-page-1"
	pub2 := "pub-test-t35f-c-page-2"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cA.startRealTask()
	cA.send("run_in_background", map[string]interface{}{"publishId": pub1, "pageId": pageId})
	cA.waitFor("background_task_enabled", 10*time.Second)

	testHookSet(&testSlowHandlerGate, make(chan struct{}))
	cA.send("check_proxy_country", map[string]interface{}{})
	cA.send("run_in_background", map[string]interface{}{"publishId": pub2, "pageId": pageId})
	time.Sleep(200 * time.Millisecond)
	testHookSet(&testPublishPanicGate2, make(chan struct{}))

	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register(pageId)
	cB.send("follow_background_task", map[string]interface{}{})
	cB.waitFor("background_task_following", 10*time.Second)
	le, ok := snapshotLoopState(t, cB.connectionID)
	if !ok {
		t.Fatalf("no loopState entry for B %s", cB.connectionID)
	}
	if got := snapshotBackgroundSessionWS(t); got != le.ws {
		t.Fatalf("backgroundSession.ws != B's ws before releasing gate")
	}

	close(testHookGet(&testSlowHandlerGate))
	testHookSet(&testSlowHandlerGate, nil)
	// A 的连接被服务端关闭（recover 的 error 发送失败 → §7.6 关闭请求连接）。
	waitForClose(t, cA, 5*time.Second)
	if e, ok := snapshotLedger(t, pub2); !ok || e.state != ledgerStateFailed {
		t.Fatalf("ledger[%s].state = %s, want failed (rollback executed)", pub2, e.state)
	}
	backgroundTaskMutex.Lock()
	bs := backgroundTaskSession
	backgroundTaskMutex.Unlock()
	if bs != nil {
		t.Fatalf("backgroundTaskSession still set after rollback, want nil")
	}
	if n := cB.countType("error", 500*time.Millisecond); n != 0 {
		t.Fatalf("B received %d error messages from A's panic, want 0", n)
	}
	cB.send("get_background_task_status", map[string]interface{}{})
	miss := cB.waitFor("background_task_missing", 5*time.Second)
	if reason, _ := msgData(miss)["reason"].(string); reason != "no-task" {
		t.Fatalf("B status reason = %q, want no-task", reason)
	}
}

// TestT35_PanicClosedConn：A 关闭后 panic 的 recover 尝试发送——发送方已死，
// B 未收到 A 的 "error"，旧连接 loopState=exited，ledger[P2]=failed；
// A 是否收到该 "error" 本身是竞态，不做断言。
func TestT35_PanicClosedConn(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t35f-d-page"
	pub2 := "pub-test-t35f-d-page-2"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cA.startRealTask()

	testHookSet(&testSlowHandlerGate, make(chan struct{}))
	cA.send("check_proxy_country", map[string]interface{}{})
	cA.send("run_in_background", map[string]interface{}{"publishId": pub2, "pageId": pageId})
	time.Sleep(200 * time.Millisecond)
	testHookSet(&testPublishPanicGate2, make(chan struct{}))
	cA.Close()

	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register(pageId)

	close(testHookGet(&testSlowHandlerGate))
	testHookSet(&testSlowHandlerGate, nil)
	// A 的旧连接 loop 退出（exited）。
	pollLoopExited(t, cA.connectionID, 10*time.Second)
	if e, ok := snapshotLedger(t, pub2); !ok || e.state != ledgerStateFailed {
		t.Fatalf("ledger[%s].state = %s, want failed (rollback executed)", pub2, e.state)
	}
	if n := cB.countType("error", 500*time.Millisecond); n != 0 {
		t.Fatalf("B received %d error messages from A's panic, want 0", n)
	}
	cB.send("get_background_task_status", map[string]interface{}{})
	cB.waitFor("background_task_missing", 5*time.Second)
}

// ---------------------------------------------------------------------------
// T36a/T36b/T37/T38/T39/T40/T41（见 §16.2）。
// ---------------------------------------------------------------------------

// CloseRST 以 RST 关闭底层 TCP 连接（SetLinger(0) 后 Close），模拟对端崩溃。
func (c *r3Client) CloseRST() {
	if tc, ok := c.rawConn.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
		_ = tc.Close()
	} else {
		c.t.Fatalf("rawConn is not *net.TCPConn")
	}
}

// recoveryRequery 发送 recovery_requery（同一 recoveryId 的再次查询）。
func (c *r3Client) recoveryRequery(recoveryId string, attempt int) {
	c.t.Helper()
	c.send("recovery_requery", map[string]interface{}{"recoveryId": recoveryId, "attempt": attempt})
}

// snapshotPageSessions 返回某 pageId 下已登记的 session 集合（加锁快照）。
func snapshotPageSessions(t *testing.T, pageId string) map[*appSession]bool {
	t.Helper()
	backgroundTaskMutex.Lock()
	defer backgroundTaskMutex.Unlock()
	out := make(map[*appSession]bool)
	for s := range pageSessions[pageId] {
		out[s] = true
	}
	return out
}

// setReadDeadlineOverride 设置测试用 ReadDeadline 覆盖值（生产为 90s）；
// 清理由 resetTestHooks 兜底（见 r3Setup 的 t.Cleanup）。
func setReadDeadlineOverride(t *testing.T, d time.Duration) {
	t.Helper()
	testHookSet(&testReadDeadlineOverride, &d)
}

// TestT36a_RegisterGateBlocksBusiness：testRegisterReadGate 挡住 register_page
// 处理时，A 的业务消息（run_in_background + check_proxy_country）仍在
// TCP 缓冲中未处理；断开 A、释放 gate 后，A 的读循环按序读到 P 并发布，
// B 随后查询得 found。前端的入队/重发部分由 Node 侧 test_register_gate.js 覆盖。
func TestT36a_RegisterGateBlocksBusiness(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t36a1-page"
	pubId := "pub-test-t36a1-page-1"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.startRealTask()
	testHookSet(&testRegisterReadGate, make(chan struct{}))
	cA.send("register_page", map[string]interface{}{"pageId": pageId})
	cA.send("run_in_background", map[string]interface{}{"publishId": pubId, "pageId": pageId})
	cA.send("check_proxy_country", map[string]interface{}{})
	time.Sleep(200 * time.Millisecond)
	// register 未处理：pageSessions 无登记，loop 条目的 pageId 仍为空。
	if got := snapshotPageSessions(t, pageId); len(got) != 0 {
		t.Fatalf("pageSessions[%s] = %d sessions while register gated, want 0", pageId, len(got))
	}
	if le, ok := snapshotLoopState(t, cA.connectionID); !ok || le.pageId != "" {
		t.Fatalf("loop entry pageId = %q, want empty while register gated", le.pageId)
	}
	if _, ok := snapshotLedger(t, pubId); ok {
		t.Fatalf("ledger already has %s while register gated", pubId)
	}
	cA.Close()
	close(testHookGet(&testRegisterReadGate))
	testHookSet(&testRegisterReadGate, nil)
	// A 的读循环在 gate 释放后按序读到 P 并发布（选项 A：写失败不 Close，
	// 缓冲消息得以排空）。
	pollLedgerState(t, pubId, ledgerStatePublished, 10*time.Second)

	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register(pageId)
	cB.recoveryQuery(pageId, []string{pubId}, "rec-t36a1", 0)
	found := cB.waitFor("background_task_found", 10*time.Second)
	if out, ok := outcomeOf(found, pubId); !ok || out != "published" {
		t.Fatalf("B requery P outcome = %q, want published: %v", out, outcomesOf(found))
	}
}

// TestT36a_NoTaskMissing：无运行任务时，register 被 gate 挡住、A 断开、
// gate 释放后，B 查询（无意图）得 missing(no-task)。
func TestT36a_NoTaskMissing(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t36a2-page"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	testHookSet(&testRegisterReadGate, make(chan struct{}))
	cA.send("register_page", map[string]interface{}{"pageId": pageId})
	cA.send("check_proxy_country", map[string]interface{}{})
	time.Sleep(200 * time.Millisecond)
	if got := snapshotPageSessions(t, pageId); len(got) != 0 {
		t.Fatalf("pageSessions[%s] = %d sessions while register gated, want 0", pageId, len(got))
	}
	cA.Close()
	close(testHookGet(&testRegisterReadGate))
	testHookSet(&testRegisterReadGate, nil)

	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.drainConnectTimeMissing(500 * time.Millisecond)
	cB.register(pageId)
	cB.recoveryQuery(pageId, nil, "rec-t36a2", 0)
	miss := cB.waitFor("background_task_missing", 10*time.Second)
	if reason, _ := msgData(miss)["reason"].(string); reason != "no-task" {
		t.Fatalf("B missing reason = %q, want no-task", reason)
	}
}

// TestT36b_RSTExitBranch：RST 后 P 的结局按平台分支，不得无条件断言 P
// 一定被分发（第二轮补齐任务 3，修订 §1.6/§1.7/§16.2 的 T36b-1；
// 原名 TestT36b_RSTFound 隐含“RST → found”的跨平台保证，已废弃）。
// Linux 上已入队数据先交付、P 被发布 → found；丢弃已入队数据的平台上
// P 从未发布 → missing(publish-lost)。两种分支都断言：A 的 loop 最终
// 退出、任务状态与实际发布状态一致、无残留 pending、无 panic/死锁/泄漏。
// 判定方法：先放行 gate、等 loop 退出后重查（重查按 §10.6 从 ledger/
// loop 状态重新派生，是确定性的；已被建立的 pending 不会被 loop 退出
// 主动决议，故不等 found 推送）。
func TestT36b_RSTExitBranch(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t36b1-page"
	pubId := "pub-test-t36b1-page-1"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cA.startRealTask()
	testHookSet(&testSlowHandlerGate, make(chan struct{}))
	defer func() { testHookSet(&testSlowHandlerGate, nil) }()
	cA.send("check_proxy_country", map[string]interface{}{})
	cA.send("run_in_background", map[string]interface{}{"publishId": pubId, "pageId": pageId})
	time.Sleep(200 * time.Millisecond)
	if _, ok := snapshotLedger(t, pubId); ok {
		t.Fatalf("ledger already has %s before RST", pubId)
	}
	cA.CloseRST()

	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.drainConnectTimeMissing(500 * time.Millisecond)
	cB.register(pageId)
	cB.recoveryQuery(pageId, []string{pubId}, "rec-t36b1", 0)
	// 查询时 A 的 loop 仍活着（阻塞在 handler 内）→ 必为 pending；
	// P 的最终结局在 gate 释放、loop 退出后才确定。
	pend := cB.waitFor("background_task_pending", 10*time.Second)
	if out, ok := outcomeOf(pend, pubId); !ok || out != "pending" {
		t.Fatalf("B pending outcome = %q, want pending: %v", out, outcomesOf(pend))
	}
	if exists, _, _ := snapshotPending(t, "rec-t36b1"); !exists {
		t.Fatal("pendingRecoveries missing B's recoveryId")
	}
	// 稳态基线（同 TestPingWriteFailureMarksCurrentConnClosed 的方法）：
	// 两连接已建立、探测 HTTP 连接池已预热后再计数。
	time.Sleep(100 * time.Millisecond)
	baseGoroutines := runtime.NumGoroutine()
	// 释放 gate：Linux 上 loop 先分发 P（发布）再读到 RST 错误退出；
	// 丢弃平台上 loop 直接读到 RST 错误退出、P 丢失。两种情况 loop 都退出。
	close(testHookGet(&testSlowHandlerGate))
	testHookSet(&testSlowHandlerGate, nil)
	pollLoopExited(t, cA.connectionID, 10*time.Second)
	// 重查判定分支：终态优先，从 ledger/loop 状态重新派生。
	cB.recoveryRequery("rec-t36b1", 1)
	typ, msg := cB.waitForAny(10*time.Second, "background_task_found", "background_task_missing")
	sessA := snapshotServerSession(t, cA.connectionID)
	if sessA == nil {
		t.Fatal("no server session for A")
	}
	switch typ {
	case "background_task_found":
		// P 已发布分支（Linux 行为）：found + ledger published +
		// 后台任务在 A 退出后仍在运行（不断言跨平台）。
		if out, ok := outcomeOf(msg, pubId); !ok || out != "published" {
			t.Fatalf("B found outcome = %q, want published: %v", out, outcomesOf(msg))
		}
		if e, ok := snapshotLedger(t, pubId); !ok || e.state != ledgerStatePublished {
			t.Fatalf("ledger[%s] = %+v, want published", pubId, e)
		}
		pollUntil(t, "A task keeps running after publish", 5*time.Second, 50*time.Millisecond, func() bool {
			sessA.taskMutex.Lock()
			defer sessA.taskMutex.Unlock()
			return sessA.isTaskRunning
		})
		t.Logf("RST branch: P published (found)")
	case "background_task_missing":
		// P 未发布分支（丢弃平台行为）：missing(publish-lost) +
		// ledger 无 P + 前台任务随断开被取消。
		if out, ok := outcomeOf(msg, pubId); !ok || out != "publish-lost" {
			t.Fatalf("B missing outcome = %q, want publish-lost: %v", out, outcomesOf(msg))
		}
		if reason, _ := msgData(msg)["reason"].(string); reason != "publish-lost" {
			t.Fatalf("B missing reason = %q, want publish-lost", reason)
		}
		if _, ok := snapshotLedger(t, pubId); ok {
			t.Fatalf("ledger has %s although B got publish-lost", pubId)
		}
		waitSessionStopped(t, sessA, 10*time.Second)
		t.Logf("RST branch: P lost (publish-lost)")
	default:
		t.Fatalf("unexpected requery reply type %q", typ)
	}
	// 两种分支的公共断言：无残留 pending；B 断开后 goroutine 回到基线。
	if exists, _, _ := snapshotPending(t, "rec-t36b1"); exists {
		t.Fatal("pending still present after requery resolved it")
	}
	cB.Close()
	pollLoopExited(t, cB.connectionID, 10*time.Second)
	time.Sleep(500 * time.Millisecond)
	if got := runtime.NumGoroutine(); got > baseGoroutines+2 {
		t.Fatalf("goroutine count = %d, baseline %d (leak > 2)", got, baseGoroutines)
	}
}

// TestT36b_PendingThenFound：gate 挡住时 B 得 pending，释放后 B 得 found；
// 两条恢复信号的 signalSeq 严格递增（FIFO，见 §10.6.1）。
func TestT36b_PendingThenFound(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t36b2-page"
	pubId := "pub-test-t36b2-page-1"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cA.startRealTask()
	testHookSet(&testSlowHandlerGate, make(chan struct{}))
	cA.send("check_proxy_country", map[string]interface{}{})
	cA.send("run_in_background", map[string]interface{}{"publishId": pubId, "pageId": pageId})
	time.Sleep(200 * time.Millisecond)
	cA.Close()

	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.drainConnectTimeMissing(500 * time.Millisecond)
	cB.register(pageId)
	cB.recoveryQuery(pageId, []string{pubId}, "rec-t36b2", 0)
	pend := cB.waitFor("background_task_pending", 10*time.Second)
	seqPend, _ := msgData(pend)["signalSeq"].(float64)
	if seqPend == 0 {
		t.Fatalf("pending signalSeq missing: %v", msgData(pend))
	}
	close(testHookGet(&testSlowHandlerGate))
	testHookSet(&testSlowHandlerGate, nil)
	found := cB.waitFor("background_task_found", 15*time.Second)
	seqFound, _ := msgData(found)["signalSeq"].(float64)
	if seqFound <= seqPend {
		t.Fatalf("signalSeq not increasing: pending=%v found=%v", seqPend, seqFound)
	}
	if exists, _, _ := snapshotPending(t, "rec-t36b2"); exists {
		t.Fatalf("pending entry still exists after found")
	}
}

// TestT36b_PublishLost：P 从未发出，A 的 loop 已退出 → B 得
// missing(publish-lost)，不创建 pending。
func TestT36b_PublishLost(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t36b3-page"
	pubId := "pub-test-t36b3-page-1"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cA.Close()
	pollLoopExited(t, cA.connectionID, 10*time.Second)

	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.drainConnectTimeMissing(500 * time.Millisecond)
	cB.register(pageId)
	cB.recoveryQuery(pageId, []string{pubId}, "rec-t36b3", 0)
	miss := cB.waitFor("background_task_missing", 10*time.Second)
	if out, ok := outcomeOf(miss, pubId); !ok || out != "publish-lost" {
		t.Fatalf("B missing outcome = %q, want publish-lost: %v", out, outcomesOf(miss))
	}
	if reason, _ := msgData(miss)["reason"].(string); reason != "publish-lost" {
		t.Fatalf("B missing reason = %q, want publish-lost", reason)
	}
	if rid, _ := msgData(miss)["recoveryId"].(string); rid != "rec-t36b3" {
		t.Fatalf("missing recoveryId = %q, want rec-t36b3 (not the connect-time signal)", rid)
	}
	if exists, _, _ := snapshotPending(t, "rec-t36b3"); exists {
		t.Fatalf("pending entry created for a terminal missing")
	}
}

// TestT36b_FINFound：FIN 断开时 B 得 found，且 A 的后台任务在 A 退出后
// 仍在运行（未被取消）。
func TestT36b_FINFound(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t36b4-page"
	pubId := "pub-test-t36b4-page-1"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cA.startRealTask()
	testHookSet(&testSlowHandlerGate, make(chan struct{}))
	cA.send("check_proxy_country", map[string]interface{}{})
	cA.send("run_in_background", map[string]interface{}{"publishId": pubId, "pageId": pageId})
	time.Sleep(200 * time.Millisecond)
	cA.Close()

	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.drainConnectTimeMissing(500 * time.Millisecond)
	cB.register(pageId)
	cB.recoveryQuery(pageId, []string{pubId}, "rec-t36b4", 0)
	cB.waitFor("background_task_pending", 10*time.Second)
	close(testHookGet(&testSlowHandlerGate))
	testHookSet(&testSlowHandlerGate, nil)
	found := cB.waitFor("background_task_found", 15*time.Second)
	if out, ok := outcomeOf(found, pubId); !ok || out != "published" {
		t.Fatalf("B found outcome = %q, want published: %v", out, outcomesOf(found))
	}
	pollLoopExited(t, cA.connectionID, 10*time.Second)
	sessA := snapshotServerSession(t, cA.connectionID)
	sessA.taskMutex.Lock()
	running := sessA.isTaskRunning
	sessA.taskMutex.Unlock()
	if !running {
		t.Fatalf("A's background task stopped after A exited, want it to keep running")
	}
}

// TestT36b_FoundWithPublishLostOutcome：P 从未发出，A 已退出；B 启动并发布
// Y（publishId Q）后查询 [P] → found(Y)，且 outcomes=[{P, publish-lost}]，
// found 的 taskVersion 与 ledger[Q].version 相对一致。
func TestT36b_FoundWithPublishLostOutcome(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t36b5-page"
	pubP := "pub-test-t36b5-page-1"
	pubQ := "pub-test-t36b5-page-2"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cA.Close()
	pollLoopExited(t, cA.connectionID, 10*time.Second)

	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.drainConnectTimeMissing(500 * time.Millisecond)
	cB.register(pageId)
	cB.startRealTask()
	cB.send("run_in_background", map[string]interface{}{"publishId": pubQ, "pageId": pageId})
	enY := cB.waitFor("background_task_enabled", 10*time.Second)
	_, yTaskId, _, _, _, _, _ := enabledFields(enY)
	cB.recoveryQuery(pageId, []string{pubP}, "rec-t36b5", 0)
	found := cB.waitFor("background_task_found", 10*time.Second)
	if out, ok := outcomeOf(found, pubP); !ok || out != "publish-lost" {
		t.Fatalf("B found outcome for P = %q, want publish-lost: %v", out, outcomesOf(found))
	}
	data := msgData(found)
	snap, _ := data["snapshot"].(map[string]interface{})
	if tid, _ := snap["taskId"].(string); tid != yTaskId {
		t.Fatalf("found snapshot taskId = %q, want Y's %q", tid, yTaskId)
	}
	ver, _ := data["taskVersion"].(float64)
	ledQ, _ := snapshotLedger(t, pubQ)
	if int64(ver) != ledQ.version {
		t.Fatalf("found taskVersion = %v, want ledger[Q].version = %d (relative)", ver, ledQ.version)
	}
}

// TestT37_HalfOpenDeadlineEviction：A 半开（不断开 TCP，只是不发任何消息），
// B 同一 pageId 注册并查询得 found；A 的 ReadDeadline（覆盖为 3s）到期后
// loop 退出，从 pageSessions 删除；B 再次查询仍得 found。
func TestT37_HalfOpenDeadlineEviction(t *testing.T) {
	r3Setup(t)
	setReadDeadlineOverride(t, 3*time.Second)
	_, wsURL := r3Server(t)
	pageId := "test-t37-page"
	pubId := "pub-test-t37-page-1"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cA.startRealTask()
	cA.send("run_in_background", map[string]interface{}{"publishId": pubId, "pageId": pageId})
	cA.waitFor("background_task_enabled", 10*time.Second)

	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register(pageId)
	stopPong := startPonger(t, cB)
	defer stopPong()
	cB.recoveryQuery(pageId, nil, "rec-t37-1", 0)
	found1 := cB.waitFor("background_task_found", 10*time.Second)
	if out, ok := outcomeOf(found1, pubId); ok && out != "published" {
		t.Fatalf("first query outcome = %q, want published or absent", out)
	}
	if got := snapshotPageSessions(t, pageId); len(got) != 2 {
		t.Fatalf("pageSessions[%s] = %d sessions, want 2 (A+B)", pageId, len(got))
	}
	// A 的 ReadDeadline 到期（3s，无 Pong）→ loop 退出。
	pollLoopExited(t, cA.connectionID, 10*time.Second)
	if got := snapshotPageSessions(t, pageId); len(got) != 1 {
		t.Fatalf("pageSessions[%s] = %d sessions after A exited, want 1 (B only)", pageId, len(got))
	}
	sessA := snapshotServerSession(t, cA.connectionID)
	sessA.taskMutex.Lock()
	running := sessA.isTaskRunning
	sessA.taskMutex.Unlock()
	if !running {
		t.Fatalf("A's background task stopped after deadline exit, want it to keep running")
	}
	cB.recoveryQuery(pageId, nil, "rec-t37-2", 0)
	found2 := cB.waitFor("background_task_found", 10*time.Second)
	if _, ok := outcomeOf(found2, pubId); !ok {
		// 无意图查询的 found 不带 outcomes 也可接受；关键是仍为终态 found。
		if _, hasSnap := msgData(found2)["snapshot"]; !hasSnap {
			t.Fatalf("second query did not return found: %v", msgData(found2))
		}
	}
}

// TestT38_ResetTwice：连续两次 resetBackendGeneration——epoch 不同，
// 五张映射为空，指针为 nil，版本为 0，代次 +2。
func TestT38_ResetTwice(t *testing.T) {
	r3Setup(t)
	gen0 := atomic.LoadUint64(&backendGeneration)
	resetBackendGeneration()
	e1 := currentBackendEpoch()
	if e1 == "" {
		t.Fatal("epoch empty after first reset")
	}
	resetBackendGeneration()
	e2 := currentBackendEpoch()
	if e2 == "" || e2 == e1 {
		t.Fatalf("epochs not distinct: %q vs %q", e1, e2)
	}
	if got := atomic.LoadUint64(&backendGeneration); got != gen0+2 {
		t.Fatalf("generation = %d, want %d", got, gen0+2)
	}
	backgroundTaskMutex.Lock()
	if len(publishLedger) != 0 || len(pageSessions) != 0 || len(pendingRecoveries) != 0 || len(resolvedRecoveries) != 0 || len(connLoopState) != 0 {
		t.Fatalf("maps not empty after reset: ledger=%d pages=%d pending=%d resolved=%d loops=%d",
			len(publishLedger), len(pageSessions), len(pendingRecoveries), len(resolvedRecoveries), len(connLoopState))
	}
	if backgroundTaskSession != nil {
		t.Fatalf("backgroundTaskSession = %v, want nil", backgroundTaskSession)
	}
	if taskIdentityVersion != 0 {
		t.Fatalf("taskIdentityVersion = %d, want 0", taskIdentityVersion)
	}
	backgroundTaskMutex.Unlock()
	// runningTasksByGeneration 不要求为空（§2.4）：上一代的孤儿任务可能仍在
	// 运行——reset 只发取消，endTask 递减是异步的；A2 的范围是上面五张映射。
	// 只断言当前代次无运行任务（本测试未启动任何任务）。
	// 该表由 globalTaskMutex 保护（beginTask/endTask 在其下读写），必须在
	// 单独的 globalTaskMutex 临界区内读——不得在 backgroundTaskMutex 内读
	// （与 endTask 的写形成 data race；也不得反序嵌套两把锁）。
	globalTaskMutex.Lock()
	n := runningTasksByGeneration[atomic.LoadUint64(&backendGeneration)]
	globalTaskMutex.Unlock()
	if n != 0 {
		t.Fatalf("runningTasksByGeneration[current] = %d, want 0", n)
	}
}

// TestT38_SetupGateAcrossReset：连接设立被 testSetupGate 挡住、跨越一次 reset
// 后释放；旧代次连接发业务消息只得一次 backend_restarted 随后关闭，
// 无 publish_failed；新代次连接正常（心跳与状态查询）。
func TestT38_SetupGateAcrossReset(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t38a4i-page"

	oldA := dialBlockedAcrossReset(t, wsURL)
	newB := r3Dial(t, wsURL)
	newB.readInit(10 * time.Second)
	newB.register(pageId)
	oldA.send("heartbeat", map[string]interface{}{})
	oldA.waitFor("backend_restarted", 5*time.Second)
	if n := oldA.countType("backend_restarted", 500*time.Millisecond); n != 0 {
		t.Fatalf("old A received %d extra backend_restarted, want exactly 1", n)
	}
	if n := oldA.countType("background_task_publish_failed", 500*time.Millisecond); n != 0 {
		t.Fatalf("old A received %d publish_failed, want 0", n)
	}
	waitForClose(t, oldA, 5*time.Second)
	newB.send("heartbeat", map[string]interface{}{})
	newB.waitFor("heartbeat_ack", 5*time.Second)
	if n := newB.countType("backend_restarted", 500*time.Millisecond); n != 0 {
		t.Fatalf("new B received %d backend_restarted, want 0", n)
	}
	newB.send("get_background_task_status", map[string]interface{}{})
	newB.waitFor("background_task_missing", 5*time.Second)
}

// TestT38_PublishLockGateAcrossReset：publishBackgroundTask 在取锁前被
// testPublishLockGate 挡住时触发 reset（关闭该连接 socket）；释放 gate 后
// 决议为 generation-mismatch，不发 publish_failed，ledger 无该 publishId；
// 新连接恢复查询得 intent-unknown。
// 注：reset 会删除 connLoopState 条目，故旧连接的 loopState=exited 在 reset
// 后不可观测；以“客户端观察到关闭 + loop 条目已删除”作为等价断言。
func TestT38_PublishLockGateAcrossReset(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t38a4ii-page"
	pubId := "pub-test-t38a4ii-page-1"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cA.startRealTask()
	testHookSet(&testPublishLockGate, make(chan struct{}))
	cA.send("run_in_background", map[string]interface{}{"publishId": pubId, "pageId": pageId})
	time.Sleep(500 * time.Millisecond) // 等 handler 抵达 gate（阻塞后确定性停留）。
	resetBackendGeneration()
	close(testHookGet(&testPublishLockGate))
	testHookSet(&testPublishLockGate, nil)
	waitForClose(t, cA, 5*time.Second)
	if _, ok := snapshotLedger(t, pubId); ok {
		t.Fatalf("ledger has %s after gen-mismatch, want no entry", pubId)
	}
	if _, ok := snapshotLoopState(t, cA.connectionID); ok {
		t.Fatalf("loopState entry for A still exists after reset (want deleted by reset)")
	}
	if n := cA.countType("background_task_publish_failed", 500*time.Millisecond); n != 0 {
		t.Fatalf("A received %d publish_failed, want 0", n)
	}

	// 新连接 C：注册另一 pageId，查询 P → intent-unknown（证据已清空）。
	cC := r3Dial(t, wsURL)
	cC.readInit(10 * time.Second)
	cC.drainConnectTimeMissing(500 * time.Millisecond)
	cC.register("test-t38a4ii-other")
	cC.recoveryQuery(pageId, []string{pubId}, "rec-t38a4ii", 0)
	miss := cC.waitFor("background_task_missing", 10*time.Second)
	if out, ok := outcomeOf(miss, pubId); !ok || out != "intent-unknown" {
		t.Fatalf("C requery P outcome = %q, want intent-unknown: %v", out, outcomesOf(miss))
	}
	if n := cC.countType("background_task_publish_failed", 500*time.Millisecond); n != 0 {
		t.Fatalf("C received %d publish_failed, want 0", n)
	}
	cC.send("get_background_task_status", map[string]interface{}{})
	cC.waitFor("background_task_missing", 5*time.Second)
}

// TestT38_PublishStaleGenerationUnit：旧代次 session 直接调用
// publishBackgroundTask → generation-mismatch，无 ledger 写入。
func TestT38_PublishStaleGenerationUnit(t *testing.T) {
	r3Setup(t)
	s, _ := r3RunningSession(t, "test-t38a4-unit")
	resetBackendGeneration()
	res := publishBackgroundTask(s, "pub-t38a4-unit-1", "t38a4-unit-page", nil)
	if res.outcome != "generation-mismatch" {
		t.Fatalf("outcome = %q, want generation-mismatch", res.outcome)
	}
	if res.committed {
		t.Fatalf("committed = true, want false")
	}
	if res.backendEpoch != "" {
		t.Fatalf("backendEpoch = %q, want empty", res.backendEpoch)
	}
	if res.hasSnapshot {
		t.Fatalf("hasSnapshot = true, want false")
	}
	if _, ok := snapshotLedger(t, "pub-t38a4-unit-1"); ok {
		t.Fatalf("ledger has the stale publishId, want no entry")
	}
}

// TestT39_SameEpoch：三条连接的 init_config epoch 相同；恢复信号与状态
// 查询响应也全部携带同一 epoch。
// findGoToolchain 定位 go 命令：PATH → /tmp/go/bin/go → /usr/local/go/bin/go；
// 都找不到时跳过（黑盒测试需要工具链编译后端二进制）。
func findGoToolchain(t *testing.T) string {
	t.Helper()
	if p, err := exec.LookPath("go"); err == nil {
		return p
	}
	for _, p := range []string{"/tmp/go/bin/go", "/usr/local/go/bin/go"} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	t.Skip("go toolchain not found; skipping black-box test")
	return ""
}

// freeLoopbackPort 返回一个当前空闲的 loopback 端口（监听 :0 后立即释放；
// 仅用于测试进程启动，占用竞态可忽略）。
func freeLoopbackPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

// startBackendProcess 启动一个独立的后端进程（编译好的二进制），等其端口
// 可连接后读 init_config 的 epoch。返回 epoch；进程由 t.Cleanup 停止。
// 每个进程用独立的 HOME（隔离配置文件）与 CFDATA_IOS=1（跳过启动时
// 阻塞的外网测速源探测与 locations 加载，只求快速启动；epoch 接线
// 在 main() 顶部，与该分支无关）。
func startBackendProcess(t *testing.T, binPath, home string) string {
	t.Helper()
	port := freeLoopbackPort(t)
	if err := os.MkdirAll(home, 0o700); err != nil {
		t.Fatalf("mkdir home: %v", err)
	}
	var out strings.Builder
	cmd := exec.Command(binPath, "-host", "127.0.0.1", "-port", strconv.Itoa(port))
	cmd.Env = append(os.Environ(), "HOME="+home, "CFDATA_IOS=1", "NO_COLOR=1")
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start backend: %v", err)
	}
	kill := func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}
	t.Cleanup(kill)
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	deadline := time.Now().Add(30 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			_ = c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("backend did not listen on %s within 30s\n%s", addr, out.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	wsURL := "ws://127.0.0.1:" + strconv.Itoa(port) + "/ws"
	c := r3Dial(t, wsURL)
	c.readInit(10 * time.Second)
	return c.initEpoch
}

// TestT38_A1_BlackBoxTwoProcesses：T38 自动化 A1——启动两个独立的后端
// 进程（编译好的二进制），分别读其 init_config 的 epoch，验证不同
// （E2 ≠ E1）。第二轮补齐任务 4 后 main() 顶部已调用
// resetBackendGeneration，此前该测试因接线缺失而无法运行（见交付报告
// 的受阻项）。进程的启动、停止与 epoch 读取均经测试代码控制。
func TestT38_A1_BlackBoxTwoProcesses(t *testing.T) {
	goBin := findGoToolchain(t)
	tmpDir := t.TempDir()
	binPath := filepath.Join(tmpDir, "cfdata-a1")
	buildCtx, buildCancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer buildCancel()
	build := exec.CommandContext(buildCtx, goBin, "build", "-o", binPath, ".")
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	build.Dir = wd
	var buildOut strings.Builder
	build.Stdout = &buildOut
	build.Stderr = &buildOut
	if err := build.Run(); err != nil {
		t.Fatalf("build backend binary: %v\n%s", err, buildOut.String())
	}
	epoch1 := startBackendProcess(t, binPath, filepath.Join(tmpDir, "home-1"))
	epoch2 := startBackendProcess(t, binPath, filepath.Join(tmpDir, "home-2"))
	if epoch1 == "" || epoch2 == "" {
		t.Fatalf("epochs must be non-empty: E1=%q E2=%q", epoch1, epoch2)
	}
	if epoch1 == epoch2 {
		t.Fatalf("epochs must differ across processes: E1=%q E2=%q", epoch1, epoch2)
	}
	t.Logf("black-box epochs differ: E1=%s E2=%s", epoch1, epoch2)
}

func TestT39_SameEpoch(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t39-page"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cC := r3Dial(t, wsURL)
	cC.readInit(10 * time.Second)
	if cA.initEpoch == "" || cA.initEpoch != cB.initEpoch || cB.initEpoch != cC.initEpoch {
		t.Fatalf("epochs differ: A=%q B=%q C=%q", cA.initEpoch, cB.initEpoch, cC.initEpoch)
	}
	e := cA.initEpoch
	cB.register(pageId)
	cB.drainConnectTimeMissing(500 * time.Millisecond)
	cB.recoveryQuery(pageId, nil, "rec-t39", 0)
	miss := cB.waitFor("background_task_missing", 10*time.Second)
	if got, _ := msgData(miss)["backendEpoch"].(string); got != e {
		t.Fatalf("recovery missing epoch = %q, want %q", got, e)
	}
	cC.send("get_background_task_status", map[string]interface{}{})
	miss2 := cC.waitFor("background_task_missing", 5*time.Second)
	if got, _ := msgData(miss2)["backendEpoch"].(string); got != e {
		t.Fatalf("status missing epoch = %q, want %q", got, e)
	}
}

// TestT40_PongExtendsDeadline：客户端显式 Pong 后 lastPongAtNano > 0，
// 且连接在初始 ReadDeadline（覆盖为 3s）之后仍存活。
func TestT40_PongExtendsDeadline(t *testing.T) {
	r3Setup(t)
	setReadDeadlineOverride(t, 3*time.Second)
	_, wsURL := r3Server(t)

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register("test-t40-page")
	time.Sleep(1 * time.Second)
	if err := cA.conn.WriteControl(websocket.PongMessage, nil, time.Now().Add(2*time.Second)); err != nil {
		t.Fatalf("client pong: %v", err)
	}
	sess := snapshotServerSession(t, cA.connectionID)
	// WriteControl 返回只代表帧已写出，服务端处理 Pong 需要一点时间——轮询等待。
	pollUntil(t, "lastPongAtNano > 0", 5*time.Second, 50*time.Millisecond, func() bool {
		return sess.lastPongAtNano.Load() > 0
	})
	if got := sess.lastPongAtNano.Load(); got < sess.connectedAtNano.Load() {
		t.Fatalf("lastPongAtNano %d < connectedAtNano %d", got, sess.connectedAtNano.Load())
	}
	// 初始 deadline 为连接后 3s；pong 将其续到 1s+3s=4s——在 3.5s 时查询应仍成功。
	time.Sleep(2500 * time.Millisecond)
	cA.send("get_background_task_status", map[string]interface{}{})
	cA.waitFor("background_task_missing", 5*time.Second)
}

// TestT41_DeadlineTimeoutResolvesPublishLost：客户端从不 Pong，ReadDeadline
// （覆盖为 2s）到期后 loop 退出、wsClosed 置位；B 的 pending 经 requery
// 决议为 missing(publish-lost)。
func TestT41_DeadlineTimeoutResolvesPublishLost(t *testing.T) {
	r3Setup(t)
	setReadDeadlineOverride(t, 2*time.Second)
	_, wsURL := r3Server(t)
	pageId := "test-t41-page"
	pubId := "pub-test-t41-page-1"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)

	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register(pageId)
	stopPong := startPonger(t, cB)
	defer stopPong()
	cB.recoveryQuery(pageId, []string{pubId}, "rec-t41", 0)
	pend := cB.waitFor("background_task_pending", 10*time.Second)
	if out, ok := outcomeOf(pend, pubId); !ok || out != "pending" {
		t.Fatalf("B pending outcome = %q, want pending: %v", out, outcomesOf(pend))
	}
	pollLoopExited(t, cA.connectionID, 10*time.Second)
	sessA := snapshotServerSession(t, cA.connectionID)
	_, closed := sessA.wsSnapshot()
	if !closed {
		t.Fatalf("A wsClosed = false after deadline exit, want true")
	}
	cB.recoveryRequery("rec-t41", 1)
	miss := cB.waitFor("background_task_missing", 10*time.Second)
	if out, ok := outcomeOf(miss, pubId); !ok || out != "publish-lost" {
		t.Fatalf("B requery outcome = %q, want publish-lost: %v", out, outcomesOf(miss))
	}
}

// ---------------------------------------------------------------------------
// T42/T43/T44（见 §16.2/§4/§8.5）。
// ---------------------------------------------------------------------------

// blockingTestConn 是 T42 的可阻塞测试连接：累计写入 ≥ 阈值（64KB）后，
// 写调用不再落到底层连接，而是先关闭 writeBlocked 再阻塞，直到 Close。
// 读路径直通，握手在 Arm 之前完成故不受影响。
const blockingTestThreshold = 64 * 1024

type blockingTestConn struct {
	net.Conn
	mu           sync.Mutex
	armed        bool
	written      int64
	writeBlocked chan struct{}
	blockedOnce  sync.Once
	closed       chan struct{}
	closeOnce    sync.Once
	// failWrites 由测试设置：为 true 时 Write 直接返回注入错误（不写底层连接）。
	// 用于 T7-35(a)/T7-37(a) 的写失败 E1 注入（v7.5/v7.6：生产完成证据为
	// writeWSMessageLocked 的返回值 false；本模式只改变底层写行为，不改生产代码）。
	failWrites atomic.Bool
}

// SetFailWrites 开关写失败注入（测试用；false 恢复直通）。
func (c *blockingTestConn) SetFailWrites(v bool) { c.failWrites.Store(v) }

func newBlockingTestConn(c net.Conn) *blockingTestConn {
	return &blockingTestConn{Conn: c, writeBlocked: make(chan struct{}), closed: make(chan struct{})}
}

// Arm 在握手完成后调用：清零计数并开始按阈值阻塞写入。
func (c *blockingTestConn) Arm() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.armed = true
	c.written = 0
}

func (c *blockingTestConn) Write(p []byte) (int, error) {
	if c.failWrites.Load() {
		return 0, errors.New("blockingTestConn: injected write failure")
	}
	c.mu.Lock()
	armed, written := c.armed, c.written
	c.mu.Unlock()
	if armed && written+int64(len(p)) >= blockingTestThreshold {
		c.blockedOnce.Do(func() { close(c.writeBlocked) })
		<-c.closed
		return 0, errors.New("blockingTestConn: closed while write blocked")
	}
	n, err := c.Conn.Write(p)
	if err == nil {
		c.mu.Lock()
		c.written += int64(n)
		c.mu.Unlock()
	}
	return n, err
}

func (c *blockingTestConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

// blockingListener 包装 Accept，把每个接受的连接包成 blockingTestConn 并记录。
type blockingListener struct {
	net.Listener
	mu    sync.Mutex
	conns []*blockingTestConn
}

func (l *blockingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	bc := newBlockingTestConn(c)
	l.mu.Lock()
	l.conns = append(l.conns, bc)
	l.mu.Unlock()
	return bc, nil
}

func (l *blockingListener) lastConn() *blockingTestConn {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.conns) == 0 {
		return nil
	}
	return l.conns[len(l.conns)-1]
}

// r3ServerBlocking 返回带可阻塞监听器的测试服务器（T42 用）。
func r3ServerBlocking(t *testing.T) (*httptest.Server, string, *blockingListener) {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handleWebSocket)
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	bl := &blockingListener{Listener: raw}
	srv := httptest.NewUnstartedServer(mux)
	srv.Listener = bl
	srv.Start()
	t.Cleanup(srv.Close)
	return srv, "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws", bl
}

// TestT42_BlockedWriteInterruptedByClose：连接写路径在 64KB 后阻塞；
// 1MB 直接写与 sender 的写都被 Close 在 2s 内打断（§4 的 wg.Wait 界），
// sender 的 exited 关闭，且无 goroutine 泄漏。
func TestT42_BlockedWriteInterruptedByClose(t *testing.T) {
	r3Setup(t)
	_, wsURL, bl := r3ServerBlocking(t)
	pageId := "test-t42-page"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	bc := bl.lastConn()
	if bc == nil {
		t.Fatal("no accepted connection recorded")
	}
	// 稳态基线：连接已建立、读循环与 ping 就绪后再计数。
	time.Sleep(100 * time.Millisecond)
	baseGoroutines := runtime.NumGoroutine()
	bc.Arm()

	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	// 1MB 直接写（goroutine 内执行，写满即阻塞）。
	bigDone := make(chan struct{})
	go func() {
		defer close(bigDone)
		sess.sendWSMessageDirect("test_big", map[string]interface{}{"blob": strings.Repeat("x", 1024*1024)})
	}()
	select {
	case <-bc.writeBlocked:
	case <-time.After(10 * time.Second):
		t.Fatal("write did not block at the 64KB threshold")
	}
	// sender 也入队一条信号：它随后在 wsMutex（被直接写持有）或写调用上阻塞。
	rs := newRecoverySender(sess)
	rs.enqueue(recoverySignal{Type: "background_task_found", Data: map[string]interface{}{"snapshot": map[string]interface{}{"taskId": "t42"}}, RecoveryID: ""})
	time.Sleep(200 * time.Millisecond) // 让 sender 抵达阻塞点。

	// Close 必须在 2s 内打断两者（T42 阈值）。
	// 注意：不能经 sess.wsSnapshot() 取 gorilla 连接再关——wsSnapshot 要拿
	// wsMutex，而 1MB 直接写正持有它阻塞在 wrapper 里等 closed，会死锁。
	// 直接关底层 blockingTestConn：它就是服务端 socket，关它同样打断写调用。
	closeStart := time.Now()
	if err := bc.Close(); err != nil {
		t.Fatalf("close server conn: %v", err)
	}
	select {
	case <-bigDone:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked 1MB direct write not interrupted within 2s of Close")
	}
	select {
	case <-rs.exited:
	case <-time.After(2 * time.Second):
		t.Fatal("sender did not exit within 2s of Close")
	}
	if d := time.Since(closeStart); d >= 2*time.Second {
		t.Fatalf("Close-to-exit took %v, want < 2s", d)
	}
	// 客户端观察到关闭；服务端读循环退出；goroutine 数回到基线。
	waitForClose(t, cA, 5*time.Second)
	pollLoopExited(t, cA.connectionID, 10*time.Second)
	time.Sleep(500 * time.Millisecond)
	if got := runtime.NumGoroutine(); got > baseGoroutines+2 {
		t.Fatalf("goroutine count = %d, baseline %d (leak > 2)", got, baseGoroutines)
	}
}

// TestT43_ConcurrentWritesIntact：10 goroutine × 100 条编号直接写发往同一
// 客户端，1000 条全部完整到达、无丢失无重复（wsWriteMu 串行化）。
func TestT43_ConcurrentWritesIntact(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register("test-t43-page")
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				sess.sendWSMessageDirect("test_numbered", map[string]interface{}{"n": g*100 + i})
			}
		}(g)
	}
	seen := make(map[int]bool, 1000)
	deadline := time.Now().Add(30 * time.Second)
	for len(seen) < 1000 {
		if time.Now().After(deadline) {
			t.Fatalf("only %d/1000 numbered messages received", len(seen))
		}
		select {
		case msg := <-cA.msgs:
			if msg["type"] != "test_numbered" {
				continue
			}
			n, _ := msgData(msg)["n"].(float64)
			if seen[int(n)] {
				t.Fatalf("duplicate numbered message n=%v", n)
			}
			seen[int(n)] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("stall: only %d/1000 numbered messages received", len(seen))
		}
	}
	wg.Wait()
	for n := 0; n < 1000; n++ {
		if !seen[n] {
			t.Fatalf("numbered message n=%d missing", n)
		}
	}
}

// TestT43_SenderSeqIncreasing：直接构造的 sender 入队 5 条信号，
// 客户端收到的 signalSeq 严格递增。
func TestT43_SenderSeqIncreasing(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register("test-t43b-page")
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	rs := newRecoverySender(sess)
	defer close(rs.done) // 测试构造的 sender 不归 Defer C 管，手动停止。
	for i := 1; i <= 5; i++ {
		rs.enqueue(recoverySignal{Type: "background_task_found", Data: map[string]interface{}{"snapshot": map[string]interface{}{"taskId": fmt.Sprintf("t43-%d", i)}}, RecoveryID: ""})
	}
	var seqs []int64
	for len(seqs) < 5 {
		msg := cA.waitFor("background_task_found", 10*time.Second)
		s, _ := msgData(msg)["signalSeq"].(float64)
		seqs = append(seqs, int64(s))
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("signalSeq not increasing: %v", seqs)
		}
	}
	if seqs[0] <= 0 {
		t.Fatalf("first signalSeq = %d, want > 0", seqs[0])
	}
}

// TestT43_WriteTimestampsLogged：写时间戳只经 testWriteTimestampLog 记录，
// 不断言任何固定时间值——只断言记录到了事件。
func TestT43_WriteTimestampsLogged(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register("test-t43c-page")
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	var mu sync.Mutex
	var events []string
	testHookSet(&testWriteTimestampLog, func(ev string) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	})
	for i := 0; i < 3; i++ {
		sess.sendWSMessageDirect("test_ts", map[string]interface{}{"i": i})
	}
	cA.waitFor("test_ts", 5*time.Second)
	mu.Lock()
	n := len(events)
	mu.Unlock()
	if n == 0 {
		t.Fatal("testWriteTimestampLog recorded no events")
	}
}

// TestT44_QueueFull：sender 写出被 testRecoverySendGate 挡住时，
// 16 条 pending 信号把队列填满，第 17 次查询得 recovery_rejected{queue-full}；
// 释放 gate、排空后重查成功。
func TestT44_QueueFull(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t44-page"
	pubId := "pub-test-t44-page-1"

	cA := r3Dial(t, wsURL) // 见证连接：保持注册与存活，使决议为 pending。
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register(pageId)
	testHookSet(&testRecoverySendGate, make(chan struct{}))
	// 等队列确实攒满 16 条再发第 17 个查询：-race 下读循环处理查询的速度
	// 不定，只靠 20ms 间隔会在第 17 个查询处理时队列仍不足 16 条、
	// 导致它建成第 17 条 pending 而不是被拒（已偶发）。
	queueFull := make(chan struct{})
	var queueFullOnce sync.Once
	queueFull17 := make(chan struct{})
	var queueFull17Once sync.Once
	testHookSet(&testQueueLenGate, func(n int) {
		if n >= 16 {
			queueFullOnce.Do(func() { close(queueFull) })
		}
		if n >= 17 {
			queueFull17Once.Do(func() { close(queueFull17) })
		}
	})
	for i := 1; i <= 16; i++ {
		cB.recoveryQuery(pageId, []string{pubId}, fmt.Sprintf("rec-t44-%d", i), 0)
		time.Sleep(20 * time.Millisecond) // 让每次查询的信号入队。
	}
	select {
	case <-queueFull:
	case <-time.After(10 * time.Second):
		t.Fatal("sender queue did not reach 16")
	}
	cB.recoveryQuery(pageId, []string{pubId}, "rec-t44-17", 0)
	// 等第 17 个查询的响应入队（队列到 17）再释放 gate：此前 gate 一直
	// armed、队列不可能排空，第 17 个 handler 必定看到 queueLen()>=16
	// 而被拒。此前测试在发出查询后立刻关 gate，sender 先排空、handler
	// 后看到不足 16 条，偶发建成第 17 条 pending（全套 -race 下已复现）。
	select {
	case <-queueFull17:
	case <-time.After(10 * time.Second):
		t.Fatal("17th query did not enqueue a response")
	}
	backgroundTaskMutex.Lock()
	nPending := len(pendingRecoveries)
	backgroundTaskMutex.Unlock()
	if nPending != 16 {
		t.Fatalf("pendingRecoveries = %d, want 16 (17th query was not rejected)", nPending)
	}
	// 释放 gate，sender 按 FIFO 写出 16 条 pending + 1 条 rejected。
	close(testHookGet(&testRecoverySendGate))
	testHookSet(&testRecoverySendGate, nil)
	pendings := make(map[string]bool)
	var rejected map[string]interface{}
	deadline := time.Now().Add(10 * time.Second)
	for len(pendings) < 16 || rejected == nil {
		if time.Now().After(deadline) {
			t.Fatalf("drain incomplete: %d/16 pendings, rejected=%v", len(pendings), rejected != nil)
		}
		select {
		case msg := <-cB.msgs:
			switch msg["type"] {
			case "background_task_pending":
				if rid, _ := msgData(msg)["recoveryId"].(string); rid != "" {
					pendings[rid] = true
				}
			case "recovery_rejected":
				rejected = msg
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("stall during drain: %d/16 pendings, rejected=%v", len(pendings), rejected != nil)
		}
	}
	if reason, _ := msgData(rejected)["reason"].(string); reason != "queue-full" {
		t.Fatalf("rejected reason = %q, want queue-full", reason)
	}
	if rid, _ := msgData(rejected)["recoveryId"].(string); rid != "rec-t44-17" {
		t.Fatalf("rejected recoveryId = %q, want rec-t44-17", rid)
	}
	// 排空后重查成功：重查一条已存在的 pending（rec-t44-1）——走 (2) 阶段重算，
	// 仍为 pending，得一条新的 pending 信号。
	// 注意：被拒的 rec-t44-17 从未建成 pending，前端重查也不带意图列表，
	// 重查它会走全新决议得 no-task（设计使然），故不以它作为“重查成功”的用例。
	cB.recoveryRequery("rec-t44-1", 1)
	again := cB.waitFor("background_task_pending", 10*time.Second)
	if rid, _ := msgData(again)["recoveryId"].(string); rid != "rec-t44-1" {
		t.Fatalf("requery pending recoveryId = %q, want rec-t44-1", rid)
	}
}

// TestT44_PendingCapacity：pendingRecoveries 填满 1000 条后，新查询得
// recovery_rejected{pending-capacity}；清理后恢复正常。
func TestT44_PendingCapacity(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t44cap-page"
	pubId := "pub-test-t44cap-page-1"

	cA := r3Dial(t, wsURL) // 见证连接：使决议为 pending。
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register(pageId)

	s, _ := r3RunningSession(t, "test-t44cap-unit")
	backgroundTaskMutex.Lock()
	entries := make([]*pendingRecoveryEntry, 0, 1000)
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("cap-t44-%d", i)
		entries = append(entries, createPendingRecoveryLocked(s, id, "test-t44-cap-page", []string{"pub-cap-1"}, nil, time.Now().Add(90*time.Second), 0))
	}
	if n := len(pendingRecoveries); n != 1000 {
		backgroundTaskMutex.Unlock()
		t.Fatalf("pendingRecoveries = %d, want 1000", n)
	}
	backgroundTaskMutex.Unlock()
	cB.recoveryQuery(pageId, []string{pubId}, "rec-t44cap", 0)
	rej := cB.waitFor("recovery_rejected", 10*time.Second)
	if reason, _ := msgData(rej)["reason"].(string); reason != "pending-capacity" {
		t.Fatalf("rejected reason = %q, want pending-capacity", reason)
	}
	if rid, _ := msgData(rej)["recoveryId"].(string); rid != "rec-t44cap" {
		t.Fatalf("rejected recoveryId = %q, want rec-t44cap", rid)
	}
	backgroundTaskMutex.Lock()
	for i, e := range entries {
		releaseWaiterLocked(fmt.Sprintf("cap-t44-%d", i), e)
	}
	left := len(pendingRecoveries)
	backgroundTaskMutex.Unlock()
	if left != 0 {
		t.Fatalf("pendingRecoveries = %d after cleanup, want 0", left)
	}
	// 清理后同一查询恢复 pending。
	cB.recoveryQuery(pageId, []string{pubId}, "rec-t44cap-2", 0)
	cB.waitFor("background_task_pending", 10*time.Second)
}

// ---------------------------------------------------------------------------
// T45/T46a/T46b/T47/T48（见 §16.2/§7/§6/§10.6）。
// ---------------------------------------------------------------------------

// snapshotBackgroundSession 返回当前后台任务指针（加锁快照，可为 nil）。
func snapshotBackgroundSession(t *testing.T) *appSession {
	t.Helper()
	backgroundTaskMutex.Lock()
	defer backgroundTaskMutex.Unlock()
	return backgroundTaskSession
}

// snapshotTaskVersion 返回当前 taskIdentityVersion（加锁快照）。
func snapshotTaskVersion(t *testing.T) int64 {
	t.Helper()
	backgroundTaskMutex.Lock()
	defer backgroundTaskMutex.Unlock()
	return taskIdentityVersion
}

// publishDirectWithRecover 直接调用 publishBackgroundTask 并捕获 panic
// （gate 触发的测试 panic），返回发布结果与捕获到的 panic 值（无 panic 为 nil）。
func publishDirectWithRecover(s *appSession, publishId, pageId string, conn *websocket.Conn) (res publishResult, recovered interface{}) {
	func() {
		defer func() { recovered = recover() }()
		res = publishBackgroundTask(s, publishId, pageId, conn)
	}()
	return res, recovered
}

// TestT45_ThreeWaitersOnePublish：三条连接查询同一未发布 P 得 pending；
// 直接发布后三方各得一次 found，resolved 缓存三条，ledger refcount 归零。
// 创建期的 pending 不是终态信号（缓存里存的是 found）。
func TestT45_ThreeWaitersOnePublish(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t45-page"
	pubId := "pub-test-t45-page-1"

	clients := make([]*r3Client, 3)
	recIds := []string{"rec-t45-a", "rec-t45-b", "rec-t45-c"}
	for i := 0; i < 3; i++ {
		c := r3Dial(t, wsURL)
		c.readInit(10 * time.Second)
		c.register(pageId)
		clients[i] = c
	}
	for i, c := range clients {
		c.recoveryQuery(pageId, []string{pubId}, recIds[i], 0)
		c.waitFor("background_task_pending", 10*time.Second)
	}
	s, _ := r3RunningSession(t, "test-t45-pub")
	res, rec := publishDirectWithRecover(s, pubId, pageId, nil)
	if rec != nil {
		t.Fatalf("unexpected panic from publish: %v", rec)
	}
	if res.outcome != "committed" || !res.committed {
		t.Fatalf("publish outcome = %q, want committed", res.outcome)
	}
	for i, c := range clients {
		c.waitFor("background_task_found", 10*time.Second)
		if n := c.countType("background_task_found", 500*time.Millisecond); n != 0 {
			t.Fatalf("client %d received %d extra found, want exactly 1", i, n)
		}
		e, ok := snapshotResolved(t, recIds[i])
		if !ok {
			t.Fatalf("resolved cache missing %s", recIds[i])
		}
		if e.signal.Type != "background_task_found" {
			t.Fatalf("resolved[%s].signal.Type = %q, want background_task_found (creation-time pending is not terminal)", recIds[i], e.signal.Type)
		}
	}
	if n, ok := snapshotLedgerRefcount(t, pubId); !ok || n != 0 {
		t.Fatalf("ledger[%s].refcount = %d, want 0", pubId, n)
	}
}

// TestT46a_PanicAfterCommit：提交后、步骤 4 前 panic（testPublishPanicGate）；
// 补救 defer 走 reconcile，等待者仍得 found 恰一次，ledger 已提交、指针已置位。
func TestT46a_PanicAfterCommit(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t46a-page"
	pubId := "pub-test-t46a-page-1"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register(pageId)
	cB.recoveryQuery(pageId, []string{pubId}, "rec-t46a", 0)
	cB.waitFor("background_task_pending", 10*time.Second)

	s, _ := r3RunningSession(t, "test-t46a-pub")
	testHookSet(&testPublishPanicGate, make(chan struct{}))
	_, recovered := publishDirectWithRecover(s, pubId, pageId, nil)
	testHookSet(&testPublishPanicGate, nil)
	if recovered == nil {
		t.Fatal("expected panic from testPublishPanicGate, got none")
	}
	if e, ok := snapshotLedger(t, pubId); !ok || e.state != ledgerStatePublished {
		t.Fatalf("ledger[%s] = %+v, want published", pubId, e)
	}
	if got := snapshotBackgroundSession(t); got != s {
		t.Fatalf("backgroundTaskSession = %v, want the publish session", got)
	}
	cB.waitFor("background_task_found", 10*time.Second)
	if n := cB.countType("background_task_found", 500*time.Millisecond); n != 0 {
		t.Fatalf("waiter received %d extra found, want exactly 1", n)
	}
	if e, ok := snapshotResolved(t, "rec-t46a"); !ok || e.signal.Type != "background_task_found" {
		t.Fatalf("resolved[rec-t46a] = %+v, want found signal", e)
	}
}

// TestT46b_PanicBeforeCommit：提交前 panic（testPublishPanicGate2）；
// 回滚把 ledger 记 failed、等待者得 missing(publish-failed)，
// 但已提交的 Q 与另一条 in-flight 的 R 不受影响；指针为 nil；版本不回退。
func TestT46b_PanicBeforeCommit(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t46b-page"
	pubP := "pub-test-t46b-page-1"
	pubQ := "pub-test-t46b-page-2"
	pubR := "pub-test-t46b-page-3"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register(pageId)
	cB.recoveryQuery(pageId, []string{pubP}, "rec-t46b", 0)
	cB.waitFor("background_task_pending", 10*time.Second)

	// 注意：beginTask 有全局单任务守卫（anyTaskRunningLocked），两个任务不能
	// 同时运行；故 Q 与 P 由同一 session 的同一任务发布（不同 publishId）。
	// 回滚的性质与任务是否相同无关：它只动失败的 publishId 的条目与指针。
	// 同一 taskId 重复发布版本不递增（§6.2），故版本断言为 1 而非 2。
	s, _ := r3RunningSession(t, "test-t46b-pub")
	resQ, recQ := publishDirectWithRecover(s, pubQ, pageId, nil)
	if recQ != nil || resQ.outcome != "committed" {
		t.Fatalf("publish Q outcome = %q, panic = %v, want committed", resQ.outcome, recQ)
	}
	// 另一条 in-flight 的 dispatched 条目 R（直接建 ledger，不经过 publish）。
	backgroundTaskMutex.Lock()
	createDispatchedLedgerLocked(s, pubR, pageId, nil)
	backgroundTaskMutex.Unlock()

	testHookSet(&testPublishPanicGate2, make(chan struct{}))
	_, recovered := publishDirectWithRecover(s, pubP, pageId, nil)
	testHookSet(&testPublishPanicGate2, nil)
	if recovered == nil {
		t.Fatal("expected panic from testPublishPanicGate2, got none")
	}
	if got := snapshotBackgroundSession(t); got != nil {
		t.Fatalf("backgroundTaskSession = %v, want nil after rollback", got)
	}
	if e, ok := snapshotLedger(t, pubP); !ok || e.state != ledgerStateFailed {
		t.Fatalf("ledger[%s] = %+v, want failed", pubP, e)
	}
	if e, ok := snapshotLedger(t, pubQ); !ok || e.state != ledgerStatePublished {
		t.Fatalf("ledger[%s] = %+v, want published (rollback must not touch Q)", pubQ, e)
	}
	if e, ok := snapshotLedger(t, pubR); !ok || e.state != ledgerStateDispatched {
		t.Fatalf("ledger[%s] = %+v, want dispatched (rollback must not touch R)", pubR, e)
	}
	if v := snapshotTaskVersion(t); v != 1 {
		t.Fatalf("taskIdentityVersion = %d, want 1 (Q v1; P 同一 taskId 重发布不递增，见 §6.2)", v)
	}
	miss := cB.waitFor("background_task_missing", 10*time.Second)
	if reason, _ := msgData(miss)["reason"].(string); reason != "publish-failed" {
		t.Fatalf("waiter missing reason = %q, want publish-failed", reason)
	}
	if out, ok := outcomeOf(miss, pubP); !ok || out != "failed" {
		t.Fatalf("waiter missing outcome for P = %q, want failed: %v", out, outcomesOf(miss))
	}
	if n := cB.countType("background_task_missing", 500*time.Millisecond); n != 0 {
		t.Fatalf("waiter received %d extra missing, want exactly 1", n)
	}
	if e, ok := snapshotResolved(t, "rec-t46b"); !ok || e.signal.Type != "background_task_missing" {
		t.Fatalf("resolved[rec-t46b] = %+v, want missing signal", e)
	}
	// 无意图查询 → missing(no-task)：指针已空，且 Q 虽已发布但不再是当前任务。
	cC := r3Dial(t, wsURL)
	cC.readInit(10 * time.Second)
	cC.drainConnectTimeMissing(500 * time.Millisecond)
	cC.register(pageId)
	cC.recoveryQuery(pageId, nil, "rec-t46b-2", 0)
	miss2 := cC.waitFor("background_task_missing", 10*time.Second)
	if reason, _ := msgData(miss2)["reason"].(string); reason != "no-task" {
		t.Fatalf("no-intent missing reason = %q, want no-task", reason)
	}
}

// TestT47_SupersedeThenPublish：X 发布后 Y 的 beginTask 清指针、X 被取代；
// 查询 [X] 得 missing(task-superseded)；再发布 Y 后查 [Y] 得 found(Y)，
// [X] 仍为 superseded。
func TestT47_SupersedeThenPublish(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t47-page"
	pubX := "pub-test-t47-page-1"
	pubY := "pub-test-t47-page-2"

	// X 与 Y 均用直接 session：beginTask 有全局单任务守卫，且
	// clearCompleted 只清已结束任务的指针——X 必须先 endTask，Y 才能开始。
	sX, _ := r3RunningSession(t, "test-t47-x")
	resX, recX := publishDirectWithRecover(sX, pubX, pageId, nil)
	if recX != nil || resX.outcome != "committed" {
		t.Fatalf("publish X outcome = %q, panic = %v, want committed", resX.outcome, recX)
	}
	xTaskId := resX.snapshot.TaskID

	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register(pageId)
	cB.recoveryQuery(pageId, []string{pubX}, "rec-t47-1", 0)
	cB.waitFor("background_task_found", 10*time.Second)

	sX.endTask()
	sY, _ := r3RunningSession(t, "test-t47-y")
	// Y 的 beginTask 清指针、X 取代（v2）。
	if v := snapshotTaskVersion(t); v != 2 {
		t.Fatalf("taskIdentityVersion after Y beginTask = %d, want 2", v)
	}
	cB.recoveryQuery(pageId, []string{pubX}, "rec-t47-2", 0)
	miss := cB.waitFor("background_task_missing", 10*time.Second)
	if reason, _ := msgData(miss)["reason"].(string); reason != "task-superseded" {
		t.Fatalf("missing reason = %q, want task-superseded", reason)
	}
	if out, ok := outcomeOf(miss, pubX); !ok || out != "superseded" {
		t.Fatalf("missing outcome for X = %q, want superseded: %v", out, outcomesOf(miss))
	}
	if v := snapshotTaskVersion(t); msgData(miss)["taskVersion"] != float64(v) {
		t.Fatalf("missing taskVersion = %v, want current %d", msgData(miss)["taskVersion"], v)
	}

	resY, recY := publishDirectWithRecover(sY, pubY, pageId, nil)
	if recY != nil || resY.outcome != "committed" {
		t.Fatalf("publish Y outcome = %q, panic = %v, want committed", resY.outcome, recY)
	}
	yTaskId := resY.snapshot.TaskID
	if yTaskId == "" || yTaskId == xTaskId {
		t.Fatalf("Y taskId = %q, want a distinct non-empty taskId (X=%q)", yTaskId, xTaskId)
	}
	cB.recoveryQuery(pageId, []string{pubY}, "rec-t47-3", 0)
	foundY := cB.waitFor("background_task_found", 10*time.Second)
	snap, _ := msgData(foundY)["snapshot"].(map[string]interface{})
	if tid, _ := snap["taskId"].(string); tid != yTaskId {
		t.Fatalf("found(Y) snapshot taskId = %q, want %q", tid, yTaskId)
	}
	// Y 发布后指针非空，再查 [X] 得的是 found(Y)（C 阶段：指针非空即整体 found），
	// 其 outcomes 中 X 仍为 superseded——missing(superseded) 只在指针为空时出现。
	cB.recoveryQuery(pageId, []string{pubX}, "rec-t47-4", 0)
	foundAfterY := cB.waitFor("background_task_found", 10*time.Second)
	snap2, _ := msgData(foundAfterY)["snapshot"].(map[string]interface{})
	if tid, _ := snap2["taskId"].(string); tid != yTaskId {
		t.Fatalf("re-query X found snapshot taskId = %q, want Y's %q", tid, yTaskId)
	}
	if out, ok := outcomeOf(foundAfterY, pubX); !ok || out != "superseded" {
		t.Fatalf("re-query X outcome = %q, want superseded: %v", out, outcomesOf(foundAfterY))
	}
}

// TestT48_CrossPage：B 以 pageA 声明查询 A 的 P_A 得 found；
// 取代后 B 以 pageB 声明同一 P_A，意图被忽略得 missing(no-task)；
// 从未存在的 PX 声明得 intent-unknown；B 以 pageB 重放 P_A 得 page-mismatch；
// 证据存在时得 publish-lost，reset 后同一查询得 intent-unknown。
func TestT48_CrossPage(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageA := "test-t48-a"
	pageB := "test-t48-b"
	pubA := "pub-test-t48-a-1"

	// A 的发布用直接 session：beginTask 有全局单任务守卫，Y 开始前 A 必须先结束。
	sA, _ := r3RunningSession(t, "test-t48-a")
	resA, recA := publishDirectWithRecover(sA, pubA, pageA, nil)
	if recA != nil || resA.outcome != "committed" {
		t.Fatalf("publish P_A outcome = %q, panic = %v, want committed", resA.outcome, recA)
	}
	aTaskId := resA.snapshot.TaskID

	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register(pageB)
	// 正向：B 以 pageA 声明查询 P_A → found。
	cB.recoveryQuery(pageA, []string{pubA}, "rec-t48-pos", 0)
	found := cB.waitFor("background_task_found", 10*time.Second)
	snap, _ := msgData(found)["snapshot"].(map[string]interface{})
	if tid, _ := snap["taskId"].(string); tid != aTaskId {
		t.Fatalf("cross-page found snapshot taskId = %q, want A's %q", tid, aTaskId)
	}

	// 取代：A 先结束任务，Y 的 beginTask 才能成功并清指针、P_A 被取代。
	sA.endTask()
	_, _ = r3RunningSession(t, "test-t48-y")
	// 反向：B 以 pageB 声明同一 P_A → 意图被忽略（ledger pageId≠查询 pageId）。
	cB.recoveryQuery(pageB, []string{pubA}, "rec-t48-rev", 0)
	miss := cB.waitFor("background_task_missing", 10*time.Second)
	if reason, _ := msgData(miss)["reason"].(string); reason != "no-task" {
		t.Fatalf("reverse missing reason = %q, want no-task", reason)
	}
	if _, ok := outcomeOf(miss, pubA); ok {
		t.Fatalf("reverse missing should not carry an outcome for ignored %s: %v", pubA, outcomesOf(miss))
	}
	// 见证 1：从未存在的 PX 声明 → intent-unknown。
	cB.recoveryQuery("PX", []string{"pub-PX-9"}, "rec-t48-w1", 0)
	missW1 := cB.waitFor("background_task_missing", 10*time.Second)
	if out, ok := outcomeOf(missW1, "pub-PX-9"); !ok || out != "intent-unknown" {
		t.Fatalf("witness-1 outcome = %q, want intent-unknown: %v", out, outcomesOf(missW1))
	}
	if reason, _ := msgData(missW1)["reason"].(string); reason != "intent-unknown" {
		t.Fatalf("witness-1 reason = %q, want intent-unknown", reason)
	}
	// 第三：B 以 pageB 重放 P_A → page-mismatch，ledger[P_A] 不变。
	before, _ := snapshotLedger(t, pubA)
	cB.send("run_in_background", map[string]interface{}{"publishId": pubA, "pageId": pageB})
	fail := cB.waitFor("background_task_publish_failed", 10*time.Second)
	if reason, _ := msgData(fail)["reason"].(string); reason != "page-mismatch" {
		t.Fatalf("replay failed reason = %q, want page-mismatch", reason)
	}
	after, _ := snapshotLedger(t, pubA)
	if before.state != after.state || before.version != after.version || before.taskId != after.taskId {
		t.Fatalf("ledger[%s] changed by the replay: %+v -> %+v", pubA, before, after)
	}
	// 见证 2（最后，含 reset）：PX 证据存在时得 publish-lost，reset 后同一查询得 intent-unknown。
	cC := r3Dial(t, wsURL)
	cC.readInit(10 * time.Second)
	cC.drainConnectTimeMissing(500 * time.Millisecond)
	cC.register("PX")
	cC.recoveryQuery("PX", []string{"pub-PX-9"}, "rec-t48-w2a", 0)
	missW2a := cC.waitFor("background_task_missing", 10*time.Second)
	if out, ok := outcomeOf(missW2a, "pub-PX-9"); !ok || out != "publish-lost" {
		t.Fatalf("witness-2a outcome = %q, want publish-lost: %v", out, outcomesOf(missW2a))
	}
	resetBackendGeneration()
	cD := r3Dial(t, wsURL)
	cD.readInit(10 * time.Second)
	cD.drainConnectTimeMissing(500 * time.Millisecond)
	cD.recoveryQuery("PX", []string{"pub-PX-9"}, "rec-t48-w2b", 0)
	missW2b := cD.waitFor("background_task_missing", 10*time.Second)
	if out, ok := outcomeOf(missW2b, "pub-PX-9"); !ok || out != "intent-unknown" {
		t.Fatalf("witness-2b outcome = %q, want intent-unknown: %v", out, outcomesOf(missW2b))
	}
	// 注：T38 A4 单元（旧代次 publish → generation-mismatch）已覆盖 §10.6 的
	// 第四种情形，本测试不再重复。
}

// ---------------------------------------------------------------------------
// T49/T50/T51 与 §11.5.10 测试 A–D（见 §16.2/§11.5）。
// ---------------------------------------------------------------------------

// assertBackgroundTaskMutexReleased 断言锁已释放（双重 panic 后 Unlock
// defer 仍执行）：另一 goroutine 在时限内能取到锁。
func assertBackgroundTaskMutexReleased(t *testing.T, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		backgroundTaskMutex.Lock()
		close(done)
		backgroundTaskMutex.Unlock()
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("backgroundTaskMutex still held after %v", timeout)
	}
}

// TestT49_ConcurrentSnapshotReads：10 个读 goroutine 在四个阶段边界上
// 并发读（原子快照 + backgroundSummary），观测到的 (taskId, version, epoch,
// running) 三元组只能是 (X,v1,E,true)、(X,v1,E,false)、(Y,v3,E,true) 三种，
// 绝无 (Y,0)/(X,v3)/(Y,v1) 等撕裂组合；主断言是 -race 下干净。
func TestT49_ConcurrentSnapshotReads(t *testing.T) {
	r3Setup(t)
	epoch := currentBackendEpoch()
	pageId := "test-t49-page"
	pubX := "pub-test-t49-page-1"
	pubY := "pub-test-t49-page-2"

	sX, _ := r3RunningSession(t, "test-t49-x")
	resX, recX := publishDirectWithRecover(sX, pubX, pageId, nil)
	if recX != nil || resX.outcome != "committed" {
		t.Fatalf("publish X outcome = %q, panic = %v, want committed", resX.outcome, recX)
	}
	xTaskId := resX.snapshot.TaskID
	ledX, _ := snapshotLedger(t, pubX)
	v1 := ledX.version

	type triple struct {
		taskId  string
		version int64
		epoch   string
		running bool
	}
	var mu sync.Mutex
	var seen []triple
	record := func(tr triple) {
		mu.Lock()
		seen = append(seen, tr)
		mu.Unlock()
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				backgroundTaskMutex.Lock()
				snap, snapEpoch, snapVersion, ok := currentBackgroundTaskSnapshotLocked()
				if ok {
					record(triple{snap.TaskID, snapVersion, snapEpoch, snap.Running})
					// 第二条读路径：同一持有内再读 backgroundSummary。
					if sess := backgroundTaskSession; sess != nil {
						sum := sess.backgroundSummary()
						record(triple{sum.TaskID, sum.TaskVersion, sum.BackendEpoch, sum.Running})
					}
				}
				backgroundTaskMutex.Unlock()
				time.Sleep(time.Millisecond)
			}
		}()
	}
	time.Sleep(300 * time.Millisecond) // 阶段 1：X 已发布、运行中。
	sX.endTask()
	time.Sleep(300 * time.Millisecond) // 阶段 2：X 已结束（Running=false），指针仍在。
	sY, _ := r3RunningSession(t, "test-t49-y")
	time.Sleep(300 * time.Millisecond) // 阶段 3：指针为空（Y 未发布），读到 ok=false、跳过。
	resY, recY := publishDirectWithRecover(sY, pubY, pageId, nil)
	if recY != nil || resY.outcome != "committed" {
		t.Fatalf("publish Y outcome = %q, panic = %v, want committed", resY.outcome, recY)
	}
	yTaskId := resY.snapshot.TaskID
	ledY, _ := snapshotLedger(t, pubY)
	v3 := ledY.version
	time.Sleep(300 * time.Millisecond) // 阶段 4：Y 已发布。
	close(stop)
	wg.Wait()

	allowed := map[triple]bool{
		{xTaskId, v1, epoch, true}:  true,
		{xTaskId, v1, epoch, false}: true,
		{yTaskId, v3, epoch, true}:  true,
	}
	counts := map[triple]int{}
	for _, tr := range seen {
		if !allowed[tr] {
			t.Fatalf("torn snapshot observed: %+v (allowed: %+v)", tr, allowed)
		}
		counts[tr]++
	}
	if len(seen) == 0 {
		t.Fatal("no snapshots observed at all")
	}
	for want, n := range map[triple]int{
		{xTaskId, v1, epoch, true}:  1,
		{xTaskId, v1, epoch, false}: 1,
		{yTaskId, v3, epoch, true}:  1,
	} {
		if counts[want] == 0 {
			t.Fatalf("phase not sampled: %+v (counts=%v)", want, counts)
		}
		_ = n
	}
}

// TestT50_RollbackHalfState：提交前 panic + 回滚内 panic 形成半态 R1——
// 锁已释放、指针为空、ledger 仍 dispatched、refcount 保持 1、等待者未得终态信号；
// 重查仍 pending；条目老化 61s 后 sweeper 将其置 failed、等待者得 missing；
// 此后新发布仍可正常完成。
func TestT50_RollbackHalfState(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t50-page"
	pubId := "pub-test-t50-page-1"
	pubId2 := "pub-test-t50-page-2"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register(pageId)
	cB.recoveryQuery(pageId, []string{pubId}, "rec-t50", 0)
	cB.waitFor("background_task_pending", 10*time.Second)

	s, _ := r3RunningSession(t, "test-t50-pub")
	testHookSet(&testPublishPanicGate2, make(chan struct{}))
	testHookSet(&testRollbackPanicGate, make(chan struct{}))
	_, recovered := publishDirectWithRecover(s, pubId, pageId, nil)
	testHookSet(&testPublishPanicGate2, nil)
	testHookSet(&testRollbackPanicGate, nil)
	if recovered != "test: rollback panic" {
		t.Fatalf("recovered panic = %v, want the rollback panic (gate2 panic must be replaced by the rollback panic)", recovered)
	}
	assertBackgroundTaskMutexReleased(t, 5*time.Second)
	if got := snapshotBackgroundSession(t); got != nil {
		t.Fatalf("backgroundTaskSession = %v, want nil after rollback", got)
	}
	if e, ok := snapshotLedger(t, pubId); !ok || e.state != ledgerStateDispatched {
		t.Fatalf("ledger[%s] = %+v, want still dispatched (half-state R1)", pubId, e)
	}
	if n, ok := snapshotLedgerRefcount(t, pubId); !ok || n != 1 {
		t.Fatalf("ledger[%s].refcount = %d, want 1", pubId, n)
	}
	if _, ok := snapshotResolved(t, "rec-t50"); ok {
		t.Fatal("resolved[rec-t50] exists, want none (waiter got no terminal signal)")
	}
	if n := cB.countType("background_task_found", 500*time.Millisecond); n != 0 {
		t.Fatalf("waiter received %d found, want 0", n)
	}
	if n := cB.countType("background_task_missing", 500*time.Millisecond); n != 0 {
		t.Fatalf("waiter received %d missing, want 0", n)
	}
	// 重查：pending 仍在，B 阶段重算仍为 pending（dispatched）。
	cB.recoveryRequery("rec-t50", 1)
	again := cB.waitFor("background_task_pending", 10*time.Second)
	if rid, _ := msgData(again)["recoveryId"].(string); rid != "rec-t50" {
		t.Fatalf("requery pending recoveryId = %q, want rec-t50", rid)
	}
	// 条目老化 61s（>60s 孤儿阈值），sweeper 将其置 failed 并决议等待者。
	backgroundTaskMutex.Lock()
	if e, ok := publishLedger[pubId]; ok {
		e.createdAt = time.Now().Add(-61 * time.Second)
		publishLedger[pubId] = e
	}
	sweepOnceLocked()
	backgroundTaskMutex.Unlock()
	if e, ok := snapshotLedger(t, pubId); !ok || e.state != ledgerStateFailed {
		t.Fatalf("ledger[%s] = %+v, want failed after sweep", pubId, e)
	}
	miss := cB.waitFor("background_task_missing", 10*time.Second)
	if reason, _ := msgData(miss)["reason"].(string); reason != "publish-failed" {
		t.Fatalf("waiter missing reason = %q, want publish-failed", reason)
	}
	if out, ok := outcomeOf(miss, pubId); !ok || out != "failed" {
		t.Fatalf("waiter missing outcome = %q, want failed: %v", out, outcomesOf(miss))
	}
	if n, ok := snapshotLedgerRefcount(t, pubId); !ok || n != 0 {
		t.Fatalf("ledger[%s].refcount = %d, want 0 after sweep", pubId, n)
	}
	// 半态未卡住锁与 ledger：新 publishId 正常完成。
	res2, rec2 := publishDirectWithRecover(s, pubId2, pageId, nil)
	if rec2 != nil || res2.outcome != "committed" {
		t.Fatalf("fresh publish outcome = %q, panic = %v, want committed", res2.outcome, rec2)
	}
}

// TestT51_ReconcileHalfState：提交后 panic + reconcile 中途 panic 形成半态 C1——
// W1 已决议（found 恰一次、缓存已写），W2 未决议（无终态信号、无缓存、
// pending 仍在、refcount 为 1）；W2 重查得 found（首个终态信号）；
// 此后新发布仍可正常完成。
func TestT51_ReconcileHalfState(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageId := "test-t51-page"
	pubId := "pub-test-t51-page-1"
	pubId2 := "pub-test-t51-page-2"

	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageId)
	cW1 := r3Dial(t, wsURL)
	cW1.readInit(10 * time.Second)
	cW1.register(pageId)
	cW1.recoveryQuery(pageId, []string{pubId}, "rec-t51-1", 0)
	cW1.waitFor("background_task_pending", 10*time.Second)
	cW2 := r3Dial(t, wsURL)
	cW2.readInit(10 * time.Second)
	cW2.register(pageId)
	cW2.recoveryQuery(pageId, []string{pubId}, "rec-t51-2", 0)
	cW2.waitFor("background_task_pending", 10*time.Second)

	s, _ := r3RunningSession(t, "test-t51-pub")
	testHookSet(&testPublishPanicGate, make(chan struct{}))
	testHookSet(&testReconcilePanicGate, &reconcilePanicGate{targetRecoveryId: "rec-t51-1"})
	_, recovered := publishDirectWithRecover(s, pubId, pageId, nil)
	testHookSet(&testPublishPanicGate, nil)
	testHookSet(&testReconcilePanicGate, nil)
	if recovered != "test: reconcile panic" {
		t.Fatalf("recovered panic = %v, want the reconcile panic", recovered)
	}
	assertBackgroundTaskMutexReleased(t, 5*time.Second)
	if e, ok := snapshotLedger(t, pubId); !ok || e.state != ledgerStatePublished {
		t.Fatalf("ledger[%s] = %+v, want published", pubId, e)
	}
	if got := snapshotBackgroundSession(t); got != s {
		t.Fatalf("backgroundTaskSession = %v, want the publish session", got)
	}
	cW1.waitFor("background_task_found", 10*time.Second)
	if n := cW1.countType("background_task_found", 500*time.Millisecond); n != 0 {
		t.Fatalf("W1 received %d extra found, want exactly 1", n)
	}
	if e, ok := snapshotResolved(t, "rec-t51-1"); !ok || e.signal.Type != "background_task_found" {
		t.Fatalf("resolved[rec-t51-1] = %+v, want found signal", e)
	}
	if n := cW2.countType("background_task_found", 500*time.Millisecond); n != 0 {
		t.Fatalf("W2 received %d found after the panic, want 0", n)
	}
	if _, ok := snapshotResolved(t, "rec-t51-2"); ok {
		t.Fatal("resolved[rec-t51-2] exists, want none")
	}
	if exists, _, _ := snapshotPending(t, "rec-t51-2"); !exists {
		t.Fatal("pending[rec-t51-2] gone, want it still present")
	}
	if n, ok := snapshotLedgerRefcount(t, pubId); !ok || n != 1 {
		t.Fatalf("ledger[%s].refcount = %d, want 1 (W2 still waiting)", pubId, n)
	}
	// W2 重查：B 阶段重算得 found（首个终态信号）。
	cW2.recoveryRequery("rec-t51-2", 1)
	cW2.waitFor("background_task_found", 10*time.Second)
	if n := cW2.countType("background_task_found", 500*time.Millisecond); n != 0 {
		t.Fatalf("W2 received %d extra found, want exactly 1", n)
	}
	if n, ok := snapshotLedgerRefcount(t, pubId); !ok || n != 0 {
		t.Fatalf("ledger[%s].refcount = %d, want 0", pubId, n)
	}
	if exists, _, _ := snapshotPending(t, "rec-t51-1"); exists {
		t.Fatal("pending[rec-t51-1] still exists")
	}
	if exists, _, _ := snapshotPending(t, "rec-t51-2"); exists {
		t.Fatal("pending[rec-t51-2] still exists")
	}
	if _, ok := snapshotResolved(t, "rec-t51-1"); !ok {
		t.Fatal("resolved[rec-t51-1] missing")
	}
	if _, ok := snapshotResolved(t, "rec-t51-2"); !ok {
		t.Fatal("resolved[rec-t51-2] missing")
	}
	// 半态未卡住：新 publishId 正常完成。
	res2, rec2 := publishDirectWithRecover(s, pubId2, pageId, nil)
	if rec2 != nil || res2.outcome != "committed" {
		t.Fatalf("fresh publish outcome = %q, panic = %v, want committed", res2.outcome, rec2)
	}
}

// --- §11.5.10 测试 A–D：refcount 的创建/递减与 identity 守卫 ---
// 全部在测试持有的 backgroundTaskMutex 内、只调生产原语
// （createPendingRecoveryLocked / createDispatchedLedgerLocked /
// releaseWaiterLocked）；绝不在持锁时调 publishBackgroundTask 或
// recovery_query handler（会自死锁）。

// TestRefcount_A_PendingFirst：先建 pending [P,P]（去重后长度 1），
// 再建 dispatched ledger（refcount=1）；释放后 refcount=0、无缓存。
func TestRefcount_A_PendingFirst(t *testing.T) {
	r3Setup(t)
	s, _ := r3RunningSession(t, "test-115a-unit")
	pageId := "test-115a-page"
	pubId := "pub-test-115a-page-1"

	backgroundTaskMutex.Lock()
	defer backgroundTaskMutex.Unlock()
	entry := createPendingRecoveryLocked(s, "rec-115a", pageId, []string{pubId, pubId}, nil, time.Now().Add(90*time.Second), 0)
	if len(entry.waitForPublishIds) != 1 {
		t.Fatalf("pending waitFor list = %v, want length 1 (dedup)", entry.waitForPublishIds)
	}
	createDispatchedLedgerLocked(s, pubId, pageId, nil)
	if e, ok := publishLedger[pubId]; !ok || e.state != ledgerStateDispatched {
		t.Fatalf("ledger[%s] = %+v, want dispatched", pubId, e)
	}
	if publishLedger[pubId].refcount != 1 {
		t.Fatalf("ledger[%s].refcount = %d, want 1", pubId, publishLedger[pubId].refcount)
	}
	if _, ok := pendingRecoveries["rec-115a"]; !ok {
		t.Fatal("pending[rec-115a] missing after ledger creation")
	}
	if _, ok := resolvedRecoveries["rec-115a"]; ok {
		t.Fatal("resolved[rec-115a] exists, want none")
	}
	releaseWaiterLocked("rec-115a", entry)
	if publishLedger[pubId].refcount != 0 {
		t.Fatalf("ledger[%s].refcount = %d after release, want 0", pubId, publishLedger[pubId].refcount)
	}
	if _, ok := pendingRecoveries["rec-115a"]; ok {
		t.Fatal("pending[rec-115a] still exists after release")
	}
	if _, ok := resolvedRecoveries["rec-115a"]; ok {
		t.Fatal("resolved[rec-115a] exists after release, want none")
	}
}

// TestRefcount_B_LedgerFirst：先建 ledger（refcount=0），再建 pending [P,P]——
// 列表长度 1 且 refcount=1（不是 2）；释放后归零、无缓存。
func TestRefcount_B_LedgerFirst(t *testing.T) {
	r3Setup(t)
	s, _ := r3RunningSession(t, "test-115b-unit")
	pageId := "test-115b-page"
	pubId := "pub-test-115b-page-1"

	backgroundTaskMutex.Lock()
	defer backgroundTaskMutex.Unlock()
	createDispatchedLedgerLocked(s, pubId, pageId, nil)
	if publishLedger[pubId].refcount != 0 {
		t.Fatalf("ledger[%s].refcount = %d, want 0 (no waiters yet)", pubId, publishLedger[pubId].refcount)
	}
	entry := createPendingRecoveryLocked(s, "rec-115b", pageId, []string{pubId, pubId}, nil, time.Now().Add(90*time.Second), 0)
	if len(entry.waitForPublishIds) != 1 {
		t.Fatalf("pending waitFor list = %v, want length 1 (dedup)", entry.waitForPublishIds)
	}
	if publishLedger[pubId].refcount != 1 {
		t.Fatalf("ledger[%s].refcount = %d, want 1 (not 2)", pubId, publishLedger[pubId].refcount)
	}
	releaseWaiterLocked("rec-115b", entry)
	if publishLedger[pubId].refcount != 0 {
		t.Fatalf("ledger[%s].refcount = %d after release, want 0", pubId, publishLedger[pubId].refcount)
	}
	if _, ok := resolvedRecoveries["rec-115b"]; ok {
		t.Fatal("resolved[rec-115b] exists after release, want none")
	}
}

// TestRefcount_C_TwoWaiters：两条 pending 等同一 P，ledger refcount=2；
// 逐条释放 2→1→0。
func TestRefcount_C_TwoWaiters(t *testing.T) {
	r3Setup(t)
	s, _ := r3RunningSession(t, "test-115c-unit")
	pageId := "test-115c-page"
	pubId := "pub-test-115c-page-1"

	backgroundTaskMutex.Lock()
	defer backgroundTaskMutex.Unlock()
	e1 := createPendingRecoveryLocked(s, "rec-115c-1", pageId, []string{pubId}, nil, time.Now().Add(90*time.Second), 0)
	e2 := createPendingRecoveryLocked(s, "rec-115c-2", pageId, []string{pubId}, nil, time.Now().Add(90*time.Second), 0)
	createDispatchedLedgerLocked(s, pubId, pageId, nil)
	if publishLedger[pubId].refcount != 2 {
		t.Fatalf("ledger[%s].refcount = %d, want 2", pubId, publishLedger[pubId].refcount)
	}
	releaseWaiterLocked("rec-115c-1", e1)
	if publishLedger[pubId].refcount != 1 {
		t.Fatalf("ledger[%s].refcount = %d after releasing W1, want 1", pubId, publishLedger[pubId].refcount)
	}
	if _, ok := pendingRecoveries["rec-115c-1"]; ok {
		t.Fatal("pending[rec-115c-1] still exists after release")
	}
	if _, ok := pendingRecoveries["rec-115c-2"]; !ok {
		t.Fatal("pending[rec-115c-2] gone, want it present")
	}
	releaseWaiterLocked("rec-115c-2", e2)
	if publishLedger[pubId].refcount != 0 {
		t.Fatalf("ledger[%s].refcount = %d after releasing W2, want 0", pubId, publishLedger[pubId].refcount)
	}
}

// TestRefcount_D_IdentityGuard：错配的 (recoveryId, entry) 调用被 identity
// 检查拦截（两条 pending 都在、refcount 不变、无缓存）；正确释放后归零，
// 重复释放不再递减。
func TestRefcount_D_IdentityGuard(t *testing.T) {
	r3Setup(t)
	s, _ := r3RunningSession(t, "test-115d-unit")
	pageId := "test-115d-page"
	pubId := "pub-test-115d-page-1"

	backgroundTaskMutex.Lock()
	defer backgroundTaskMutex.Unlock()
	createDispatchedLedgerLocked(s, pubId, pageId, nil)
	eA := createPendingRecoveryLocked(s, "rec-115d-a", pageId, []string{pubId}, nil, time.Now().Add(90*time.Second), 0)
	if publishLedger[pubId].refcount != 1 {
		t.Fatalf("ledger[%s].refcount = %d, want 1", pubId, publishLedger[pubId].refcount)
	}
	eB := createPendingRecoveryLocked(s, "rec-115d-b", pageId, []string{pubId}, nil, time.Now().Add(90*time.Second), 0)
	if publishLedger[pubId].refcount != 2 {
		t.Fatalf("ledger[%s].refcount = %d, want 2", pubId, publishLedger[pubId].refcount)
	}
	// 错配：(rec-A, entryB) 应被拦截，什么都不发生。
	releaseWaiterLocked("rec-115d-a", eB)
	if _, ok := pendingRecoveries["rec-115d-a"]; !ok {
		t.Fatal("pending[rec-115d-a] gone after mismatched release, want present")
	}
	if _, ok := pendingRecoveries["rec-115d-b"]; !ok {
		t.Fatal("pending[rec-115d-b] gone after mismatched release, want present")
	}
	if publishLedger[pubId].refcount != 2 {
		t.Fatalf("ledger[%s].refcount = %d after mismatched release, want 2", pubId, publishLedger[pubId].refcount)
	}
	if _, ok := resolvedRecoveries["rec-115d-a"]; ok {
		t.Fatal("resolved[rec-115d-a] exists after mismatched release, want none")
	}
	// 正确释放。
	releaseWaiterLocked("rec-115d-a", eA)
	if publishLedger[pubId].refcount != 1 {
		t.Fatalf("ledger[%s].refcount = %d after releasing A, want 1", pubId, publishLedger[pubId].refcount)
	}
	if _, ok := pendingRecoveries["rec-115d-a"]; ok {
		t.Fatal("pending[rec-115d-a] still exists after release")
	}
	if _, ok := pendingRecoveries["rec-115d-b"]; !ok {
		t.Fatal("pending[rec-115d-b] gone, want present")
	}
	releaseWaiterLocked("rec-115d-b", eB)
	if publishLedger[pubId].refcount != 0 {
		t.Fatalf("ledger[%s].refcount = %d after releasing B, want 0", pubId, publishLedger[pubId].refcount)
	}
	// 重复释放同一条目：identity 检查拦截，refcount 保持 0。
	releaseWaiterLocked("rec-115d-a", eA)
	if publishLedger[pubId].refcount != 0 {
		t.Fatalf("ledger[%s].refcount = %d after double release, want 0", pubId, publishLedger[pubId].refcount)
	}
}

// --- S6 结果重放（T6-01～T6-25；计划 v4.1 §5）---
//
// 约定：Go 测试分两类——(a) 直接会话测试：用 t6BG 创建后台任务会话，
// 以 emit 捕获发送（不经过真实 socket），确定性驱动 sendTaskResult /
// replayResults；(b) 真实 WS 测试：r3Server + r3Dial + follow，走完整
// 的 replay_results 协议。T6-07/17b/20/25 与 T6-04/15 的前端部分在
// phase3-review-tests/test_s6_replay.js（前端桩）中覆盖。

// t6EmitCapture 捕获直接会话的 emit 输出（线程安全）。
type t6EmitMsg struct {
	typ  string
	data interface{}
}

type t6EmitCapture struct {
	mu   sync.Mutex
	msgs []t6EmitMsg
}

func (c *t6EmitCapture) emit(typ string, data interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, t6EmitMsg{typ, data})
}

func (c *t6EmitCapture) count(typ string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, m := range c.msgs {
		if m.typ == typ {
			n++
		}
	}
	return n
}

func (c *t6EmitCapture) all() []t6EmitMsg {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]t6EmitMsg, len(c.msgs))
	copy(out, c.msgs)
	return out
}

// t6SeqOf 从一条已发送载荷中提取 seq（ScanResult/TestResult/nsbScanMessage
// 的 Seq 字段，或 map[string]string 的 "seq" 键）。
func t6SeqOf(data interface{}) (int64, bool) {
	switch v := data.(type) {
	case ScanResult:
		return v.Seq, true
	case TestResult:
		return v.Seq, true
	case nsbScanMessage:
		return v.Seq, true
	case map[string]string:
		n, err := strconv.ParseInt(v["seq"], 10, 64)
		return n, err == nil
	case map[string]interface{}:
		// seq 是 JSON number：生产路径 withSeq 写入 int64；经 JSON 往返后为 float64。
		switch n := v["seq"].(type) {
		case int64:
			return n, true
		case float64:
			return int64(n), true
		case int:
			return int64(n), true
		}
		return 0, false
	case json.RawMessage:
		// 重放行的 emit 数据是日志 Data（原始 JSON 字节）：先解析再取 seq。
		var d map[string]interface{}
		if err := json.Unmarshal(v, &d); err != nil {
			return 0, false
		}
		if f, ok := d["seq"].(float64); ok {
			return int64(f), true
		}
		return 0, false
	}
	return 0, false
}

// t6BG 创建一个后台任务会话（beginTask + enableBackgroundTask），并注册
// 清理（cancel + endTask + 清指针 + 复位 hook）。返回会话与 cancel。
// t6Num 从 emit 捕获的载荷里取一个数字字段：生产经 emit 透传 Go 值时为
// int64，经 JSON（真实 socket）往返后为 float64；两种都接受。
func t6Num(m map[string]interface{}, key string) (int64, bool) {
	switch v := m[key].(type) {
	case int64:
		return v, true
	case float64:
		return int64(v), true
	case int:
		return int64(v), true
	}
	return 0, false
}

// t6Ctx 返回绑定到 sess 当前任务日志的 ctx（模拟 startTaskNamed 在 beginTask
// 成功后的绑定；见 session.go withTaskLog）。正常 T6 测试用它；陈旧 worker
// 测试显式传入旧任务开始时捕获的 ctx，以验证身份校验丢弃旧发送。
func t6Ctx(sess *appSession) context.Context {
	return context.WithValue(context.Background(), taskLogKey, sess.resultLog.Load())
}

func t6BG(t *testing.T, connID string) (*appSession, context.CancelFunc) {
	t.Helper()
	r3Setup(t)
	bg := &appSession{connectionID: connID, backendGeneration: atomic.LoadUint64(&backendGeneration)}
	_, cancel := context.WithCancel(context.Background())
	if _, ok := bg.beginTask(cancel, "t6 test task", "official", nil); !ok {
		t.Fatal("beginTask failed")
	}
	if !bg.enableBackgroundTask() {
		t.Fatal("enableBackgroundTask failed")
	}
	t.Cleanup(func() {
		cancel()
		bg.endTask()
		clearCompletedBackgroundTaskSession()
		testWriteDelayGate = nil
		testEndTaskEnteredHook = nil
		testReplayPauseGate = nil
	})
	return bg, cancel
}

// t6Follow 让 r3Client 跟随后台任务：等 init_config 与 background_task_found，
// 发 follow_background_task，等 background_task_following。返回 init_config
// 的 backendEpoch。
func t6Follow(t *testing.T, r *t6Reader) string {
	t.Helper()
	initCfg := r.waitFor("init_config", 10*time.Second)
	epoch, _ := msgData(initCfg)["backendEpoch"].(string)
	if epoch == "" {
		t.Fatal("init_config 缺少 backendEpoch")
	}
	r.waitFor("background_task_found", 5*time.Second)
	r.c.send("follow_background_task", map[string]interface{}{})
	r.waitFor("background_task_following", 5*time.Second)
	return epoch
}

// t6SendReplay 让客户端发送 replay_results 请求。
func t6SendReplay(c *r3Client, taskID, epoch string, lastSeq int64, requestID string) {
	c.send("replay_results", map[string]interface{}{
		"taskId":       taskID,
		"backendEpoch": epoch,
		"lastSeq":      lastSeq,
		"requestId":    requestID,
	})
}

// t6Reader 是带暂存缓冲的测试客户端读端：collectRows / waitFor / waitForAny
// 在读取时遇到的非目标消息暂存入 pending，下次读取时优先返回，不丢弃。
// （r3Client.msgs 是裸 channel，读出即消失；直接丢弃非目标消息会丢掉
// 已到达的 replay_complete，见 T6-17a 的修复。）
type t6Reader struct {
	t       *testing.T
	c       *r3Client
	pending []map[string]interface{}
}

func t6NewReader(t *testing.T, c *r3Client) *t6Reader {
	return &t6Reader{t: t, c: c}
}

// takePending 取出 pending 中第一个指定类型的消息（按到达顺序）。
// 非目标类型的暂存消息不会被重复取出，避免 waitFor/collectRows 在同一
// 次调用内反复读到同一条不匹配消息而空转。
func (r *t6Reader) takePending(typ string) (map[string]interface{}, bool) {
	for i, m := range r.pending {
		if t, _ := m["type"].(string); t == typ {
			r.pending = append(r.pending[:i], r.pending[i+1:]...)
			return m, true
		}
	}
	return nil, false
}

// next 只从通道读一条消息（带超时）；pending 由 takePending 按类型取用。
func (r *t6Reader) next(timeout time.Duration) (map[string]interface{}, bool) {
	deadline := time.After(timeout)
	select {
	case msg, ok := <-r.c.msgs:
		if !ok {
			return nil, false
		}
		return msg, true
	case <-deadline:
		return nil, false
	}
}

// collectRows 收集 n 条指定类型的行（跳过其他类型并暂存），返回每条的 seq。
// 全局截止时间：整个收集过程共享一个 timeout，不再按条重置。
func (r *t6Reader) collectRows(typ string, n int, timeout time.Duration) []int64 {
	r.t.Helper()
	seqs := make([]int64, 0, n)
	deadline := time.Now().Add(timeout)
	for len(seqs) < n {
		if m, ok := r.takePending(typ); ok {
			seq, ok := t6SeqOf(msgData(m))
			if !ok {
				r.t.Fatalf("%s row 缺少 seq: %v", typ, m)
			}
			seqs = append(seqs, seq)
			continue
		}
		rem := time.Until(deadline)
		if rem <= 0 {
			r.t.Fatalf("timeout collecting %d %s rows (got %v)", n, typ, seqs)
		}
		msg, ok := r.next(rem)
		if !ok {
			r.t.Fatalf("timeout collecting %d %s rows (got %v)", n, typ, seqs)
		}
		if mt, _ := msg["type"].(string); mt != typ {
			r.pending = append(r.pending, msg)
			continue
		}
		seq, ok := t6SeqOf(msgData(msg))
		if !ok {
			r.t.Fatalf("%s row 缺少 seq: %v", typ, msg)
		}
		seqs = append(seqs, seq)
	}
	return seqs
}

// waitFor 等待指定类型的下一条消息（跳过其他类型并暂存），超时即失败。
func (r *t6Reader) waitFor(typ string, timeout time.Duration) map[string]interface{} {
	r.t.Helper()
	if m, ok := r.takePending(typ); ok {
		return m
	}
	for {
		msg, ok := r.next(timeout)
		if !ok {
			r.t.Fatalf("timeout waiting for %s", typ)
		}
		if t, _ := msg["type"].(string); t == typ {
			return msg
		}
		r.pending = append(r.pending, msg)
	}
}

// waitForAny 等待多个类型中任意一个的下一条消息，返回实际类型与消息。
func (r *t6Reader) waitForAny(timeout time.Duration, types ...string) (string, map[string]interface{}) {
	r.t.Helper()
	for _, typ := range types {
		if m, ok := r.takePending(typ); ok {
			return typ, m
		}
	}
	want := make(map[string]bool, len(types))
	for _, typ := range types {
		want[typ] = true
	}
	for {
		msg, ok := r.next(timeout)
		if !ok {
			r.t.Fatalf("timeout waiting for any of %v", types)
		}
		if t, _ := msg["type"].(string); want[t] {
			return t, msg
		}
		r.pending = append(r.pending, msg)
	}
}

// t6LogLen 读日志行数（持 resultLog.mu）。
func t6LogLen(log *taskResultLog) int {
	log.mu.Lock()
	defer log.mu.Unlock()
	return len(log.rows)
}

// t6LogNextSeq 读 nextSeq（持 resultLog.mu）。
func t6LogNextSeq(log *taskResultLog) int64 {
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.nextSeq
}

// T6-01——正常连接，结果实时接收：直接调 sendTaskResult 产生 5 行 scan_result。
func TestT6_01_RealtimeRows(t *testing.T) {
	bg, _ := t6BG(t, "t6-01")
	cap := &t6EmitCapture{}
	bg.emit = cap.emit
	for i := 0; i < 5; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("1.1.1.%d", i+1), Port: 443})
	}
	if got := cap.count("scan_result"); got != 5 {
		t.Fatalf("scan_result 发送 = %d, want 5", got)
	}
	got := make([]int64, 0, 5)
	for _, m := range cap.all() {
		if m.typ != "scan_result" {
			continue
		}
		seq, ok := t6SeqOf(m.data)
		if !ok {
			t.Fatalf("scan_result 缺少 seq: %v", m.data)
		}
		got = append(got, seq)
	}
	for i, seq := range got {
		if seq != int64(i+1) {
			t.Fatalf("第 %d 行 seq = %d, want %d（序号须 1..5 连续）", i+1, seq, i+1)
		}
	}
	if n := t6LogLen(bg.resultLog.Load()); n != 5 {
		t.Fatalf("日志行数 = %d, want 5", n)
	}
	if ns := t6LogNextSeq(bg.resultLog.Load()); ns != 6 {
		t.Fatalf("nextSeq = %d, want 6", ns)
	}
}

// T6-02——任务运行中断线，随后重连（具体序号见计划 §5）：断线前 lastSeq=10；
// 断线期间生成 seq 11..15；重连后 replay_results {lastSeq: 10}，预期补发 11..15，
// replayThroughSeq=15，replay_complete 后 lastSeq=15。
func TestT6_02_ReconnectReplay(t *testing.T) {
	bg, _ := t6BG(t, "t6-02")
	taskID := bg.taskIDSnapshot()
	_, wsURL2 := r3Server(t)
	c1 := r3Dial(t, wsURL2)
	r1 := t6NewReader(t, c1)

	epoch := t6Follow(t, r1)
	// 断线前：产生 10 行，客户端逐条收到（lastSeq=10）。
	for i := 0; i < 10; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("1.1.1.%d", i+1), Port: 443})
	}
	if got := r1.collectRows("scan_result", 10, 10*time.Second); len(got) != 10 || got[9] != 10 {
		t.Fatalf("断线前收到 seq = %v, want 1..10", got)
	}
	// 断线：关闭 c1；后台任务继续运行。
	c1.Close()
	time.Sleep(200 * time.Millisecond)
	// 断线期间：产生 5 行（写失败不回滚，仍入日志）。
	for i := 0; i < 5; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("2.2.2.%d", i+1), Port: 443})
	}
	if n := t6LogLen(bg.resultLog.Load()); n != 15 {
		t.Fatalf("断线期间日志行数 = %d, want 15（写失败不回滚）", n)
	}
	// 重连：新连接跟随，请求重放 lastSeq=10。
	c2 := r3Dial(t, wsURL2)
	r2 := t6NewReader(t, c2)

	epoch2 := t6Follow(t, r2)
	if epoch2 != epoch {
		t.Fatalf("重连后 epoch = %q, want %q", epoch2, epoch)
	}
	t6SendReplay(c2, taskID, epoch, 10, "req-t6-02")
	if got := r2.collectRows("scan_result", 5, 10*time.Second); len(got) != 5 || got[0] != 11 || got[4] != 15 {
		t.Fatalf("补发 seq = %v, want 11..15", got)
	}
	complete := r2.waitFor("replay_complete", 10*time.Second)
	d := msgData(complete)
	if rt, _ := d["replayThroughSeq"].(float64); rt != 15 {
		t.Fatalf("replayThroughSeq = %v, want 15", d["replayThroughSeq"])
	}
	if ls, _ := d["lastSeq"].(float64); ls != 15 {
		t.Fatalf("replay_complete.lastSeq = %v, want 15", d["lastSeq"])
	}
	if fin, _ := d["final"].(bool); fin {
		t.Fatalf("replay_complete.final = true, want false（任务仍在运行）")
	}
}

// T6-03——断线期间产生多条结果，重连后完整补发（nsb 类型，多行、按序、各一次）。
func TestT6_03_MultiRowReplay(t *testing.T) {
	bg, _ := t6BG(t, "t6-03")
	taskID := bg.taskIDSnapshot()
	_, wsURL := r3Server(t)
	c1 := r3Dial(t, wsURL)
	r1 := t6NewReader(t, c1)

	epoch := t6Follow(t, r1)
	for i := 0; i < 3; i++ {
		bg.sendTaskResult(t6Ctx(bg), "nsb_scan_result", nsbScanMessage{IP: fmt.Sprintf("3.3.3.%d", i+1), Port: "443"})
	}
	if got := r1.collectRows("nsb_scan_result", 3, 10*time.Second); got[2] != 3 {
		t.Fatalf("断线前收到 seq = %v, want 1..3", got)
	}
	c1.Close()
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < 7; i++ {
		bg.sendTaskResult(t6Ctx(bg), "nsb_scan_result", nsbScanMessage{IP: fmt.Sprintf("4.4.4.%d", i+1), Port: "443"})
	}
	c2 := r3Dial(t, wsURL)
	r2 := t6NewReader(t, c2)

	epoch2 := t6Follow(t, r2)
	if epoch2 != epoch {
		t.Fatalf("重连后 epoch 变化：%q -> %q", epoch, epoch2)
	}
	t6SendReplay(c2, taskID, epoch, 3, "req-t6-03")
	got := r2.collectRows("nsb_scan_result", 7, 10*time.Second)
	for i, seq := range got {
		if seq != int64(4+i) {
			t.Fatalf("补发第 %d 行 seq = %d, want %d", i+1, seq, 4+i)
		}
	}
	complete := r2.waitFor("replay_complete", 10*time.Second)
	if rt, _ := msgData(complete)["replayThroughSeq"].(float64); rt != 10 {
		t.Fatalf("replayThroughSeq = %v, want 10", msgData(complete)["replayThroughSeq"])
	}
}

// clientReplayModel 是 index.html applyResultRow（设计 v7 §5.6.2）的 Go 侧
// 忠实模型，仅用于验证“客户端可见结果”。输入原始帧 seq 流（可含重放与
// 实时发送的重叠重复），输出客户端实际消费（显示）的 seq 序列。语义逐条
// 对应 applyResultRow（index.html:3539 起）与 drainReplayPending（:3497 起）：
//   - seq <= replayLastSeq：丢弃（已消费行的重放到达；去重的第一道关）；
//   - 已在 replayPending：丢弃（缺口缓冲中的重复；第二道关）；
//   - seq > replayLastSeq+1：入缺口缓冲；
//   - 否则消费该行，并按升序排空 pending 中已连续的行。
//
// 刻意不模拟的两处，均不影响本轮两处调用：
//   - 64 项上限 clear + 重新请求：pending 最多 2 项（21、22 先于 11 到达），
//     远小于上限，不可能触发；
//   - drain 中“pending 项 <= replayLastSeq 时删除”分支：消费严格连续，
//     lastSeq 不可能越过 pending 项，该分支不可达。
//
// typeReconciled 分支不影响本模型：它只覆盖 test/nsb 类型（index.html:3456），
// scan_result 行消费时恒显示，故 consumed 即显示序列。
// test_s6_replay.js 在本工作树中不存在（仅 T6-04 注释引用），Go 侧模型是
// 目前唯一对客户端去重的验证。
type clientReplayModel struct {
	lastSeq  int64
	pending  map[int64]bool
	consumed []int64
}

func (m *clientReplayModel) apply(seq int64) {
	if seq <= m.lastSeq {
		return
	}
	if m.pending[seq] {
		return
	}
	if seq > m.lastSeq+1 {
		m.pending[seq] = true
		return
	}
	m.consume(seq)
	for m.pending[m.lastSeq+1] {
		m.consume(m.lastSeq + 1)
	}
}

func (m *clientReplayModel) consume(seq int64) {
	delete(m.pending, seq)
	m.lastSeq = seq
	m.consumed = append(m.consumed, seq)
}

// collectClientVisibleRows 收集原始行，直到客户端模型消费完
// wantLow..wantHigh 的全部序号（各一次），或超时失败。返回原始帧 seq 序列
// （含重叠重复）与模型消费序列。原始帧数不固定（重放边界 20..23 时为
// 13..16 帧），故不按固定条数收集；跳过其他类型消息并暂存（与 collectRows
// 一致，pending 机制共享）。注意：本函数在模型消费完 wantHigh 时即返回，
// 此后才到达的尾部重复帧不在返回的原始序列中——T6-04 的断言不依赖
// 观测到每一个尾部重复（只要求重复帧属于实时窗口、可见结果完整），故适用；
// 需精确原始帧流的测试（如变体）应固定条数收集后离线喂给模型。
func (r *t6Reader) collectClientVisibleRows(typ string, startLastSeq, wantLow, wantHigh int64, timeout time.Duration) (raw []int64, consumed []int64) {
	r.t.Helper()
	m := &clientReplayModel{lastSeq: startLastSeq, pending: make(map[int64]bool)}
	deadline := time.Now().Add(timeout)
	for m.lastSeq != wantHigh {
		rem := time.Until(deadline)
		if rem <= 0 {
			r.t.Fatalf("timeout waiting for client-visible rows %d..%d (raw %v, consumed %v)", wantLow, wantHigh, raw, m.consumed)
		}
		if msg, ok := r.takePending(typ); ok {
			seq, ok := t6SeqOf(msgData(msg))
			if !ok {
				r.t.Fatalf("%s row 缺少 seq: %v", typ, msg)
			}
			raw = append(raw, seq)
			m.apply(seq)
			continue
		}
		msg, ok := r.next(rem)
		if !ok {
			r.t.Fatalf("timeout waiting for client-visible rows %d..%d (raw %v, consumed %v)", wantLow, wantHigh, raw, m.consumed)
		}
		if mt, _ := msg["type"].(string); mt != typ {
			r.pending = append(r.pending, msg)
			continue
		}
		seq, ok := t6SeqOf(msgData(msg))
		if !ok {
			r.t.Fatalf("%s row 缺少 seq: %v", typ, msg)
		}
		raw = append(raw, seq)
		m.apply(seq)
	}
	return raw, m.consumed
}

// T6-04——重放过程中又产生实时结果（Go 部分）：重放行 11..20 与实时行 21..23
// 交错到达。读法二（已裁决）：原始帧流允许重放与实时发送重叠，同一 seq 可
// 出现两次；客户端按 applyResultRow 去重后可见的结果必须完整、无重复、
// 无丢失、无乱序。此处验证客户端可见的最终状态（Go 侧 clientReplayModel），
// 不是原始帧唯一性；交错顺序本身不由 Go 测试控制（真实并发）。
// 注：注释原引用的 test_s6_replay.js 在本工作树中不存在，固定交错的
// 确定性输入由 TestT6_04_H01DeterministicReplay 提供（seq=21 两次）。
func TestT6_04_ReplayPlusLive(t *testing.T) {
	bg, _ := t6BG(t, "t6-04")
	taskID := bg.taskIDSnapshot()
	_, wsURL := r3Server(t)
	c1 := r3Dial(t, wsURL)
	r1 := t6NewReader(t, c1)

	epoch := t6Follow(t, r1)
	// 产生 20 行，客户端消费前 10 行（lastSeq=10）。
	for i := 0; i < 20; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("5.5.5.%d", i+1), Port: 443})
	}
	if got := r1.collectRows("scan_result", 10, 10*time.Second); got[9] != 10 {
		t.Fatalf("前 10 行 seq = %v, want 1..10", got)
	}
	// 断线后重连：请求重放 lastSeq=10；重放进行中并发产生实时行 21..23。
	c1.Close()
	time.Sleep(200 * time.Millisecond)
	c2 := r3Dial(t, wsURL)
	r2 := t6NewReader(t, c2)

	epoch2 := t6Follow(t, r2)
	if epoch2 != epoch {
		t.Fatalf("重连后 epoch 变化")
	}
	// 先发重放请求，再并发产生实时行（重放与实时发送交错）。
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 3; i++ {
			bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("6.6.6.%d", i+1), Port: 443})
		}
	}()
	t6SendReplay(c2, taskID, epoch, 10, "req-t6-04")
	<-done
	// 收集全部行：重放行 11..20（10 行）+ 实时行 21..23（3 行），另加
	// 可能的重叠重复（重放边界 20..23 时原始帧为 13..16 帧）。原始帧的
	// 重复只允许是实时行 21..23（重放集合与实时发送的交集）；11..20 在
	// c2 上只可能来自重放（单次发送），重复即为真缺陷。
	raw, consumed := r2.collectClientVisibleRows("scan_result", 10, 11, 23, 15*time.Second)
	counts := make(map[int64]int)
	for _, seq := range raw {
		counts[seq]++
	}
	for seq, n := range counts {
		if n > 2 {
			t.Fatalf("原始帧 seq=%d 出现 %d 次，want <= 2（收到 %v）", seq, n, raw)
		}
		if n == 2 && (seq < 21 || seq > 23) {
			t.Fatalf("原始帧 seq=%d 重复，但它不在实时窗口 21..23 内（收到 %v）", seq, raw)
		}
	}
	// 客户端可见序列必须恰好是 11..23 各一次、按序消费。
	if len(consumed) != 13 {
		t.Fatalf("客户端可见行数 = %d, want 13（消费 %v，原始 %v）", len(consumed), consumed, raw)
	}
	for i, seq := range consumed {
		if seq != int64(11+i) {
			t.Fatalf("客户端可见第 %d 行 seq = %d, want %d（消费 %v，原始 %v）", i+1, seq, 11+i, consumed, raw)
		}
	}
	complete := r2.waitFor("replay_complete", 10*time.Second)
	// 重放边界在服务端处理请求时捕获：实时行 21..23 与重放请求并发产生，
	// 若它们先于请求被处理，边界即包含它们（20..23 均合法）；若后于，
	// 边界为 20，实时行另行到达。边界本身是时序相关的，不硬编码为 20。
	rt, _ := msgData(complete)["replayThroughSeq"].(float64)
	if rt < 20 || rt > 23 {
		t.Fatalf("replayThroughSeq = %v, want 20..23", rt)
	}
	if ls, _ := msgData(complete)["lastSeq"].(float64); ls != rt {
		t.Fatalf("replay_complete.lastSeq = %v, want replayThroughSeq %v", ls, rt)
	}
	if ts, _ := msgData(complete)["totalSeq"].(float64); ts < rt {
		t.Fatalf("totalSeq = %v, want >= replayThroughSeq %v", ts, rt)
	}
}

// TestT6_04_H01DeterministicReplay——H-01 确定性重叠场景（测试专用注入；
// H-01 调查轮加入，读法二裁决后保留为协议重叠场景的确定性输入）：人为制造
// H-A 交错——实时行 21 已追加到日志、尚未写出 socket 时，重放快照复制发生；
// 随后实时 21 写出、重放再写出 21，同一 seq 在同一连接上交付两次（历史
// seq=21 签名）。22/23 在重放完成后再产生（与原 T6-04 的并发产生不同），
// 以便将复制边界确定为 21（原测试的并发 22/23 使边界在 20..23 间浮动）。
// 本测试断言两层：原始帧的机制事实（首帧实时 21、11..20 各一次、21 恰好
// 两次、边界 21），以及客户端可见结果（模型消费 11..21 各一次、无重复、
// 无丢失）。只改测试文件：不改生产代码，不改现有测试与辅助函数。
func TestT6_04_H01DeterministicReplay(t *testing.T) {
	bg, _ := t6BG(t, "t6-04-h01")
	taskID := bg.taskIDSnapshot()
	_, wsURL := r3Server(t)
	c1 := r3Dial(t, wsURL)
	r1 := t6NewReader(t, c1)

	epoch := t6Follow(t, r1)
	// 产生 20 行，客户端消费前 10 行（lastSeq=10）。
	for i := 0; i < 20; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("5.5.5.%d", i+1), Port: 443})
	}
	if got := r1.collectRows("scan_result", 10, 10*time.Second); got[9] != 10 {
		t.Fatalf("前 10 行 seq = %v, want 1..10", got)
	}
	// 断线后重连：请求重放 lastSeq=10。
	c1.Close()
	time.Sleep(200 * time.Millisecond)
	c2 := r3Dial(t, wsURL)
	r2 := t6NewReader(t, c2)

	epoch2 := t6Follow(t, r2)
	if epoch2 != epoch {
		t.Fatalf("重连后 epoch 变化")
	}
	// 武装写延迟门：下一个 sendTaskResult 在分配 seq=21、追加日志之后、
	// 写 socket 之前阻塞（持有 resultSendMu）。
	g := &writeDelayGate{entered: make(chan struct{}), release: make(chan struct{})}
	testWriteDelayGate = g
	released := false
	defer func() {
		if !released {
			close(g.release)
		}
		testWriteDelayGate = nil
	}()
	// 只产生实时行 21，让它停在门上（已追加、未写出）。
	done21 := make(chan struct{})
	go func() {
		defer close(done21)
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "6.6.6.1", Port: 443})
	}()
	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("实时行 21 未进入写延迟门")
	}
	// 此时 seq=21 已在日志中、尚未写出。发重放请求 lastSeq=10；
	// 快照复制（只持 log.mu）不受门阻塞，复制集合包含 21，边界为 21。
	// 重放写出行 11 时被 resultSendMu（由停在门上的 21 持有）阻塞。
	t6SendReplay(c2, taskID, epoch, 10, "req-t6-04-h01")
	// 释放门：实时 21 写出（c2 上的第 1 帧），随后重放写出 11..21。
	close(g.release)
	released = true
	<-done21
	// 收集 12 帧：实时 21 + 重放 11..21。断言机制事实：首帧必为实时 21；
	// 11..20 各一次；21 恰好两次；再以客户端模型验证可见结果 11..21
	// 各一次、无重复、无丢失。
	got := r2.collectRows("scan_result", 12, 15*time.Second)
	count := make(map[int64]int)
	for _, seq := range got {
		count[seq]++
	}
	if got[0] != 21 {
		t.Fatalf("首帧 seq = %d, want 21（实时行应先于重放行到达）", got[0])
	}
	for seq := int64(11); seq <= 20; seq++ {
		if count[seq] != 1 {
			t.Fatalf("seq=%d 出现 %d 次，want 1（收到 %v）", seq, count[seq], got)
		}
	}
	if count[21] != 2 {
		t.Fatalf("seq=21 出现 %d 次，want 2（收到 %v）", count[21], got)
	}
	m := &clientReplayModel{lastSeq: 10, pending: make(map[int64]bool)}
	for _, seq := range got {
		m.apply(seq)
	}
	if len(m.consumed) != 11 {
		t.Fatalf("客户端可见行数 = %d, want 11（消费 %v，原始 %v）", len(m.consumed), m.consumed, got)
	}
	for i, seq := range m.consumed {
		if seq != int64(11+i) {
			t.Fatalf("客户端可见第 %d 行 seq = %d, want %d（消费 %v，原始 %v）", i+1, seq, 11+i, m.consumed, got)
		}
	}
	complete := r2.waitFor("replay_complete", 10*time.Second)
	if rt, _ := msgData(complete)["replayThroughSeq"].(float64); rt != 21 {
		t.Fatalf("replayThroughSeq = %v, want 21", rt)
	}
	if ls, _ := msgData(complete)["lastSeq"].(float64); ls != 21 {
		t.Fatalf("replay_complete.lastSeq = %v, want 21", ls)
	}
	// 复制边界确定后，再产生实时行 22、23（重放已完成，不再影响边界）。
	for i := 1; i < 3; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("6.6.6.%d", i+1), Port: 443})
	}
	if got := r2.collectRows("scan_result", 2, 10*time.Second); got[0] != 22 || got[1] != 23 {
		t.Fatalf("后续实时行 seq = %v, want [22 23]", got)
	}
}

// T6-05——重放过程中再次断线并重连：重放中途断线，重连后用更新的 lastSeq
// 续传，仅补发仍缺失的行，最终完整，无重复、无 lastSeq 回退。
func TestT6_05_ReplayInterrupted(t *testing.T) {
	bg, _ := t6BG(t, "t6-05")
	taskID := bg.taskIDSnapshot()
	_, wsURL := r3Server(t)
	c1 := r3Dial(t, wsURL)
	r1 := t6NewReader(t, c1)

	epoch := t6Follow(t, r1)
	for i := 0; i < 12; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("7.7.7.%d", i+1), Port: 443})
	}
	if got := r1.collectRows("scan_result", 4, 10*time.Second); got[3] != 4 {
		t.Fatalf("前 4 行 seq = %v, want 1..4", got)
	}
	c1.Close()
	time.Sleep(200 * time.Millisecond)
	// 重连 1：请求重放 lastSeq=4，只收前 3 行（5,6,7）就再次断线。
	c2 := r3Dial(t, wsURL)
	r2 := t6NewReader(t, c2)

	epoch2 := t6Follow(t, r2)
	if epoch2 != epoch {
		t.Fatalf("重连后 epoch 变化")
	}
	t6SendReplay(c2, taskID, epoch, 4, "req-t6-05-a")
	if got := r2.collectRows("scan_result", 3, 10*time.Second); got[0] != 5 || got[2] != 7 {
		t.Fatalf("第一次重放收到 seq = %v, want 5,6,7", got)
	}
	c2.Close()
	time.Sleep(200 * time.Millisecond)
	// 重连 2：用更新的 lastSeq=7 续传，应只补发 8..12，不重复 5..7。
	c3 := r3Dial(t, wsURL)
	r3 := t6NewReader(t, c3)

	epoch3 := t6Follow(t, r3)
	if epoch3 != epoch {
		t.Fatalf("第二次重连后 epoch 变化")
	}
	t6SendReplay(c3, taskID, epoch, 7, "req-t6-05-b")
	got := r3.collectRows("scan_result", 5, 10*time.Second)
	for i, seq := range got {
		if seq != int64(8+i) {
			t.Fatalf("续传第 %d 行 seq = %d, want %d（不得重复 5..7）", i+1, seq, 8+i)
		}
	}
	complete := r3.waitFor("replay_complete", 10*time.Second)
	if rt, _ := msgData(complete)["replayThroughSeq"].(float64); rt != 12 {
		t.Fatalf("replayThroughSeq = %v, want 12", msgData(complete)["replayThroughSeq"])
	}
}

// T6-06——客户端重复发送恢复请求（选 B，v7 §5.4.5）：服务端均重新计算并
// 重新发送相同结果（至少一次），不缓存、不拒绝；前端按 seq 去重。
// (a) 同 requestId 同 lastSeq；(b) 同 requestId 更新 lastSeq；
// (c) 不同 requestId 同 lastSeq。
func TestT6_06_DuplicateRequest(t *testing.T) {
	bg, _ := t6BG(t, "t6-06")
	taskID := bg.taskIDSnapshot()
	_, wsURL := r3Server(t)
	c1 := r3Dial(t, wsURL)
	r1 := t6NewReader(t, c1)

	epoch := t6Follow(t, r1)
	for i := 0; i < 8; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("8.8.8.%d", i+1), Port: 443})
	}
	if got := r1.collectRows("scan_result", 8, 10*time.Second); got[7] != 8 {
		t.Fatalf("收到 seq = %v, want 1..8", got)
	}
	// (a) 同 requestId 同 lastSeq=5 发两次：两次均返回行 6..8。
	t6SendReplay(c1, taskID, epoch, 5, "req-t6-06")
	gotA1 := r1.collectRows("scan_result", 3, 10*time.Second)
	if gotA1[0] != 6 || gotA1[2] != 8 {
		t.Fatalf("(a) 第一次收到 seq = %v, want 6..8", gotA1)
	}
	r1.waitFor("replay_complete", 10*time.Second)
	t6SendReplay(c1, taskID, epoch, 5, "req-t6-06")
	gotA2 := r1.collectRows("scan_result", 3, 10*time.Second)
	if gotA2[0] != 6 || gotA2[2] != 8 {
		t.Fatalf("(a) 第二次收到 seq = %v, want 6..8（至少一次重发）", gotA2)
	}
	r1.waitFor("replay_complete", 10*time.Second)
	// (b) 同 requestId 更新 lastSeq=7：服务端当作全新请求，按新 lastSeq 返回行 8。
	t6SendReplay(c1, taskID, epoch, 7, "req-t6-06")
	gotB := r1.collectRows("scan_result", 1, 10*time.Second)
	if gotB[0] != 8 {
		t.Fatalf("(b) 收到 seq = %v, want [8]", gotB)
	}
	r1.waitFor("replay_complete", 10*time.Second)
	// (c) 不同 requestId 同 lastSeq=5：与 (a) 相同，返回行 6..8。
	t6SendReplay(c1, taskID, epoch, 5, "req-t6-06-c")
	gotC := r1.collectRows("scan_result", 3, 10*time.Second)
	if gotC[0] != 6 || gotC[2] != 8 {
		t.Fatalf("(c) 收到 seq = %v, want 6..8", gotC)
	}
	r1.waitFor("replay_complete", 10*time.Second)
}

// T6-08——多个任务并发，各自断线和恢复：单任务守卫下第二任务被拒绝；
// 第一任务完成后再起第二任务，各自断线恢复（串行任务交替，见 v7 §9.2）。
func TestT6_08_SerialTasks(t *testing.T) {
	r3Setup(t)
	// 第一任务。
	s1 := &appSession{connectionID: "t6-08-a", backendGeneration: atomic.LoadUint64(&backendGeneration)}
	_, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	if _, ok := s1.beginTask(cancel1, "first", "official", nil); !ok {
		t.Fatal("第一任务 beginTask 失败")
	}
	// 第二任务在第一任务运行期间必须被拒绝。
	s2 := &appSession{connectionID: "t6-08-b", backendGeneration: atomic.LoadUint64(&backendGeneration)}
	_, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	if _, ok := s2.beginTask(cancel2, "second", "official", nil); ok {
		t.Fatal("第二任务在第一任务运行期间被接受，单任务守卫失效")
	}
	// 第一任务产生 3 行后结束；第二任务随后启动并各自完整重放。
	cap1 := &t6EmitCapture{}
	s1.emit = cap1.emit
	for i := 0; i < 3; i++ {
		s1.sendTaskResult(t6Ctx(s1), "scan_result", ScanResult{IP: fmt.Sprintf("9.9.9.%d", i+1), Port: 443})
	}
	s1.endTask()
	if n := t6LogLen(s1.resultLog.Load()); n != 3 {
		t.Fatalf("第一任务日志行数 = %d, want 3", n)
	}
	if _, ok := s2.beginTask(cancel2, "second", "official", nil); !ok {
		t.Fatal("第一任务结束后第二任务仍被拒绝")
	}
	defer s2.endTask()
	cap2 := &t6EmitCapture{}
	s2.emit = cap2.emit
	for i := 0; i < 2; i++ {
		s2.sendTaskResult(t6Ctx(s2), "scan_result", ScanResult{IP: fmt.Sprintf("10.10.10.%d", i+1), Port: 443})
	}
	if n := t6LogLen(s2.resultLog.Load()); n != 2 {
		t.Fatalf("第二任务日志行数 = %d, want 2（不得混入第一任务的行）", n)
	}
	if ns := t6LogNextSeq(s2.resultLog.Load()); ns != 3 {
		t.Fatalf("第二任务 nextSeq = %d, want 3（新任务序号从 1 开始）", ns)
	}
}

// T6-09——任务完成后客户端重连：从保留日志补发；final=true 的 replay_complete。
func TestT6_09_FinishedTaskReplay(t *testing.T) {
	bg, cancel := t6BG(t, "t6-09")
	taskID := bg.taskIDSnapshot()
	_, wsURL := r3Server(t)
	c1 := r3Dial(t, wsURL)
	r1 := t6NewReader(t, c1)

	epoch := t6Follow(t, r1)
	for i := 0; i < 4; i++ {
		bg.sendTaskResult(t6Ctx(bg), "test_result", TestResult{IP: fmt.Sprintf("11.11.11.%d", i+1), Port: 443})
	}
	if got := r1.collectRows("test_result", 4, 10*time.Second); got[3] != 4 {
		t.Fatalf("收到 seq = %v, want 1..4", got)
	}
	// 任务完成（endTask 冻结日志），客户端断线重连。
	cancel()
	bg.endTask()
	c1.Close()
	time.Sleep(200 * time.Millisecond)
	c2 := r3Dial(t, wsURL)
	r2 := t6NewReader(t, c2)

	epoch2 := t6Follow(t, r2)
	if epoch2 != epoch {
		t.Fatalf("重连后 epoch 变化")
	}
	t6SendReplay(c2, taskID, epoch, 2, "req-t6-09")
	got := r2.collectRows("test_result", 2, 10*time.Second)
	if got[0] != 3 || got[1] != 4 {
		t.Fatalf("补发 seq = %v, want 3,4（已完成日志保留至下一任务开始）", got)
	}
	complete := r2.waitFor("replay_complete", 10*time.Second)
	d := msgData(complete)
	if fin, _ := d["final"].(bool); !fin {
		t.Fatalf("replay_complete.final = false, want true（任务已冻结）")
	}
	if ts, _ := d["totalSeq"].(float64); ts != 4 {
		t.Fatalf("totalSeq = %v, want 4（冻结时 totalSeq == finalSeq）", ts)
	}
	if rt, _ := d["replayThroughSeq"].(float64); rt != 4 {
		t.Fatalf("replayThroughSeq = %v, want 4", rt)
	}
}

// T6-10——任务取消与断线同时发生：取消 ctx 与断线同时触发；任务终态正确；
// 冻结后发送被守卫拦截；无 panic、无死锁。
func TestT6_10_CancelAndDisconnect(t *testing.T) {
	bg, cancel := t6BG(t, "t6-10")
	cap := &t6EmitCapture{}
	bg.emit = cap.emit
	for i := 0; i < 3; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("12.12.12.%d", i+1), Port: 443})
	}
	// 取消与“断线”（emit 置空模拟连接丢失）同时触发，随后任务结束。
	cancel()
	bg.emit = nil
	bg.endTask()
	// 冻结后发送必须被守卫拦截：不分配序号、不记日志、不发送。
	bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "12.12.12.99", Port: 443})
	if n := t6LogLen(bg.resultLog.Load()); n != 3 {
		t.Fatalf("冻结后日志行数 = %d, want 3（守卫须拦截迟到发送）", n)
	}
	if ns := t6LogNextSeq(bg.resultLog.Load()); ns != 4 {
		t.Fatalf("冻结后 nextSeq = %d, want 4（不得分配新序号）", ns)
	}
	// 终态：任务已结束，日志已冻结，finalSeq == 3。
	bg.resultLog.Load().mu.Lock()
	frozen, finalSeq := bg.resultLog.Load().frozen, bg.resultLog.Load().finalSeq
	bg.resultLog.Load().mu.Unlock()
	if !frozen || finalSeq != 3 {
		t.Fatalf("冻结状态 frozen=%v finalSeq=%d, want true/3", frozen, finalSeq)
	}
}

// T6-11——缓存达到容量上限（P7）：直接驱动日志至 20,000 行 / 8 MiB（任一达到）。
// (a) 行数上限：20,000 行后 truncated，第 20,001 行仍发送、不入日志；
// (b) 字节上限：大行使 bytes 达 8 MiB 后 truncated。
// 上限是已批准初始参数，未实测最优（见计划 §8）。
func TestT6_11_LogCapacity(t *testing.T) {
	t.Run("row cap", func(t *testing.T) {
		bg, _ := t6BG(t, "t6-11a")
		cap := &t6EmitCapture{}
		bg.emit = cap.emit
		for i := 0; i < maxResultLogRows; i++ {
			bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "13.13.13.1", Port: 443})
		}
		// 上限检查在追加之前（v7 §5.1）：20,000 行整时日志恰好满格，尚未
		// 超限，truncated=false，可正常重放；第 20,001 行到达时才触发截断。
		bg.resultLog.Load().mu.Lock()
		truncated, n, next := bg.resultLog.Load().truncated, len(bg.resultLog.Load().rows), bg.resultLog.Load().nextSeq
		bg.resultLog.Load().mu.Unlock()
		if truncated {
			t.Fatalf("20,000 行整时 truncated=true, want false（尚未超限）")
		}
		if n != maxResultLogRows {
			t.Fatalf("日志行数 = %d, want %d", n, maxResultLogRows)
		}
		if next != maxResultLogRows+1 {
			t.Fatalf("nextSeq = %d, want %d", next, maxResultLogRows+1)
		}
		// 第 20,001 行：触发截断，仍发送（emit 收到），不入日志，序号继续递增。
		before := cap.count("scan_result")
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "13.13.13.2", Port: 443})
		if got := cap.count("scan_result"); got != before+1 {
			t.Fatalf("超限行发送数 = %d, want %d（超限行仍发送）", got, before+1)
		}
		bg.resultLog.Load().mu.Lock()
		tr := bg.resultLog.Load().truncated
		bg.resultLog.Load().mu.Unlock()
		if !tr {
			t.Fatalf("第 20,001 行后 truncated=false, want true")
		}
		if n := t6LogLen(bg.resultLog.Load()); n != maxResultLogRows {
			t.Fatalf("超限后日志行数 = %d, want %d（不再追加）", n, maxResultLogRows)
		}
		if ns := t6LogNextSeq(bg.resultLog.Load()); ns != maxResultLogRows+2 {
			t.Fatalf("超限后 nextSeq = %d, want %d（序号不回退）", ns, maxResultLogRows+2)
		}
	})
	t.Run("byte cap", func(t *testing.T) {
		bg, _ := t6BG(t, "t6-11b")
		cap := &t6EmitCapture{}
		bg.emit = cap.emit
		// 每行约 10 KiB（IP 字段填长字符串），约 820 行可达 8 MiB。
		bigIP := strings.Repeat("14.14.14.14-", 700)
		rows := 0
		for {
			bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: bigIP, Port: 443})
			rows++
			bg.resultLog.Load().mu.Lock()
			tr := bg.resultLog.Load().truncated
			bg.resultLog.Load().mu.Unlock()
			if tr {
				break
			}
			if rows > 5000 {
				t.Fatal("5000 行仍未触发字节上限")
			}
		}
		bg.resultLog.Load().mu.Lock()
		n, byts := len(bg.resultLog.Load().rows), bg.resultLog.Load().bytes
		bg.resultLog.Load().mu.Unlock()
		if n >= maxResultLogRows {
			t.Fatalf("字节上限测试误触行数上限：行数=%d", n)
		}
		// 五项修复问题 5（严格上限）：截断时 bytes 永不超过 8 MiB；截断的
		// 原因是下一行放不下（bytes + 一行 > 上限），而非 bytes 已达上限。
		if byts > maxResultLogBytes {
			t.Fatalf("截断时 bytes=%d, want <= %d（严格上限被突破）", byts, maxResultLogBytes)
		}
		t.Logf("字节上限在 %d 行触发，bytes=%d（行均约 %d 字节，距上限 %d 字节）", n, byts, byts/int64(n), maxResultLogBytes-byts)
		// 超限行仍发送。
		before := cap.count("scan_result")
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "14.14.14.15", Port: 443})
		if got := cap.count("scan_result"); got != before+1 {
			t.Fatalf("超限行发送数 = %d, want %d", got, before+1)
		}
	})
}

// T6-12——缓存过期或任务清理后客户端请求恢复：
// (a) 日志过期（冻结超 5 分钟）→ replay_rejected {log-expired}；
// (b) 日志被清理（下一任务开始）→ replay_rejected {task-not-found}。
func TestT6_12_LogExpired(t *testing.T) {
	t.Run("expired", func(t *testing.T) {
		bg, cancel := t6BG(t, "t6-12a")
		taskID := bg.taskIDSnapshot()
		_, wsURL := r3Server(t)
		c1 := r3Dial(t, wsURL)
		r1 := t6NewReader(t, c1)
		epoch := t6Follow(t, r1)
		for i := 0; i < 3; i++ {
			bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("15.15.15.%d", i+1), Port: 443})
		}
		if got := r1.collectRows("scan_result", 3, 10*time.Second); got[2] != 3 {
			t.Fatalf("收到 seq = %v, want 1..3", got)
		}
		cancel()
		bg.endTask()
		// 人为把冻结时刻拨到 6 分钟前（模拟 5 分钟重放可用期已过）。
		bg.resultLog.Load().mu.Lock()
		bg.resultLog.Load().frozenAt = time.Now().Add(-6 * time.Minute)
		bg.resultLog.Load().mu.Unlock()
		t6SendReplay(c1, taskID, epoch, 1, "req-t6-12a")
		typ, msg := r1.waitForAny(10*time.Second, "replay_rejected", "replay_complete")
		if typ != "replay_rejected" {
			t.Fatalf("过期日志收到 %s, want replay_rejected: %v", typ, msg)
		}
		if reason, _ := msgData(msg)["reason"].(string); reason != "log-expired" {
			t.Fatalf("拒绝原因 = %q, want log-expired", reason)
		}
	})
	t.Run("cleaned", func(t *testing.T) {
		bg, cancel := t6BG(t, "t6-12b")
		taskID := bg.taskIDSnapshot()
		_, wsURL := r3Server(t)
		c1 := r3Dial(t, wsURL)
		r1 := t6NewReader(t, c1)
		epoch := t6Follow(t, r1)
		for i := 0; i < 2; i++ {
			bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("16.16.16.%d", i+1), Port: 443})
		}
		if got := r1.collectRows("scan_result", 2, 10*time.Second); got[1] != 2 {
			t.Fatalf("收到 seq = %v, want 1,2", got)
		}
		// 结束旧任务并启动新任务（beginTask 清理旧日志）。
		cancel()
		bg.endTask()
		s2 := &appSession{connectionID: "t6-12b-2", backendGeneration: atomic.LoadUint64(&backendGeneration)}
		_, cancel2 := context.WithCancel(context.Background())
		defer cancel2()
		if _, ok := s2.beginTask(cancel2, "next task", "official", nil); !ok {
			t.Fatal("新任务 beginTask 失败")
		}
		defer s2.endTask()
		// 旧任务的重放请求应回 task-not-found（新任务已接管指针）。
		t6SendReplay(c1, taskID, epoch, 1, "req-t6-12b")
		typ, msg := r1.waitForAny(10*time.Second, "replay_rejected", "replay_complete")
		if typ != "replay_rejected" {
			t.Fatalf("已清理日志收到 %s, want replay_rejected: %v", typ, msg)
		}
		if reason, _ := msgData(msg)["reason"].(string); reason != "task-not-found" {
			t.Fatalf("拒绝原因 = %q, want task-not-found", reason)
		}
	})
}

// T6-13——旧连接迟到消息与新连接同时存在：旧连接的重放请求在重绑定后
// 应回 not-owner，不污染新连接状态。
func TestT6_13_StaleConnection(t *testing.T) {
	bg, _ := t6BG(t, "t6-13")
	taskID := bg.taskIDSnapshot()
	_, wsURL := r3Server(t)
	c1 := r3Dial(t, wsURL)
	r1 := t6NewReader(t, c1)

	epoch := t6Follow(t, r1)
	for i := 0; i < 5; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("17.17.17.%d", i+1), Port: 443})
	}
	if got := r1.collectRows("scan_result", 5, 10*time.Second); got[4] != 5 {
		t.Fatalf("c1 收到 seq = %v, want 1..5", got)
	}
	// 新连接跟随（重绑定到 c2），旧连接 c1 仍保持打开。
	c2 := r3Dial(t, wsURL)
	r2 := t6NewReader(t, c2)

	epoch2 := t6Follow(t, r2)
	if epoch2 != epoch {
		t.Fatalf("重绑定后 epoch 变化")
	}
	// 旧连接 c1 发重放请求：应回 not-owner。
	t6SendReplay(c1, taskID, epoch, 3, "req-t6-13-old")
	typ, msg := r1.waitForAny(10*time.Second, "replay_rejected", "replay_complete")
	if typ != "replay_rejected" {
		t.Fatalf("旧连接收到 %s, want replay_rejected: %v", typ, msg)
	}
	if reason, _ := msgData(msg)["reason"].(string); reason != "not-owner" {
		t.Fatalf("拒绝原因 = %q, want not-owner", reason)
	}
	// 新连接 c2 发重放请求：应正常返回行 4,5。
	t6SendReplay(c2, taskID, epoch, 3, "req-t6-13-new")
	got := r2.collectRows("scan_result", 2, 10*time.Second)
	if got[0] != 4 || got[1] != 5 {
		t.Fatalf("c2 补发 seq = %v, want 4,5", got)
	}
	r2.waitFor("replay_complete", 10*time.Second)
}

// T6-14——scan、test、nsb 三类结果分别验证：三类行均有 seq、均入同一日志、均可重放。
func TestT6_14_ThreeTypes(t *testing.T) {
	bg, _ := t6BG(t, "t6-14")
	cap := &t6EmitCapture{}
	bg.emit = cap.emit
	bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "18.18.18.1", Port: 443})
	bg.sendTaskResult(t6Ctx(bg), "test_result", TestResult{IP: "18.18.18.2", Port: 443})
	bg.sendTaskResult(t6Ctx(bg), "nsb_scan_result", nsbScanMessage{IP: "18.18.18.3", Port: "443"})
	bg.sendTaskResult(t6Ctx(bg), "speed_test_result", map[string]interface{}{"ip": "18.18.18.4", "speed": "10.00MB/s"})
	// 四类行共享同一序号空间：1,2,3,4。
	for _, typ := range []string{"scan_result", "test_result", "nsb_scan_result", "speed_test_result"} {
		if got := cap.count(typ); got != 1 {
			t.Fatalf("%s 发送 = %d, want 1", typ, got)
		}
	}
	seqs := map[string]int64{}
	for _, m := range cap.all() {
		seq, ok := t6SeqOf(m.data)
		if !ok {
			t.Fatalf("%s 缺少 seq: %v", m.typ, m.data)
		}
		seqs[m.typ] = seq
	}
	if seqs["scan_result"] != 1 || seqs["test_result"] != 2 || seqs["nsb_scan_result"] != 3 || seqs["speed_test_result"] != 4 {
		t.Fatalf("四类 seq = %v, want scan=1 test=2 nsb=3 speed=4", seqs)
	}
	if n := t6LogLen(bg.resultLog.Load()); n != 4 {
		t.Fatalf("日志行数 = %d, want 4", n)
	}
	// 重放：直接调 replayResults（emit 捕获），四类行均可重放。
	cap2 := &t6EmitCapture{}
	bg.emit = cap2.emit
	completed, reason := bg.replayResults(nil, bg.taskIDSnapshot(), currentBackendEpoch(), 0, "req-t6-14")
	if !completed || reason != "" {
		t.Fatalf("replayResults completed=%v reason=%q, want true/\"\"", completed, reason)
	}
	if got := cap2.count("replay_complete"); got != 1 {
		t.Fatalf("replay_complete 发送 = %d, want 1", got)
	}
	for _, typ := range []string{"scan_result", "test_result", "nsb_scan_result", "speed_test_result"} {
		if got := cap2.count(typ); got != 1 {
			t.Fatalf("重放 %s = %d, want 1", typ, got)
		}
	}
}

// T6-15——结果序号连续性、完整性和最终顺序验证（Go 部分）：产生连续行 →
// 断线 → 重放 → lastSeq 连续、无缺口、无重复、最终顺序与产生顺序一致。
// 前端部分见 test_s6_replay.js。
func TestT6_15_SeqIntegrity(t *testing.T) {
	bg, _ := t6BG(t, "t6-15")
	taskID := bg.taskIDSnapshot()
	_, wsURL := r3Server(t)
	c1 := r3Dial(t, wsURL)
	r1 := t6NewReader(t, c1)

	epoch := t6Follow(t, r1)
	for i := 0; i < 6; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("19.19.19.%d", i+1), Port: 443})
	}
	if got := r1.collectRows("scan_result", 6, 10*time.Second); got[5] != 6 {
		t.Fatalf("收到 seq = %v, want 1..6", got)
	}
	c1.Close()
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < 6; i++ {
		bg.sendTaskResult(t6Ctx(bg), "test_result", TestResult{IP: fmt.Sprintf("20.20.20.%d", i+1), Port: 443})
	}
	c2 := r3Dial(t, wsURL)
	r2 := t6NewReader(t, c2)

	epoch2 := t6Follow(t, r2)
	if epoch2 != epoch {
		t.Fatalf("重连后 epoch 变化")
	}
	t6SendReplay(c2, taskID, epoch, 6, "req-t6-15")
	got := r2.collectRows("test_result", 6, 10*time.Second)
	for i, seq := range got {
		if seq != int64(7+i) {
			t.Fatalf("补发第 %d 行 seq = %d, want %d", i+1, seq, 7+i)
		}
	}
	complete := r2.waitFor("replay_complete", 10*time.Second)
	if rt, _ := msgData(complete)["replayThroughSeq"].(float64); rt != 12 {
		t.Fatalf("replayThroughSeq = %v, want 12", rt)
	}
}

// T6-16——两个 worker 分配序号与实际发送顺序竞态：两 worker 并发调
// sendTaskResult，用钩子阻塞写操作，交错释放；序号顺序 = 日志顺序 =
// 实际发送顺序，无错位。
func TestT6_16_TwoWorkerRace(t *testing.T) {
	bg, _ := t6BG(t, "t6-16")
	cap := &t6EmitCapture{}
	bg.emit = cap.emit
	g := &writeDelayGate{entered: make(chan struct{}), release: make(chan struct{})}
	testWriteDelayGate = g
	defer func() { testWriteDelayGate = nil }()
	// worker A 先进入临界区：分配 seq=1，关闭 entered，阻塞于 release。
	// worker B 随后进入：阻塞于 resultSendMu（A 持有），尚未分配序号。
	doneA := make(chan struct{})
	doneB := make(chan struct{})
	go func() {
		defer close(doneA)
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "21.21.21.1", Port: 443})
	}()
	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("worker A 未进入写延迟门")
	}
	go func() {
		defer close(doneB)
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "21.21.21.2", Port: 443})
	}()
	// 给 B 走到 resultSendMu 阻塞点的时间（B 不得先于 A 分配序号）。
	time.Sleep(100 * time.Millisecond)
	// 释放 A：A 写出 seq=1，释放 resultSendMu；B 随后分配 seq=2 写出。
	close(g.release)
	select {
	case <-doneA:
	case <-time.After(5 * time.Second):
		t.Fatal("worker A 未在释放后返回")
	}
	select {
	case <-doneB:
	case <-time.After(5 * time.Second):
		t.Fatal("worker B 未返回")
	}
	// 发送顺序须为 seq=1 在前、seq=2 在后（emit 捕获顺序即发送顺序）。
	msgs := cap.all()
	if len(msgs) != 2 {
		t.Fatalf("emit 收到 %d 条，want 2", len(msgs))
	}
	s1, _ := t6SeqOf(msgs[0].data)
	s2, _ := t6SeqOf(msgs[1].data)
	if s1 != 1 || s2 != 2 {
		t.Fatalf("发送顺序 seq = [%d %d], want [1 2]（seq=2 不得先于 seq=1 写入）", s1, s2)
	}
	// 日志顺序同样为 1,2。
	bg.resultLog.Load().mu.Lock()
	l1, l2 := bg.resultLog.Load().rows[0].Seq, bg.resultLog.Load().rows[1].Seq
	bg.resultLog.Load().mu.Unlock()
	if l1 != 1 || l2 != 2 {
		t.Fatalf("日志顺序 seq = [%d %d], want [1 2]", l1, l2)
	}
}

// T6-17a——重放完成消息交错——后端（方案 B，不增加同步点；计划 §5）。
// 日志含行 1..10；客户端 lastSeq=3；任务仍在运行。测试从 socket 读取重放行，
// 收到 seq=6 后触发实时行 seq=11；随后继续读取重放行 7..10 与 replay_complete。
// 确定性验收（9 项）：边界 10；重放行恰为 4..10；实时行可在观察到 6 后触发；
// 不断言 11 插入 6 与 7 之间；不断言 11 与 7..10 的相对顺序；complete 只能在
// 边界行全部写完后发送；totalSeq >= replayThroughSeq（实测 10 或 11，不硬编码）；
// 交错仅作最终状态观察。
func TestT6_17a_BackendBoundary(t *testing.T) {
	bg, _ := t6BG(t, "t6-17a")
	taskID := bg.taskIDSnapshot()
	_, wsURL := r3Server(t)
	c1 := r3Dial(t, wsURL)
	r1 := t6NewReader(t, c1)

	epoch := t6Follow(t, r1)
	for i := 0; i < 10; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("22.22.22.%d", i+1), Port: 443})
	}
	if got := r1.collectRows("scan_result", 3, 10*time.Second); got[2] != 3 {
		t.Fatalf("前 3 行 seq = %v, want 1..3", got)
	}
	c1.Close()
	time.Sleep(200 * time.Millisecond)
	c2 := r3Dial(t, wsURL)
	r2 := t6NewReader(t, c2)

	epoch2 := t6Follow(t, r2)
	if epoch2 != epoch {
		t.Fatalf("重连后 epoch 变化")
	}
	t6SendReplay(c2, taskID, epoch, 3, "req-t6-17a")
	// 读重放行 4,5,6；收到 6 后（此时边界已捕获——边界捕获先于任何重放行
	// 发送），触发实时行 seq=11；继续读 7..10 与 replay_complete。
	got := r2.collectRows("scan_result", 3, 10*time.Second)
	if got[0] != 4 || got[1] != 5 || got[2] != 6 {
		t.Fatalf("重放前 3 行 seq = %v, want 4,5,6", got)
	}
	bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "22.22.22.11", Port: 443})
	// 继续收集：重放行 7..10（4 行）+ 实时行 11（1 行），共 5 行，顺序不限。
	rest := r2.collectRows("scan_result", 5, 10*time.Second)
	seen := map[int64]bool{}
	for _, seq := range rest {
		if seen[seq] {
			t.Fatalf("重复行 seq=%d（%v）", seq, rest)
		}
		seen[seq] = true
	}
	for _, seq := range []int64{7, 8, 9, 10, 11} {
		if !seen[seq] {
			t.Fatalf("缺失行 seq=%d（收到 %v）", seq, rest)
		}
	}
	complete := r2.waitFor("replay_complete", 10*time.Second)
	d := msgData(complete)
	if rt, _ := d["replayThroughSeq"].(float64); rt != 10 {
		t.Fatalf("replayThroughSeq = %v, want 10", rt)
	}
	if ls, _ := d["lastSeq"].(float64); ls != 10 {
		t.Fatalf("replay_complete.lastSeq = %v, want 10", ls)
	}
	// totalSeq >= replayThroughSeq，实测 10 或 11，不硬编码为 11。
	ts, _ := d["totalSeq"].(float64)
	if ts < 10 || ts > 11 {
		t.Fatalf("totalSeq = %v, want 10 或 11", ts)
	}
	t.Logf("T6-17a 实测 totalSeq=%v（方案 B：不硬编码为 11）", ts)
	if fin, _ := d["final"].(bool); fin {
		t.Fatalf("replay_complete.final = true, want false（任务仍在运行）")
	}
}

// T6-17a 写失败子场景：重放中写失败（连接已死）时不得发送 replay_complete。
// 确定性子场景：预置 wsClosed 后直接调 replayResults，断言返回 (false, "")
// 且 emit 未收到 replay_complete。真实 socket 中途关闭是时序相关的
// （TCP 缓冲），由本确定性子场景覆盖代码路径。
func TestT6_17a_WriteFailure(t *testing.T) {
	bg, _ := t6BG(t, "t6-17a-wf")
	cap := &t6EmitCapture{}
	bg.emit = cap.emit
	for i := 0; i < 5; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("23.23.23.%d", i+1), Port: 443})
	}
	// 预置连接已死。
	bg.wsMutex.Lock()
	bg.wsClosed = true
	bg.wsMutex.Unlock()
	completed, reason := bg.replayResults(nil, bg.taskIDSnapshot(), currentBackendEpoch(), 2, "req-t6-17a-wf")
	if completed || reason != "" {
		t.Fatalf("写失败时 replayResults = (%v, %q), want (false, \"\")", completed, reason)
	}
	if got := cap.count("replay_complete"); got != 0 {
		t.Fatalf("写失败时 replay_complete 发送 = %d, want 0", got)
	}
}

// T6-18——相同 requestId 重复请求：同 requestId 同 lastSeq 发两次，两次均
// 重新计算并发送相同行；前端去重（Go 侧验证两次返回相同行）。
func TestT6_18_SameRequestId(t *testing.T) {
	bg, _ := t6BG(t, "t6-18")
	taskID := bg.taskIDSnapshot()
	_, wsURL := r3Server(t)
	c1 := r3Dial(t, wsURL)
	r1 := t6NewReader(t, c1)

	epoch := t6Follow(t, r1)
	for i := 0; i < 6; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("24.24.24.%d", i+1), Port: 443})
	}
	if got := r1.collectRows("scan_result", 6, 10*time.Second); got[5] != 6 {
		t.Fatalf("收到 seq = %v, want 1..6", got)
	}
	t6SendReplay(c1, taskID, epoch, 4, "req-t6-18")
	got1 := r1.collectRows("scan_result", 2, 10*time.Second)
	if got1[0] != 5 || got1[1] != 6 {
		t.Fatalf("第一次收到 seq = %v, want 5,6", got1)
	}
	r1.waitFor("replay_complete", 10*time.Second)
	t6SendReplay(c1, taskID, epoch, 4, "req-t6-18")
	got2 := r1.collectRows("scan_result", 2, 10*time.Second)
	if got2[0] != 5 || got2[1] != 6 {
		t.Fatalf("第二次收到 seq = %v, want 5,6（至少一次重发，不得回缓存/拒绝）", got2)
	}
	r1.waitFor("replay_complete", 10*time.Second)
}

// T6-19——不同 requestId、更新 lastSeq 请求（选 B）：R1/lastSeq=3 收行 4、5
// 后断线 → R2/lastSeq=5；后端按 (taskId, lastSeq=5) 返回行 6..10，不返回
// 4..5；不因 requestId 不同拒绝。
func TestT6_19_NewRequestIdNewSeq(t *testing.T) {
	bg, _ := t6BG(t, "t6-19")
	taskID := bg.taskIDSnapshot()
	_, wsURL := r3Server(t)
	c1 := r3Dial(t, wsURL)
	r1 := t6NewReader(t, c1)

	epoch := t6Follow(t, r1)
	for i := 0; i < 10; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("25.25.25.%d", i+1), Port: 443})
	}
	if got := r1.collectRows("scan_result", 3, 10*time.Second); got[2] != 3 {
		t.Fatalf("前 3 行 seq = %v, want 1..3", got)
	}
	// R1：lastSeq=3，收行 4、5 后断线。
	t6SendReplay(c1, taskID, epoch, 3, "req-t6-19-r1")
	got := r1.collectRows("scan_result", 2, 10*time.Second)
	if got[0] != 4 || got[1] != 5 {
		t.Fatalf("R1 收到 seq = %v, want 4,5", got)
	}
	// 不等 replay_complete（继续读会收到它，但断线模拟优先）：直接关闭。
	c1.Close()
	time.Sleep(200 * time.Millisecond)
	// R2：lastSeq=5（已消费到 5），应只返回 6..10。
	c2 := r3Dial(t, wsURL)
	r2 := t6NewReader(t, c2)

	epoch2 := t6Follow(t, r2)
	if epoch2 != epoch {
		t.Fatalf("重连后 epoch 变化")
	}
	t6SendReplay(c2, taskID, epoch, 5, "req-t6-19-r2")
	got2 := r2.collectRows("scan_result", 5, 10*time.Second)
	for i, seq := range got2 {
		if seq != int64(6+i) {
			t.Fatalf("R2 第 %d 行 seq = %d, want %d（不得返回 4..5）", i+1, seq, 6+i)
		}
	}
	complete := r2.waitFor("replay_complete", 10*time.Second)
	if rt, _ := msgData(complete)["replayThroughSeq"].(float64); rt != 10 {
		t.Fatalf("replayThroughSeq = %v, want 10", rt)
	}
}

// T6-21——endTask 与最后一个 worker 结果发送并发（冻结屏障）：worker 分配
// seq=N 进入临界区，写操作被 testWriteDelayGate 阻塞 → 取消 → endTask →
// 释放写操作。冻结等待在途发送结束；seq=N 写完后冻结才完成；终态消息不先于
// 该行；finalSeq == N。
// 证明等级：有界观察（200ms 失败探测，方案 A）。200ms 内 endTask 未返回只
// 验证可观测行为，不证明 goroutine 必然阻塞于 Lock()（见 v7 §9.4.3；
// 屏障的存在由源码锁顺序分析证明）。
func TestT6_21_FreezeBarrierCancel(t *testing.T) {
	bg, cancel := t6BG(t, "t6-21")
	cap := &t6EmitCapture{}
	bg.emit = cap.emit
	for i := 0; i < 2; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("26.26.26.%d", i+1), Port: 443})
	}
	g := &writeDelayGate{entered: make(chan struct{}), release: make(chan struct{})}
	testWriteDelayGate = g
	defer func() { testWriteDelayGate = nil }()
	// worker 发送 seq=3，进入写延迟门后阻塞（持有 resultSendMu）。
	doneSend := make(chan struct{})
	go func() {
		defer close(doneSend)
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "26.26.26.3", Port: 443})
	}()
	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("worker 未进入写延迟门")
	}
	// 取消任务，随后 endTask（冻结屏障须等待在途发送）。
	cancel()
	enteredHook := make(chan struct{})
	var hookOnce sync.Once
	testEndTaskEnteredHook = func() { hookOnce.Do(func() { close(enteredHook) }) }
	defer func() { testEndTaskEnteredHook = nil }()
	doneEnd := make(chan struct{})
	go func() {
		defer close(doneEnd)
		bg.endTask()
	}()
	select {
	case <-enteredHook:
	case <-time.After(5 * time.Second):
		t.Fatal("endTask 未进入冻结路径")
	}
	// 有界否定检查：200ms 内 endTask 不得返回（在途写操作仍被门阻塞）。
	select {
	case <-doneEnd:
		t.Fatal("endTask 在写操作仍被阻塞期间返回，冻结屏障失效")
	case <-time.After(200 * time.Millisecond):
	}
	// 释放写操作：seq=3 写完，endTask 随后返回。
	close(g.release)
	select {
	case <-doneSend:
	case <-time.After(5 * time.Second):
		t.Fatal("worker 发送未在释放后返回")
	}
	select {
	case <-doneEnd:
	case <-time.After(5 * time.Second):
		t.Fatal("endTask 在写操作释放后仍未返回（死锁？）")
	}
	// seq=3 已发送且已入日志；finalSeq == 3；终态在行之后。
	if got := cap.count("scan_result"); got != 3 {
		t.Fatalf("scan_result 发送 = %d, want 3", got)
	}
	bg.resultLog.Load().mu.Lock()
	frozen, finalSeq, n := bg.resultLog.Load().frozen, bg.resultLog.Load().finalSeq, len(bg.resultLog.Load().rows)
	bg.resultLog.Load().mu.Unlock()
	if !frozen || finalSeq != 3 || n != 3 {
		t.Fatalf("冻结状态 frozen=%v finalSeq=%d 行数=%d, want true/3/3", frozen, finalSeq, n)
	}
}

// T6-22——任务取消与结果发送并发：取消任务的同时 worker 发送结果。
// 冻结后调用的 sendTaskResult 被守卫拦截（不分配序号、不记日志、不发送）；
// 日志不再增长；无 panic、无死锁。
func TestT6_22_CancelRace(t *testing.T) {
	bg, cancel := t6BG(t, "t6-22")
	cap := &t6EmitCapture{}
	bg.emit = cap.emit
	for i := 0; i < 3; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("27.27.27.%d", i+1), Port: 443})
	}
	// 并发突发：10 个 goroutine 在 endTask 执行期间同时发送。部分可能在
	// 冻结前被准入（得序号、入日志），其余在冻结后被守卫拦截。断言不变量：
	// 不 panic、不死锁；冻结后 finalSeq == 日志行数；nextSeq == finalSeq+1。
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("27.27.27.%d", 100+i), Port: 443})
		}(i)
	}
	cancel()
	bg.endTask()
	wg.Wait()
	bg.resultLog.Load().mu.Lock()
	frozen, finalSeq, n, next := bg.resultLog.Load().frozen, bg.resultLog.Load().finalSeq, len(bg.resultLog.Load().rows), bg.resultLog.Load().nextSeq
	bg.resultLog.Load().mu.Unlock()
	if !frozen {
		t.Fatal("并发取消后日志未冻结")
	}
	if finalSeq != int64(n) {
		t.Fatalf("finalSeq=%d, 日志行数=%d（冻结时 finalSeq 须等于已准入行数）", finalSeq, n)
	}
	if next != finalSeq+1 {
		t.Fatalf("nextSeq=%d, want finalSeq+1=%d", next, finalSeq+1)
	}
	if n < 3 {
		t.Fatalf("日志行数=%d, want >= 3（冻结前已产生的行不得丢失）", n)
	}
	// 冻结已完成后再次发送：必须被守卫拦截，日志不再增长。
	before := cap.count("scan_result")
	for i := 0; i < 3; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "27.27.27.99", Port: 443})
	}
	if n := t6LogLen(bg.resultLog.Load()); n != int(finalSeq) {
		t.Fatalf("冻结后日志行数 = %d, want %d（守卫须拦截）", n, finalSeq)
	}
	if got := cap.count("scan_result"); got != before {
		t.Fatalf("冻结后发送数 = %d, want %d（守卫须拦截，不发送）", got, before)
	}
}

// T6-23——任务日志冻结后迟到的发送请求：日志冻结后调 sendTaskResult，
// 被守卫拦截（不分配序号、不记日志、不发送、记调试日志 send_after_freeze）。
func TestT6_23_SendAfterFreeze(t *testing.T) {
	bg, cancel := t6BG(t, "t6-23")
	cap := &t6EmitCapture{}
	bg.emit = cap.emit
	for i := 0; i < 2; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("28.28.28.%d", i+1), Port: 443})
	}
	cancel()
	bg.endTask()
	// 冻结后三次迟到发送：均须被拦截。
	for i := 0; i < 3; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "28.28.28.99", Port: 443})
		bg.sendTaskResult(t6Ctx(bg), "test_result", TestResult{IP: "28.28.28.99", Port: 443})
	}
	if n := t6LogLen(bg.resultLog.Load()); n != 2 {
		t.Fatalf("冻结后日志行数 = %d, want 2", n)
	}
	if ns := t6LogNextSeq(bg.resultLog.Load()); ns != 3 {
		t.Fatalf("冻结后 nextSeq = %d, want 3（不得分配新序号）", ns)
	}
	if got := cap.count("scan_result"); got != 2 {
		t.Fatalf("冻结后 scan_result 发送 = %d, want 2", got)
	}
	if got := cap.count("test_result"); got != 0 {
		t.Fatalf("冻结后 test_result 发送 = %d, want 0", got)
	}
}

// T6-24——冻结屏障独立行为测试（含有界失败探测）：一 goroutine 调
// sendTaskResult（写操作被门阻塞）→ 另一 goroutine 调 endTask → 释放写操作。
// endTask 在门释放前不返回；释放后 endTask 返回；finalSeq == N。
// 证明等级与 T6-21 相同：有界观察（200ms），不证明锁内等待状态。
func TestT6_24_FreezeBarrierOnly(t *testing.T) {
	r3Setup(t)
	s := &appSession{connectionID: "t6-24", backendGeneration: atomic.LoadUint64(&backendGeneration)}
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, ok := s.beginTask(cancel, "t6-24 task", "official", nil); !ok {
		t.Fatal("beginTask failed")
	}
	defer func() {
		testEndTaskEnteredHook = nil
		testWriteDelayGate = nil
		s.endTask()
		clearCompletedBackgroundTaskSession()
	}()
	cap := &t6EmitCapture{}
	s.emit = cap.emit
	g := &writeDelayGate{entered: make(chan struct{}), release: make(chan struct{})}
	testWriteDelayGate = g
	defer func() {
		// 失败路径下写操作可能仍被门阻塞：先释放，避免 endTask 死锁。
		select {
		case <-g.release:
		default:
			close(g.release)
		}
	}()
	doneSend := make(chan struct{})
	go func() {
		defer close(doneSend)
		s.sendTaskResult(t6Ctx(s), "scan_result", ScanResult{IP: "29.29.29.1", Port: 443})
	}()
	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("发送未进入写延迟门")
	}
	enteredHook := make(chan struct{})
	var hookOnce sync.Once
	testEndTaskEnteredHook = func() { hookOnce.Do(func() { close(enteredHook) }) }
	doneEnd := make(chan struct{})
	go func() {
		defer close(doneEnd)
		s.endTask()
	}()
	select {
	case <-enteredHook:
	case <-time.After(5 * time.Second):
		t.Fatal("endTask 未进入冻结路径")
	}
	// 有界否定检查：200ms 内 endTask 不得返回。
	select {
	case <-doneEnd:
		t.Fatal("endTask 在写操作仍被阻塞期间返回，冻结屏障失效")
	case <-time.After(200 * time.Millisecond):
	}
	close(g.release)
	select {
	case <-doneSend:
	case <-time.After(5 * time.Second):
		t.Fatal("发送未在释放后返回")
	}
	select {
	case <-doneEnd:
	case <-time.After(5 * time.Second):
		t.Fatal("endTask 在写操作释放后仍未返回（死锁？）")
	}
	s.resultLog.Load().mu.Lock()
	frozen, finalSeq := s.resultLog.Load().frozen, s.resultLog.Load().finalSeq
	s.resultLog.Load().mu.Unlock()
	if !frozen || finalSeq != 1 {
		t.Fatalf("冻结状态 frozen=%v finalSeq=%d, want true/1", frozen, finalSeq)
	}
	if got := cap.count("scan_result"); got != 1 {
		t.Fatalf("scan_result 发送 = %d, want 1", got)
	}
}

// ==========================================================================
// 五项修复回归测试（2026-11-12 授权；与 S6 实施计划 v4.1 / 设计 v7 同基线）
// ==========================================================================

// --- 问题 1：speed_test_result 的 seq 必须是 JSON number ---
// 测速行以 map[string]interface{} 发送；日志 Data 与线上载荷的 seq 均为
// JSON number（不是字符串），四类结果共享同一序号空间与同一处理路径。
func TestT6_SpeedSeqJSONNumber(t *testing.T) {
	bg, _ := t6BG(t, "t6-speed-num")
	cap := &t6EmitCapture{}
	bg.emit = cap.emit
	bg.sendTaskResult(t6Ctx(bg), "speed_test_result", map[string]interface{}{"ip": "1.2.3.4", "speed": "10.00MB/s"})
	// 1. 线上载荷（emit 捕获的 Go 值）：seq 是 int64（JSON number），不是 string。
	msgs := cap.all()
	if len(msgs) != 1 {
		t.Fatalf("emit 消息 = %d, want 1", len(msgs))
	}
	m, ok := msgs[0].data.(map[string]interface{})
	if !ok {
		t.Fatalf("speed 载荷类型 = %T, want map[string]interface{}", msgs[0].data)
	}
	seqVal, ok := m["seq"]
	if !ok {
		t.Fatalf("speed 载荷缺少 seq: %v", m)
	}
	if _, isNum := seqVal.(int64); !isNum {
		t.Fatalf("speed seq Go 类型 = %T(%v), want int64（JSON number）", seqVal, seqVal)
	}
	if seqVal.(int64) != 1 {
		t.Fatalf("speed seq = %v, want 1", seqVal)
	}
	// 2. 日志 Data（实际 JSON）：seq 是 JSON number，不是字符串。
	log := bg.resultLog.Load()
	log.mu.Lock()
	if len(log.rows) != 1 {
		log.mu.Unlock()
		t.Fatalf("日志行数 = %d, want 1", len(log.rows))
	}
	raw := log.rows[0].Data
	log.mu.Unlock()
	var decoded map[string]interface{}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("日志 Data 解析失败：%v（%s）", err, string(raw))
	}
	num, ok := decoded["seq"].(float64)
	if !ok {
		t.Fatalf("日志 Data 的 seq 不是 JSON number：%s", string(raw))
	}
	if num != 1 {
		t.Fatalf("日志 Data seq = %v, want 1", num)
	}
	// 3. 重放该行：重放行同样带 number seq（经 JSON 往返后为 float64）。
	completed, reason := bg.replayResults(nil, bg.taskIDSnapshot(), currentBackendEpoch(), 0, "req-speed-num")
	if !completed || reason != "" {
		t.Fatalf("replayResults = (%v, %q), want (true, \"\")", completed, reason)
	}
	replayed := cap.all()
	var found bool
	for _, mm := range replayed {
		if mm.typ != "speed_test_result" {
			continue
		}
		// 重放行经 emit 直接透传 Data（json.RawMessage）；解析确认 number seq。
		if rm, ok := mm.data.(json.RawMessage); ok {
			var d map[string]interface{}
			if err := json.Unmarshal(rm, &d); err == nil {
				if n, ok := d["seq"].(float64); ok && n == 1 {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatalf("重放的 speed 行未带 JSON number seq=1：%v", replayed)
	}
}

// --- 问题 2：旧 worker 跨任务污染日志 ---
// 旧任务 worker 在新任务开始后以旧 ctx 发送，必须被丢弃，不得写入新任务日志。
func TestT6_StaleWorkerNoPollution(t *testing.T) {
	bg, _ := t6BG(t, "t6-stale")
	cap := &t6EmitCapture{}
	bg.emit = cap.emit
	ctxA := t6Ctx(bg) // 绑定任务 A 身份
	taskA := bg.taskIDSnapshot()
	logA := bg.resultLog.Load()
	// 任务 A 产生 1 行。
	bg.sendTaskResult(ctxA, "scan_result", ScanResult{IP: "1.1.1.1", Port: 443})
	if n := t6LogLen(logA); n != 1 {
		t.Fatalf("任务 A 日志行数 = %d, want 1", n)
	}
	// 结束任务 A 并清理，启动任务 B（同一 session）。
	bg.endTask()
	clearCompletedBackgroundTaskSession()
	_, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	if _, ok := bg.beginTask(cancelB, "task B", "official", nil); !ok {
		t.Fatal("beginTask B failed")
	}
	taskB := bg.taskIDSnapshot()
	if taskB == taskA {
		t.Fatalf("任务 B ID 与 A 相同：%q", taskB)
	}
	logB := bg.resultLog.Load()
	if logB == logA {
		t.Fatal("任务 B 日志指针与 A 相同（应为新日志）")
	}
	ctxB := t6Ctx(bg)
	// 旧 worker（任务 A）以旧 ctx 发送：必须被丢弃。
	before := cap.count("scan_result")
	bg.sendTaskResult(ctxA, "scan_result", ScanResult{IP: "2.2.2.2", Port: 443})
	if got := cap.count("scan_result"); got != before {
		t.Fatalf("旧 worker 发送未被丢弃：emit scan_result %d -> %d", before, got)
	}
	if n := t6LogLen(logB); n != 0 {
		t.Fatalf("新任务日志被旧 worker 污染：行数=%d, want 0", n)
	}
	if ns := t6LogNextSeq(logB); ns != 1 {
		t.Fatalf("新任务 nextSeq = %d, want 1（旧行不得占用新任务序号）", ns)
	}
	// 新 worker（任务 B）发送：正常进入新日志，seq=1。
	bg.sendTaskResult(ctxB, "scan_result", ScanResult{IP: "3.3.3.3", Port: 443})
	if n := t6LogLen(logB); n != 1 {
		t.Fatalf("新任务日志行数 = %d, want 1", n)
	}
	logB.mu.Lock()
	rowB := logB.rows[0]
	logB.mu.Unlock()
	if rowB.Seq != 1 {
		t.Fatalf("新任务首行 seq = %d, want 1", rowB.Seq)
	}
	// 旧任务日志保持隔离：仍只有任务 A 的 1 行，finalSeq=1。
	if n := t6LogLen(logA); n != 1 {
		t.Fatalf("旧任务日志行数 = %d, want 1（隔离被破坏）", n)
	}
	logA.mu.Lock()
	frozen, finalSeq := logA.frozen, logA.finalSeq
	logA.mu.Unlock()
	if !frozen || finalSeq != 1 {
		t.Fatalf("旧任务日志 frozen=%v finalSeq=%d, want true/1", frozen, finalSeq)
	}
}

// 问题 2：旧 worker 与 beginTask() 并发——新任务日志不得混入旧任务行。
// 以 -race 运行，验证 resultLog 指针与身份校验无数据竞争。
func TestT6_StaleWorkerConcurrentBegin(t *testing.T) {
	bg, _ := t6BG(t, "t6-stale-race")
	cap := &t6EmitCapture{}
	bg.emit = cap.emit
	ctxA := t6Ctx(bg)
	bg.sendTaskResult(ctxA, "scan_result", ScanResult{IP: "1.1.1.1", Port: 443})
	bg.endTask()
	clearCompletedBackgroundTaskSession()
	// 10 个旧 worker goroutine 持续以旧 ctx 发送；主 goroutine 启动任务 B。
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					bg.sendTaskResult(ctxA, "scan_result", ScanResult{IP: fmt.Sprintf("9.9.9.%d", i), Port: 443})
				}
			}
		}(i)
	}
	_, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	if _, ok := bg.beginTask(cancelB, "task B", "official", nil); !ok {
		close(stop)
		wg.Wait()
		t.Fatal("beginTask B failed")
	}
	// 任务 B 启动后旧 worker 继续发一会儿，再停止。
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
	// 新任务日志必须为空（旧发送全部被丢弃）；新 worker 发送后只有新行。
	if n := t6LogLen(bg.resultLog.Load()); n != 0 {
		t.Fatalf("并发 beginTask 后新任务日志行数 = %d, want 0（旧行污染）", n)
	}
	if ns := t6LogNextSeq(bg.resultLog.Load()); ns != 1 {
		t.Fatalf("并发 beginTask 后新任务 nextSeq = %d, want 1", ns)
	}
	bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "3.3.3.3", Port: 443})
	if n := t6LogLen(bg.resultLog.Load()); n != 1 {
		t.Fatalf("新 worker 发送后新任务日志行数 = %d, want 1", n)
	}
}

// 问题 2：旧 worker 与日志清理并发——清理后旧发送不得写入已清理/新日志。
func TestT6_StaleWorkerConcurrentCleanup(t *testing.T) {
	bg, _ := t6BG(t, "t6-stale-clean")
	cap := &t6EmitCapture{}
	bg.emit = cap.emit
	ctxA := t6Ctx(bg)
	bg.sendTaskResult(ctxA, "scan_result", ScanResult{IP: "1.1.1.1", Port: 443})
	logA := bg.resultLog.Load()
	bg.endTask()
	// 旧 worker 持续发送；主 goroutine 反复清理。
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				bg.sendTaskResult(ctxA, "scan_result", ScanResult{IP: "9.9.9.9", Port: 443})
			}
		}
	}()
	for i := 0; i < 50; i++ {
		clearCompletedBackgroundTaskSession()
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()
	// 旧日志仍只有任务 A 的 1 行（清理后旧发送被丢弃，不再追加）。
	if n := t6LogLen(logA); n != 1 {
		t.Fatalf("清理并发后旧任务日志行数 = %d, want 1", n)
	}
	// 会话当前日志指针已被清理为 nil（清理路径生效）。
	if bg.resultLog.Load() != nil {
		t.Fatalf("清理后 resultLog 未 nil")
	}
}

// 问题 2 要求 8：replayResults 与任务切换并发——旧任务请求不得拿到新任务数据。
func TestT6_ReplayAfterTaskSwitch(t *testing.T) {
	bg, _ := t6BG(t, "t6-replay-switch")
	cap := &t6EmitCapture{}
	bg.emit = cap.emit
	epoch := currentBackendEpoch()
	taskA := bg.taskIDSnapshot()
	for i := 0; i < 3; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("1.1.1.%d", i+1), Port: 443})
	}
	bg.endTask()
	clearCompletedBackgroundTaskSession()
	_, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	if _, ok := bg.beginTask(cancelB, "task B", "official", nil); !ok {
		t.Fatal("beginTask B failed")
	}
	taskB := bg.taskIDSnapshot()
	for i := 0; i < 2; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("2.2.2.%d", i+1), Port: 443})
	}
	// 旧任务 ID 请求重放：当前日志已是任务 B，应回 task-not-found。
	completed, reason := bg.replayResults(nil, taskA, epoch, 0, "req-old")
	if completed || reason != "task-not-found" {
		t.Fatalf("旧任务重放 = (%v, %q), want (false, \"task-not-found\")", completed, reason)
	}
	// 新任务 ID 请求重放：只返回任务 B 的 2 行（seq 1,2），不含任务 A 的行。
	before := len(cap.all())
	completed, reason = bg.replayResults(nil, taskB, epoch, 0, "req-new")
	if !completed || reason != "" {
		t.Fatalf("新任务重放 = (%v, %q), want (true, \"\")", completed, reason)
	}
	after := cap.all()[before:]
	var rows []int64
	var complete map[string]interface{}
	for _, m := range after {
		if m.typ == "scan_result" {
			if s, ok := t6SeqOf(m.data); ok {
				rows = append(rows, s)
			}
		}
		if m.typ == "replay_complete" {
			complete, _ = m.data.(map[string]interface{})
		}
	}
	if len(rows) != 2 || rows[0] != 1 || rows[1] != 2 {
		t.Fatalf("新任务重放行 seq = %v, want [1 2]", rows)
	}
	if rt, ok := t6Num(complete, "replayThroughSeq"); !ok || rt != 2 {
		t.Fatalf("新任务 replayThroughSeq = %v, want 2", complete["replayThroughSeq"])
	}
}

// 本轮修复：在途重放期间的任务切换归属——任务 A 的重放已开始（已发 3 行、
// 未发完成消息）后，同一 session、同一连接上启动任务 B；旧重放必须失效，
// 不再发送 A 的后续行与 replay_complete，B 的日志/序号/结果不受影响。
// 确定性同步：testReplayPauseGate 在第 3 行写出后暂停重放（entered 通道），
// 测试在此时启动 B，释放后旧重放恢复并应立即中止；全程无 time.Sleep 猜测
// 竞态时机（两个通道等待各带 10s 超时，仅防死锁导致套件挂起）。
func TestT6_ReplayTaskSwitchMidflight(t *testing.T) {
	bg, _ := t6BG(t, "t6-replay-midflight")
	cap := &t6EmitCapture{}
	bg.emit = cap.emit
	epoch := currentBackendEpoch()
	taskA := bg.taskIDSnapshot()
	// 任务 A 产生 10 行后结束（冻结，日志保留待重放）。
	for i := 0; i < 10; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("1.1.1.%d", i+1), Port: 443})
	}
	if n := t6LogLen(bg.resultLog.Load()); n != 10 {
		t.Fatalf("任务 A 日志行数 = %d, want 10", n)
	}
	bg.endTask()

	// 在第 3 行写出后暂停重放（门在锁外触发，beginTask 可取锁，不死锁）。
	gate := &replayPauseGate{afterRows: 3, entered: make(chan struct{}), release: make(chan struct{})}
	testReplayPauseGate = gate
	beforeReplay := len(cap.all()) // 重放开始前已捕获 A 的 10 个实时行
	done := make(chan struct{})
	var completed bool
	var reason string
	go func() {
		defer close(done)
		completed, reason = bg.replayResults(nil, taskA, epoch, 0, "req-midflight-A")
	}()
	// 确定性等待：第 3 行已写出、重放已暂停。
	select {
	case <-gate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("重放未在 10s 内到达第 3 行暂停点")
	}
	replayed := cap.all()[beforeReplay:]
	if len(replayed) != 3 {
		t.Fatalf("暂停时重放已发送 = %d 条，want 3", len(replayed))
	}
	for _, m := range replayed {
		if m.typ != "scan_result" {
			t.Fatalf("暂停时重放出现非行消息 %q，want 仅 scan_result", m.typ)
		}
	}
	if n := cap.count("replay_complete"); n != 0 {
		t.Fatalf("暂停时 replay_complete 已发送，want 0")
	}

	// 同一 session、同一连接上启动任务 B（beginTask 内替换 resultLog）。
	_, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	if _, ok := bg.beginTask(cancelB, "task B", "official", nil); !ok {
		t.Fatal("beginTask B failed")
	}
	taskB := bg.taskIDSnapshot()
	if taskB == taskA {
		t.Fatalf("任务 B taskID 与 A 相同：%q", taskB)
	}
	// B 的日志全新：0 行，nextSeq 从 1 开始（旧重放尚未恢复，未污染）。
	if logB := bg.resultLog.Load(); logB == nil || t6LogLen(logB) != 0 || logB.nextSeq != 1 {
		t.Fatalf("任务 B 日志初始状态异常")
	}

	// 恢复旧重放：应立即中止，不再发送 A 的任何后续行与完成消息。
	close(gate.release)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("旧重放恢复后 10s 内未返回（疑似死锁）")
	}
	testReplayPauseGate = nil
	if completed || reason != "" {
		t.Fatalf("旧重放返回 = (%v, %q), want (false, \"\")（中止、无 rejected）", completed, reason)
	}
	afterAbort := cap.all()[beforeReplay:]
	if len(afterAbort) != 3 {
		t.Fatalf("旧重放恢复后重放段总消息 = %d, want 3（只应有暂停前发出的 3 行）", len(afterAbort))
	}
	if n := cap.count("replay_complete"); n != 0 {
		t.Fatalf("旧重放恢复后 replay_complete = %d, want 0", n)
	}
	// B 的日志仍干净：0 行（旧重放未写入）。
	if n := t6LogLen(bg.resultLog.Load()); n != 0 {
		t.Fatalf("任务 B 日志行数 = %d, want 0（旧重放污染）", n)
	}

	// 任务 B 正常发送实时结果（seq 从 1 开始）。
	for i := 0; i < 2; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("2.2.2.%d", i+1), Port: 443})
	}
	liveB := cap.all()[len(cap.all())-2:]
	var liveSeqs []int64
	for _, m := range liveB {
		if s, ok := t6SeqOf(m.data); ok {
			liveSeqs = append(liveSeqs, s)
		}
	}
	if len(liveSeqs) != 2 || liveSeqs[0] != 1 || liveSeqs[1] != 2 {
		t.Fatalf("任务 B 实时行 seq = %v, want [1 2]", liveSeqs)
	}
	// 任务 B 自己的重放正常：只返回 B 的 2 行（seq 1,2）+ 完成消息。
	before := len(cap.all())
	completed, reason = bg.replayResults(nil, taskB, epoch, 0, "req-midflight-B")
	if !completed || reason != "" {
		t.Fatalf("任务 B 重放 = (%v, %q), want (true, \"\")", completed, reason)
	}
	after := cap.all()[before:]
	var rows []int64
	var complete map[string]interface{}
	for _, m := range after {
		if m.typ == "scan_result" {
			if s, ok := t6SeqOf(m.data); ok {
				rows = append(rows, s)
			}
		}
		if m.typ == "replay_complete" {
			complete, _ = m.data.(map[string]interface{})
		}
	}
	if len(rows) != 2 || rows[0] != 1 || rows[1] != 2 {
		t.Fatalf("任务 B 重放行 seq = %v, want [1 2]", rows)
	}
	if complete == nil {
		t.Fatal("任务 B 重放缺少 replay_complete")
	}
	if rt, ok := t6Num(complete, "replayThroughSeq"); !ok || rt != 2 {
		t.Fatalf("任务 B replayThroughSeq = %v, want 2", complete["replayThroughSeq"])
	}
}

// --- 问题 3：重放期间 WebSocket 连接换绑 ---
// 真实 socket：c1 请求 2000 行重放，收到 50 行后 c2 跟随接管；旧重放必须
// 安全中止——c1 不再收到行与 replay_complete，c2 只收到自己的消息。
func TestT6_ReplayRebindMidway(t *testing.T) {
	bg, _ := t6BG(t, "t6-rebind")
	taskID := bg.taskIDSnapshot()
	// 断线前产生 2000 行（无连接时发送写失败但仍入日志）。
	for i := 0; i < 2000; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("1.1.1.%d", i%250+1), Port: 443})
	}
	if n := t6LogLen(bg.resultLog.Load()); n != 2000 {
		t.Fatalf("日志行数 = %d, want 2000", n)
	}
	_, wsURL := r3Server(t)
	c1 := r3Dial(t, wsURL)
	r1 := t6NewReader(t, c1)
	epoch := t6Follow(t, r1)
	// c1 请求重放（lastSeq=0，全部 2000 行）。
	t6SendReplay(c1, taskID, epoch, 0, "req-t6-rebind")
	// 等 c1 收到 50 行，确认重放已开始。
	got50 := r1.collectRows("scan_result", 50, 10*time.Second)
	if len(got50) != 50 || got50[49] != 50 {
		t.Fatalf("重放开始后 c1 收到 seq = %v, want 1..50", got50)
	}
	// c2 跟随接管（attachWebSocket 把 bg.ws 换成 c2 的连接）。
	c2 := r3Dial(t, wsURL)
	r2 := t6NewReader(t, c2)
	epoch2 := t6Follow(t, r2)
	if epoch2 != epoch {
		t.Fatalf("c2 epoch = %q, want %q", epoch2, epoch)
	}
	// 换绑后：先排空已在途的行（换绑生效前重放已发出的行仍会到达 c1，属合法；
	// 换绑生效后 sendWSMessageToConn 拒绝一切新发送，故排空后应彻底安静）。
	// 持续读 r1 直到 300ms 内无新消息（非 scan_result 放回 pending）。
	drainDeadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := r1.takePending("scan_result"); ok {
			continue // 在途行，丢弃
		}
		if time.Until(drainDeadline) <= 0 {
			break
		}
		msg, ok := r1.next(300 * time.Millisecond)
		if !ok {
			break // 300ms 安静，排空完成
		}
		if mt, _ := msg["type"].(string); mt != "scan_result" {
			r1.pending = append(r1.pending, msg)
		}
	}
	// 排空后断言：旧重放不得再向 c1 发送任何行或 complete；c2 不得收到 replay 行。
	// reader 级别的缺席检查（非目标消息放回 pending，不丢失；目标消息出现即失败）。
	assertAbsent := func(r *t6Reader, typ string, d time.Duration) {
		r.t.Helper()
		deadline := time.Now().Add(d)
		for {
			if m, ok := r.takePending(typ); ok {
				r.t.Fatalf("换绑后仍收到 %s：%v（旧请求未中止）", typ, m)
			}
			rem := time.Until(deadline)
			if rem <= 0 {
				return
			}
			msg, ok := r.next(rem)
			if !ok {
				return
			}
			if mt, _ := msg["type"].(string); mt == typ {
				r.t.Fatalf("换绑后仍收到 %s：%v（旧请求未中止）", typ, msg)
			}
			r.pending = append(r.pending, msg)
		}
	}
	// 断言 1：c1 在 1 秒内不再收到 scan_result 行。
	assertAbsent(r1, "scan_result", 1*time.Second)
	// 断言 2：c1 不收到 replay_complete。
	assertAbsent(r1, "replay_complete", 1*time.Second)
	// 断言 3：c2 在 500ms 内不收到任何 replay 行（只应有自己的 follow/snapshot 消息）。
	assertAbsent(r2, "scan_result", 500*time.Millisecond)
}

// 问题 3 单元级：换绑后 sendReplayMsg 拒绝向旧连接发送，接受向新连接发送。
func TestT6_ReplayMsgRefusedAfterRebind(t *testing.T) {
	bg, _ := t6BG(t, "t6-rebind-unit")
	_, wsURL := r3Server(t)
	c1 := r3Dial(t, wsURL)
	r1 := t6NewReader(t, c1)
	_ = t6Follow(t, r1)
	// 此时 bg.ws 是 c1 的服务端连接。
	conn1, closed1 := bg.wsSnapshot()
	if conn1 == nil || closed1 {
		t.Fatal("follow 后 bg.ws 为空或已关闭")
	}
	// c2 跟随接管。
	c2 := r3Dial(t, wsURL)
	r2 := t6NewReader(t, c2)
	_ = t6Follow(t, r2)
	conn2, closed2 := bg.wsSnapshot()
	if conn2 == nil || closed2 || conn2 == conn1 {
		t.Fatal("接管后 bg.ws 未换成新连接")
	}
	// 向旧连接发送：必须拒绝。
	if ok := bg.sendReplayMsg(conn1, "scan_result", map[string]interface{}{"seq": int64(1)}); ok {
		t.Fatal("换绑后 sendReplayMsg(旧连接) = true, want false")
	}
	// 向新连接发送：必须接受（且 c2 能收到）。
	if ok := bg.sendReplayMsg(conn2, "scan_result", map[string]interface{}{"seq": int64(1)}); !ok {
		t.Fatal("换绑后 sendReplayMsg(新连接) = false, want true")
	}
	if got := r2.collectRows("scan_result", 1, 5*time.Second); len(got) != 1 || got[0] != 1 {
		t.Fatalf("c2 收到 seq = %v, want [1]", got)
	}
	// c1 不得收到该行。
	c1.waitForAbsent("scan_result", 300*time.Millisecond)
}

// --- 问题 4：replay_complete.final 与 finalSeq 不变量 ---
func TestT6_FinalInvariant(t *testing.T) {
	t.Run("frozen during replay", func(t *testing.T) {
		bg, _ := t6BG(t, "t6-final-1")
		cap := &t6EmitCapture{}
		bg.emit = cap.emit
		// 10 行，边界=10（先用普通 emit 完成实时发送）。
		for i := 0; i < 10; i++ {
			bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("1.1.1.%d", i+1), Port: 443})
		}
		// 在重放第 5 行写出后暂停（testReplayPauseGate；门在锁外触发，
		// 重放此时不持有 resultSendMu，测试可正常插入 seq=11 并冻结）。
		// 旧机制（emit 阻塞包装）已不可用：本轮修复后重放行发送在
		// resultSendMu 内完成，旧包装在 emit 内阻塞会与随后需要的
		// sendTaskResult/endTask 互相等待而死锁；断言本身未改。
		blocked := make(chan struct{})
		release := make(chan struct{})
		testReplayPauseGate = &replayPauseGate{afterRows: 5, entered: blocked, release: release}
		// 重放（goroutine）+ 测试插入 seq=11 并冻结。
		done := make(chan [2]interface{}, 1)
		go func() {
			c, r := bg.replayResults(nil, bg.taskIDSnapshot(), currentBackendEpoch(), 0, "req-final-1")
			done <- [2]interface{}{c, r}
		}()
		// 等重放暂停在第 5 行后，产生 seq=11 并冻结（finalSeq=11）。
		select {
		case <-blocked:
		case <-time.After(10 * time.Second):
			t.Fatal("重放未在第 5 行暂停")
		}
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "2.2.2.11", Port: 443})
		bg.endTask()
		close(release)
		select {
		case res := <-done:
			if res[0] != true || res[1] != "" {
				t.Fatalf("replayResults = (%v, %q), want (true, \"\")", res[0], res[1])
			}
		case <-time.After(10 * time.Second):
			t.Fatal("重放未完成")
		}
		testReplayPauseGate = nil
		// replay_complete：replayThroughSeq=10，totalSeq=11，final=false。
		var complete map[string]interface{}
		for _, m := range cap.all() {
			if m.typ == "replay_complete" {
				complete, _ = m.data.(map[string]interface{})
			}
		}
		if complete == nil {
			t.Fatal("未收到 replay_complete")
		}
		if rt, ok := t6Num(complete, "replayThroughSeq"); !ok || rt != 10 {
			t.Fatalf("replayThroughSeq = %v, want 10", complete["replayThroughSeq"])
		}
		if ts, ok := t6Num(complete, "totalSeq"); !ok || ts != 11 {
			t.Fatalf("totalSeq = %v, want 11", complete["totalSeq"])
		}
		if fin, _ := complete["final"].(bool); fin {
			t.Fatalf("final = true, want false（finalSeq=11 != replayThroughSeq=10）")
		}
		// finalSeq=11 与 totalSeq=11 一致；本次重放只发了边界内 10 行。
		log := bg.resultLog.Load()
		log.mu.Lock()
		finalSeq := log.finalSeq
		log.mu.Unlock()
		if finalSeq != 11 {
			t.Fatalf("finalSeq = %d, want 11", finalSeq)
		}
		var rows []int64
		for _, m := range cap.all() {
			if m.typ == "scan_result" {
				if s, ok := t6SeqOf(m.data); ok {
					rows = append(rows, s)
				}
			}
		}
		// 10 行实时 + 10 行重放（seq 1..10 各两次：实时与重放），seq=11 只在实时出现一次。
		if len(rows) != 21 {
			t.Fatalf("scan_result 总数 = %d, want 21（10 实时 + 10 重放 + 1 实时 seq=11）", len(rows))
		}
	})
	t.Run("already frozen", func(t *testing.T) {
		bg, _ := t6BG(t, "t6-final-2")
		cap := &t6EmitCapture{}
		bg.emit = cap.emit
		for i := 0; i < 10; i++ {
			bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("1.1.1.%d", i+1), Port: 443})
		}
		bg.endTask() // 冻结：finalSeq=10
		completed, reason := bg.replayResults(nil, bg.taskIDSnapshot(), currentBackendEpoch(), 0, "req-final-2")
		if !completed || reason != "" {
			t.Fatalf("replayResults = (%v, %q), want (true, \"\")", completed, reason)
		}
		var complete map[string]interface{}
		for _, m := range cap.all() {
			if m.typ == "replay_complete" {
				complete, _ = m.data.(map[string]interface{})
			}
		}
		if rt, ok := t6Num(complete, "replayThroughSeq"); !ok || rt != 10 {
			t.Fatalf("replayThroughSeq = %v, want 10", complete["replayThroughSeq"])
		}
		if ts, ok := t6Num(complete, "totalSeq"); !ok || ts != 10 {
			t.Fatalf("totalSeq = %v, want 10", complete["totalSeq"])
		}
		if fin, _ := complete["final"].(bool); !fin {
			t.Fatalf("final = false, want true（已冻结且 finalSeq=10=replayThroughSeq）")
		}
	})
	t.Run("running no freeze", func(t *testing.T) {
		bg, _ := t6BG(t, "t6-final-3")
		cap := &t6EmitCapture{}
		bg.emit = cap.emit
		for i := 0; i < 10; i++ {
			bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("1.1.1.%d", i+1), Port: 443})
		}
		completed, reason := bg.replayResults(nil, bg.taskIDSnapshot(), currentBackendEpoch(), 0, "req-final-3")
		if !completed || reason != "" {
			t.Fatalf("replayResults = (%v, %q), want (true, \"\")", completed, reason)
		}
		var complete map[string]interface{}
		for _, m := range cap.all() {
			if m.typ == "replay_complete" {
				complete, _ = m.data.(map[string]interface{})
			}
		}
		if rt, ok := t6Num(complete, "replayThroughSeq"); !ok || rt != 10 {
			t.Fatalf("replayThroughSeq = %v, want 10", complete["replayThroughSeq"])
		}
		if ts, ok := t6Num(complete, "totalSeq"); !ok || ts != 10 {
			t.Fatalf("totalSeq = %v, want 10", complete["totalSeq"])
		}
		if fin, _ := complete["final"].(bool); fin {
			t.Fatalf("final = true, want false（任务仍在运行）")
		}
	})
}

// --- 问题 5：8 MiB 结果日志严格上限 ---
// 以实测行字节数为准，直接设定 log.bytes 到边界，验证追加前判定。
func TestT6_StrictByteCap(t *testing.T) {
	// 测一行 scan_result 的实际字节数（seq 取 1 位数时的大小，供边界计算）。
	measureRowLen := func(t *testing.T) int {
		t.Helper()
		bg, _ := t6BG(t, "t6-cap-measure")
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "1.1.1.1", Port: 443})
		log := bg.resultLog.Load()
		log.mu.Lock()
		defer log.mu.Unlock()
		if len(log.rows) != 1 {
			t.Fatalf("测量行数 = %d, want 1", len(log.rows))
		}
		return len(log.rows[0].Data)
	}
	rowLen := measureRowLen(t)
	if rowLen <= 0 {
		t.Fatalf("行字节数 = %d, want > 0", rowLen)
	}
	t.Logf("单行 scan_result 字节数 = %d", rowLen)

	// 辅助：把 log.bytes 设为指定值（直接赋值，测试专用；模拟已累积的日志）。
	setBytes := func(bg *appSession, v int64) {
		log := bg.resultLog.Load()
		log.mu.Lock()
		log.bytes = v
		log.mu.Unlock()
	}
	readState := func(bg *appSession) (n int, byts int64, truncated bool) {
		log := bg.resultLog.Load()
		log.mu.Lock()
		defer log.mu.Unlock()
		return len(log.rows), log.bytes, log.truncated
	}

	t.Run("exact fit", func(t *testing.T) {
		bg, _ := t6BG(t, "t6-cap-fit")
		cap := &t6EmitCapture{}
		bg.emit = cap.emit
		// 先发一行占位，再把 bytes 设为“上限减一行”，使下一行恰好填满。
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "1.1.1.1", Port: 443})
		setBytes(bg, maxResultLogBytes-int64(rowLen))
		before := cap.count("scan_result")
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "1.1.1.2", Port: 443})
		n, byts, truncated := readState(bg)
		if truncated {
			t.Fatalf("恰好填满时 truncated=true, want false")
		}
		if byts != maxResultLogBytes {
			t.Fatalf("恰好填满后 bytes=%d, want %d", byts, maxResultLogBytes)
		}
		if n != 2 {
			t.Fatalf("恰好填满后行数=%d, want 2（应追加）", n)
		}
		if got := cap.count("scan_result"); got != before+1 {
			t.Fatalf("恰好填满的行未发送：%d -> %d", before, got)
		}
	})
	t.Run("one byte over", func(t *testing.T) {
		bg, _ := t6BG(t, "t6-cap-over1")
		cap := &t6EmitCapture{}
		bg.emit = cap.emit
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "1.1.1.1", Port: 443})
		// 距上限差一行减 1 字节：下一行超 1 字节，必须拒绝追加。
		setBytes(bg, maxResultLogBytes-int64(rowLen)+1)
		before := cap.count("scan_result")
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "1.1.1.2", Port: 443})
		n, byts, truncated := readState(bg)
		if !truncated {
			t.Fatalf("超 1 字节时 truncated=false, want true")
		}
		if byts != maxResultLogBytes-int64(rowLen)+1 {
			t.Fatalf("超 1 字节后 bytes=%d, want %d（不得增长）", byts, maxResultLogBytes-int64(rowLen)+1)
		}
		if byts > maxResultLogBytes {
			t.Fatalf("bytes=%d 超过上限 %d", byts, maxResultLogBytes)
		}
		if n != 1 {
			t.Fatalf("超 1 字节后行数=%d, want 1（不得追加）", n)
		}
		if got := cap.count("scan_result"); got != before+1 {
			t.Fatalf("超限行应仍发送：%d -> %d", before, got)
		}
	})
	t.Run("exactly at cap", func(t *testing.T) {
		bg, _ := t6BG(t, "t6-cap-at")
		cap := &t6EmitCapture{}
		bg.emit = cap.emit
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "1.1.1.1", Port: 443})
		setBytes(bg, maxResultLogBytes)
		before := cap.count("scan_result")
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "1.1.1.2", Port: 443})
		n, byts, truncated := readState(bg)
		if !truncated || byts != maxResultLogBytes || n != 1 {
			t.Fatalf("恰达上限时 truncated=%v bytes=%d n=%d, want true/%d/1", truncated, byts, n, maxResultLogBytes)
		}
		if got := cap.count("scan_result"); got != before+1 {
			t.Fatalf("恰达上限的行应仍发送：%d -> %d", before, got)
		}
	})
	t.Run("cap minus 50, 100-byte row", func(t *testing.T) {
		// 问题陈述原例：上限减 50 字节，下一行 100 字节，必须拒绝。
		// 用 speed_test_result（map，仅 ip/speed/seq，基准更小）构造恰好 100 字节的行。
		bg, _ := t6BG(t, "t6-cap-50-100")
		cap := &t6EmitCapture{}
		bg.emit = cap.emit
		// 构造恰好 100 字节的行：以 ip 填充调大小（seq 为 1 位数）。
		payload := map[string]interface{}{"ip": "1.1.1.1", "speed": "10.00MB/s"}
		base := len(mustJSON(withSeq(payload, 1)))
		if base >= 100 {
			t.Fatalf("基准行已 %d 字节，无法构造 100 字节行", base)
		}
		pad := strings.Repeat("x", 100-base)
		payload["ip"] = "1.1.1.1" + pad
		if got := len(mustJSON(withSeq(payload, 1))); got != 100 {
			t.Fatalf("构造行字节数 = %d, want 100", got)
		}
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "1.1.1.1", Port: 443})
		setBytes(bg, maxResultLogBytes-50)
		before := cap.count("speed_test_result")
		bg.sendTaskResult(t6Ctx(bg), "speed_test_result", payload)
		n, byts, truncated := readState(bg)
		if !truncated {
			t.Fatalf("上限-50 遇 100 字节行时 truncated=false, want true")
		}
		if byts != maxResultLogBytes-50 {
			t.Fatalf("bytes=%d, want %d（不得增长）", byts, maxResultLogBytes-50)
		}
		if n != 1 {
			t.Fatalf("行数=%d, want 1（不得追加）", n)
		}
		if got := cap.count("speed_test_result"); got != before+1 {
			t.Fatalf("超限行应仍发送：%d -> %d", before, got)
		}
	})
	t.Run("never exceeds after overflow", func(t *testing.T) {
		bg, _ := t6BG(t, "t6-cap-never")
		cap := &t6EmitCapture{}
		bg.emit = cap.emit
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: "1.1.1.1", Port: 443})
		setBytes(bg, maxResultLogBytes-10)
		for i := 0; i < 100; i++ {
			bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("1.1.1.%d", i%250+1), Port: 443})
			_, byts, _ := readState(bg)
			if byts > maxResultLogBytes {
				t.Fatalf("第 %d 次发送后 bytes=%d 超过上限 %d", i, byts, maxResultLogBytes)
			}
		}
		n, byts, truncated := readState(bg)
		if !truncated {
			t.Fatalf("多次超限后 truncated=false, want true")
		}
		if byts > maxResultLogBytes {
			t.Fatalf("最终 bytes=%d 超过上限 %d", byts, maxResultLogBytes)
		}
		if n != 1 {
			t.Fatalf("超限后行数=%d, want 1（仅首行）", n)
		}
		if got := cap.count("scan_result"); got != 101 {
			t.Fatalf("超限行应全部发送：got %d, want 101", got)
		}
	})
}

// ---------------------------------------------------------------------------
// S7 Batch 1：后端状态机、断线武装 helper 与三阶段计时器回调（v7.10 §9/§9A/§9B/§10A.2/§11.5）。
// 本批测试：T7-02、T7-03、T7-04、T7-09、T7-15、T7-22、T7-23、T7-25，
// 以及 T7-26 的真实读错误武装部分（认领验证留待 Batch 2）。
// 全部测试串行执行（不使用 t.Parallel()），每个测试以 r3Setup 隔离全局状态。
// 窗口值用 var 覆盖缩短，不改生产默认值；同步用通道/测试门/有界轮询，
// 不用 time.Sleep() 猜测竞态时机（有界否定检查除外，已明确标注）。
// ---------------------------------------------------------------------------

// setTestGracePeriod 覆盖 disconnectGracePeriod（生产 30s），测试结束时恢复。
// 须在 r3Dial 之前调用：武装时的 AfterFunc 在武装调用内读该 var。
func setTestGracePeriod(t *testing.T, d time.Duration) {
	t.Helper()
	old := time.Duration(disconnectGracePeriod.Load())
	disconnectGracePeriod.Store(int64(d))
	t.Cleanup(func() { disconnectGracePeriod.Store(int64(old)) })
}

// setTestPingInterval 覆盖 wsPingInterval（生产 30s），测试结束时恢复。
// 须在 r3Dial 之前调用：ping goroutine 启动时在 server.go:170 读该 var。
func setTestPingInterval(t *testing.T, d time.Duration) {
	t.Helper()
	old := time.Duration(wsPingInterval.Load())
	wsPingInterval.Store(int64(d))
	t.Cleanup(func() { wsPingInterval.Store(int64(old)) })
}

// waitFirstPingTick 等待指定连接的 ping goroutine 已执行首个 tick：
// 首 tick 的 pong 使服务端 lastPongAtNano（atomic.Int64）前进；
// 该 Store 与测试侧的 Load 建立 happens-before，而 ticker 创建
// （server.go:170 读 wsPingInterval）在程序序上先于首 tick。
// 只适用于短间隔（100ms）测试；time.Hour 的测试在期间无 tick，不可用。
// 须在连接 RST/关闭之前调用（关闭后 ping 不再 tick，pong 永不到达）。
func waitFirstPingTick(t *testing.T, connID string, timeout time.Duration) {
	t.Helper()
	pollUntil(t, "first ping tick for "+connID, timeout, 20*time.Millisecond, func() bool {
		sess := snapshotServerSession(t, connID)
		return sess != nil && sess.lastPongAtNano.Load() > 0
	})
}

// closeClaimedTask 经 WS stop_task 取消 sess 上的任务并等其结束（10s）。
// 仅用于 longTaskRun 任务（stop_task 能结束它）且认领者连接 c 仍存活时。
// 完成后清理关闭连接不再触发 armDisconnectTimer 读 disconnectGracePeriod
// （任务已停，武装在状态检查处返回），与 setTestGracePeriod 的恢复写无竞争。
// 不得用于：可自然完成的夹具（须关 done，见 c2CompleteTask）、已 reset 的
// session（任务已由 reset 取消，见 T7_50/T7_C2_07）、读循环被阻塞时
// （WS stop_task 发不出去，须直接 stopTask，见 T7_C2_06）、白盒直接
// stopTask 的测试。
func closeClaimedTask(t *testing.T, sess *appSession, c *r3Client) {
	t.Helper()
	c.send("stop_task", nil)
	pollTaskEnded(t, sess, 10*time.Second)
}

// setTestMinPostResumeOpportunity 覆盖 minPostResumeOpportunity（生产 10s，
// 待确认建议值），测试结束时恢复。
func setTestMinPostResumeOpportunity(t *testing.T, d time.Duration) {
	t.Helper()
	old := minPostResumeOpportunity
	minPostResumeOpportunity = d
	t.Cleanup(func() { minPostResumeOpportunity = old })
}

// longTaskRun 是合成长任务：阻塞至 ctx 取消才返回（不发业务行、不做网络 I/O）。
// 用于 Batch 1 的断线/取消测试，避免真实扫描任务的网络时延与不确定性。
func longTaskRun(ctx context.Context, s *appSession) {
	<-ctx.Done()
}

// snapshotDisconnectState 在 taskMutex 内一次性读断线状态、运行标志与计时器字段
// （§1.6 加锁快照规则：取锁—复制—释放—锁外断言）。
func snapshotDisconnectState(t *testing.T, sess *appSession) (disconnectState, bool, *time.Timer) {
	t.Helper()
	sess.taskMutex.Lock()
	defer sess.taskMutex.Unlock()
	return sess.disconnectState, sess.isTaskRunning, sess.disconnectTimer
}

// snapshotDisconnectIdentity 在 taskMutex 内一次性读武装身份字段与当前计时器。
func snapshotDisconnectIdentity(t *testing.T, sess *appSession) (taskID, epoch string, gen uint64, at int64, timer *time.Timer) {
	t.Helper()
	sess.taskMutex.Lock()
	defer sess.taskMutex.Unlock()
	return sess.disconnectArmedTaskID, sess.disconnectArmedEpoch, sess.disconnectArmedGen, sess.disconnectAt, sess.disconnectTimer
}

// pollSessionRunning 轮询 session 进入运行态。
func pollSessionRunning(t *testing.T, sess *appSession, timeout time.Duration) {
	t.Helper()
	pollUntil(t, "session running", timeout, 20*time.Millisecond, func() bool {
		sess.taskMutex.Lock()
		defer sess.taskMutex.Unlock()
		return sess.isTaskRunning
	})
}

// pollTaskEnded 轮询任务结束（isTaskRunning == false）。
func pollTaskEnded(t *testing.T, sess *appSession, timeout time.Duration) {
	t.Helper()
	pollUntil(t, "task ended", timeout, 20*time.Millisecond, func() bool {
		sess.taskMutex.Lock()
		defer sess.taskMutex.Unlock()
		return !sess.isTaskRunning
	})
}

// pollDisconnectState 轮询断线状态 == want。
func pollDisconnectState(t *testing.T, sess *appSession, want disconnectState, timeout time.Duration) {
	t.Helper()
	pollUntil(t, fmt.Sprintf("disconnectState==%d", int(want)), timeout, 20*time.Millisecond, func() bool {
		sess.taskMutex.Lock()
		defer sess.taskMutex.Unlock()
		return sess.disconnectState == want
	})
}

// startSyntheticTask 在服务端 session 上直接启动一个合成任务（与 start_task
// handler 同一 startTaskNamed 入口），返回前不等待任务 goroutine 启动；
// 调用方用 pollSessionRunning 确认运行态。
func startSyntheticTask(t *testing.T, sess *appSession, run taskStarter) {
	t.Helper()
	sess.startTaskNamed("s7 test task", "official", nil, run)
}

// injectFreshWatchdogSnapshot 注入一个新鲜快照（lastTickAt = now），模拟看门狗
// 刚刚 tick 过。本批未实现看门狗 ticker（后续批次），快照不会自动前进；
// 需要“gap 小”才能取消的测试（T7-02/T7-15）须在武装后调用本函数——
// 否则 liveElapsed 将停滞在 interval+tolerance-δ 而永不取消（见交付报告）。
// 须在武装完成后调用（lastTickAt 不得早于 disconnectAt）。
func injectFreshWatchdogSnapshot(t *testing.T) {
	t.Helper()
	testHookSet(&testWatchdogSnapshotOverride, &watchdogState{
		generation:     atomic.LoadUint64(&backendGeneration),
		lastTickAt:     time.Now().UnixNano(),
		suspendedTotal: 0,
	})
	t.Cleanup(func() { testHookSet(&testWatchdogSnapshotOverride, nil) })
}

// injectStaleWatchdogSnapshot 注入一个陈旧快照（lastTickAt = now − staleness），
// 模拟看门狗长时间未 tick。用于 T7-B1-R2：首次回调进入 defer 分支而非 cancel。
// 与 injectFreshWatchdogSnapshot 同理，测试结束时清除。
func injectStaleWatchdogSnapshot(t *testing.T, staleness time.Duration) {
	t.Helper()
	testHookSet(&testWatchdogSnapshotOverride, &watchdogState{
		generation:     atomic.LoadUint64(&backendGeneration),
		lastTickAt:     time.Now().UnixNano() - int64(staleness),
		suspendedTotal: 0,
	})
	t.Cleanup(func() { testHookSet(&testWatchdogSnapshotOverride, nil) })
}

// TestT7_02_GraceExpiryCancels：断线后超过宽限期，计时器到期取消任务。
// 窗口 2s；断线后不重连，等取消完成；取消发生在 [窗口，窗口+2s] 内。
func TestT7_02_GraceExpiryCancels(t *testing.T) {
	r3Setup(t)
	setTestGracePeriod(t, 2*time.Second)
	_, wsURL := r3Server(t)
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	startSyntheticTask(t, sess, func(ctx context.Context, s *appSession) {
		s.sendTaskResult(ctx, "scan_result", ScanResult{IP: "1.2.3.4", Port: 443})
		s.sendTaskResult(ctx, "scan_result", ScanResult{IP: "1.2.3.5", Port: 443})
		<-ctx.Done()
	})
	pollSessionRunning(t, sess, 10*time.Second)
	cA.Close()
	// 等待武装：状态变为 disconnectPending（有界轮询）。
	pollDisconnectState(t, sess, stateDisconnectPending, 10*time.Second)
	// 注入新鲜快照（模拟看门狗 tick）：本批无 ticker，快照 lastTickAt 停在
	// reset 时刻；不注入则 gap 持续增大、trailing 抵消全部经过时间，
	// liveElapsed 停滞而永不取消（见 injectFreshWatchdogSnapshot 注释）。
	injectFreshWatchdogSnapshot(t)
	tArmed := time.Now() // 首次观测到武装的时刻（真实武装时刻在此之前 ≤ 轮询间隔）
	// 等待取消完成：任务结束。
	pollTaskEnded(t, sess, 10*time.Second)
	elapsed := time.Since(tArmed)
	// 取消发生在 [窗口，窗口+2s] 内；观测粒度 20ms，下限允许 100ms 宽容。
	if elapsed < 2*time.Second-100*time.Millisecond || elapsed > 4*time.Second {
		t.Fatalf("cancel elapsed = %v, want within [2s, 4s]", elapsed)
	}
	// 终态与清理：状态 cancelled、计时器字段 nil。
	st, running, timer := snapshotDisconnectState(t, sess)
	if st != stateCancelled {
		t.Fatalf("state = %d, want cancelled", int(st))
	}
	if running || timer != nil {
		t.Fatalf("after expiry cancel: running=%v timerNil=%v, want false/true", running, timer == nil)
	}
	// 旧任务结果行保留在日志中（S6 语义）。
	if n := t6LogLen(sess.resultLog.Load()); n != 2 {
		t.Fatalf("result log rows = %d, want 2 (S6 rows retained after cancel)", n)
	}
	// 槽已释放：新任务可启动。
	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	sessB := snapshotServerSession(t, cB.connectionID)
	if sessB == nil {
		t.Fatal("no server session for B")
	}
	startSyntheticTask(t, sessB, longTaskRun)
	pollSessionRunning(t, sessB, 10*time.Second)
	// 清理：停掉 B 的任务，避免残留阻塞后续测试。
	sessB.stopTask()
	pollTaskEnded(t, sessB, 10*time.Second)
}

// TestT7_03_ActiveCancelBeatsTimer：主动取消与延迟取消同时发生，stop_task
// 在窗口内立即生效，计时器解除，只发一次 task_stopped 语义（无重复 cancel）。
func TestT7_03_ActiveCancelBeatsTimer(t *testing.T) {
	r3Setup(t)
	setTestGracePeriod(t, 2*time.Second)
	// 回调门：计时器回调一旦到达（锁释放后、cancel 前）即关闭 entered；
	// 本测试断言它永不触发（计时器已停）。release 只在 cleanup 关闭
	//（若回调因缺陷到达，cleanup 释放它，避免生产 goroutine 永久阻塞）。
	cbGate := newDisconnectTestGate()
	testHookSet(&testTimerCallbackGate, cbGate)
	t.Cleanup(func() { testHookSet(&testTimerCallbackGate, nil); close(cbGate.release) })
	_, wsURL := r3Server(t)
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	startSyntheticTask(t, sess, longTaskRun)
	pollSessionRunning(t, sess, 10*time.Second)
	cA.Close()
	pollDisconnectState(t, sess, stateDisconnectPending, 10*time.Second)
	// 窗口内主动取消（stop_task 的 in-process 等价：同一 session 上直接调 stopTask；
	// 新连接的 stop_task handler 只取消本连接任务，断线后无法经 WS 取消旧任务）。
	sess.stopTask()
	// 任务立即取消：isTaskRunning false、状态 cancelled、计时器字段 nil。
	pollTaskEnded(t, sess, 10*time.Second)
	st, _, timer := snapshotDisconnectState(t, sess)
	if st != stateCancelled {
		t.Fatalf("state = %d, want cancelled after stopTask", int(st))
	}
	if timer != nil {
		t.Fatal("disconnectTimer not nil after stopTask (timer not stopped)")
	}
	// 计时器回调门在原窗口到期后仍未触发（2.5s 有界否定检查：原窗口 2s 已过）。
	select {
	case <-cbGate.entered:
		t.Fatal("timer callback gate entered after stopTask (timer was not stopped)")
	case <-time.After(2500 * time.Millisecond):
	}
	// 槽已释放：新任务可启动（只发一次 task_stopped 语义的结构性保证：
	// startTaskNamed 的 task_stopped 发送在唯一 defer 内，重复 cancel 不会
	// 使其重复；此处断言槽释放与终态，不重复统计已断开连接上的发送）。
	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	sessB := snapshotServerSession(t, cB.connectionID)
	if sessB == nil {
		t.Fatal("no server session for B")
	}
	startSyntheticTask(t, sessB, longTaskRun)
	pollSessionRunning(t, sessB, 10*time.Second)
	sessB.stopTask()
	pollTaskEnded(t, sessB, 10*time.Second)
}

// TestT7_04_NaturalCompleteClearsTimer：任务自然完成与延迟取消同时发生，
// 窗口内完成不触发误取消。
func TestT7_04_NaturalCompleteClearsTimer(t *testing.T) {
	r3Setup(t)
	setTestGracePeriod(t, 2*time.Second)
	cbGate := newDisconnectTestGate()
	testHookSet(&testTimerCallbackGate, cbGate)
	t.Cleanup(func() { testHookSet(&testTimerCallbackGate, nil); close(cbGate.release) })
	_, wsURL := r3Server(t)
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	release := make(chan struct{})
	startSyntheticTask(t, sess, func(ctx context.Context, s *appSession) {
		<-release // 测试释放后自然返回 → 任务自然完成
	})
	pollSessionRunning(t, sess, 10*time.Second)
	cA.Close()
	pollDisconnectState(t, sess, stateDisconnectPending, 10*time.Second)
	// 窗口内释放任务：自然完成。
	close(release)
	pollTaskEnded(t, sess, 10*time.Second)
	st, _, timer := snapshotDisconnectState(t, sess)
	if st != stateCompleted {
		t.Fatalf("state = %d, want completed after natural finish", int(st))
	}
	if timer != nil {
		t.Fatal("disconnectTimer not nil after natural complete (timer not stopped)")
	}
	// 窗口到期后仍无 cancel 调用：回调门未触发 + 状态保持 completed
	//（2.5s 有界否定检查：原窗口 2s 已过）。
	select {
	case <-cbGate.entered:
		t.Fatal("timer callback gate entered after natural complete (late cancel)")
	case <-time.After(2500 * time.Millisecond):
	}
	st2, _, _ := snapshotDisconnectState(t, sess)
	if st2 != stateCompleted {
		t.Fatalf("state changed after window: %d, want completed", int(st2))
	}
}

// TestT7_09_ResetStopsTimerAndSnapshot：后端关闭时仍有待取消任务，
// resetBackendGeneration 停表 + 取消孤儿 + 重置看门狗快照。
func TestT7_09_ResetStopsTimerAndSnapshot(t *testing.T) {
	r3Setup(t)
	setTestGracePeriod(t, 30*time.Second) // 窗口长，保证 reset 前计时器不触发
	_, wsURL := r3Server(t)
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	startSyntheticTask(t, sess, longTaskRun)
	pollSessionRunning(t, sess, 10*time.Second)
	cA.Close()
	pollDisconnectState(t, sess, stateDisconnectPending, 10*time.Second)
	genBefore := atomic.LoadUint64(&backendGeneration)
	snapBefore := watchdogSnapshot.Load()
	if snapBefore == nil {
		t.Fatal("watchdog snapshot nil before reset")
	}
	tBefore := time.Now()
	resetBackendGeneration()
	// 计时器已停：字段 nil。
	_, _, timer := snapshotDisconnectState(t, sess)
	if timer != nil {
		t.Fatal("disconnectTimer not nil after resetBackendGeneration")
	}
	// 孤儿任务被取消：任务结束、状态 cancelled。
	pollTaskEnded(t, sess, 10*time.Second)
	st, _, _ := snapshotDisconnectState(t, sess)
	if st != stateCancelled {
		t.Fatalf("state = %d, want cancelled after reset", int(st))
	}
	// 看门狗快照已重置：新代次、suspendedTotal=0、lastTickAt ≈ now。
	snap := watchdogSnapshot.Load()
	if snap == nil {
		t.Fatal("watchdog snapshot nil after reset")
	}
	if snap.generation != genBefore+1 {
		t.Fatalf("snapshot generation = %d, want %d", snap.generation, genBefore+1)
	}
	if snap.suspendedTotal != 0 {
		t.Fatalf("snapshot suspendedTotal = %d, want 0", snap.suspendedTotal)
	}
	if snap.lastTickAt < tBefore.UnixNano()-int64(time.Second) || snap.lastTickAt > time.Now().UnixNano() {
		t.Fatalf("snapshot lastTickAt = %d, want ≈ now (%d)", snap.lastTickAt, tBefore.UnixNano())
	}
	// 新代次下旧武装被拒：对旧 session 再次武装返回 false（代次校验）。
	// s.ws 仍是已断开的旧连接（未换绑），归属检查通过，代次检查拦截。
	conn, _ := sess.wsSnapshot()
	if conn == nil {
		t.Fatal("session ws is nil")
	}
	if sess.armDisconnectTimer(conn) {
		t.Fatal("armDisconnectTimer succeeded after generation reset, want false (generation check)")
	}
}

// TestT7_15_NoTimerLeakOverCycles：长时间运行下资源回收，50 次断线→到期，
// 无计时器/goroutine 泄漏。只覆盖本批已实现的生命周期（到期取消路径），
// 不声称 claim 路径已覆盖（认领属 Batch 2）。
func TestT7_15_NoTimerLeakOverCycles(t *testing.T) {
	r3Setup(t)
	setTestGracePeriod(t, 200*time.Millisecond)
	_, wsURL := r3Server(t)
	baseGoroutines := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		c := r3Dial(t, wsURL)
		c.readInit(10 * time.Second)
		sess := snapshotServerSession(t, c.connectionID)
		if sess == nil {
			t.Fatalf("cycle %d: no server session", i)
		}
		startSyntheticTask(t, sess, longTaskRun)
		pollSessionRunning(t, sess, 10*time.Second)
		c.Close()
		pollDisconnectState(t, sess, stateDisconnectPending, 10*time.Second)
		// 注入新鲜快照（模拟看门狗 tick），否则到期评估将因陈旧快照 defer
		// 而永不取消（见 injectFreshWatchdogSnapshot 注释）。
		injectFreshWatchdogSnapshot(t)
		pollTaskEnded(t, sess, 10*time.Second)
		st, _, timer := snapshotDisconnectState(t, sess)
		if st != stateCancelled {
			t.Fatalf("cycle %d: state = %d, want cancelled", i, int(st))
		}
		if timer != nil {
			t.Fatalf("cycle %d: disconnectTimer not nil after expiry", i)
		}
	}
	// goroutine 数回到基线（±2）：给 handler/ping/sender goroutine 退出
	// 留出有界等待（轮询，非 Sleep 猜测）。
	pollUntil(t, "goroutines back to baseline", 10*time.Second, 50*time.Millisecond, func() bool {
		return runtime.NumGoroutine() <= baseGoroutines+2
	})
	if got := runtime.NumGoroutine(); got > baseGoroutines+2 {
		t.Fatalf("goroutine count = %d, baseline %d (leak > 2)", got, baseGoroutines)
	}
	// 无存活计时器：connLoopState 中全部 session 的计时器字段为 nil。
	//（sweeper 兜底：本批测试环境不启动 stateSweeper，断言改为“无残留
	// 待取消任务”——全部 session 均已终态且计时器字段为 nil。）
	backgroundTaskMutex.Lock()
	sessions := make([]*appSession, 0, len(connLoopState))
	for _, e := range connLoopState {
		sessions = append(sessions, e.session)
	}
	backgroundTaskMutex.Unlock()
	for _, s := range sessions {
		st, _, timer := snapshotDisconnectState(t, s)
		if timer != nil {
			t.Fatalf("live timer on session %s after 50 cycles (state=%d)", s.connectionID, int(st))
		}
		if st != stateCancelled && st != stateCompleted {
			t.Fatalf("session %s not terminal after 50 cycles (state=%d)", s.connectionID, int(st))
		}
	}
}

// TestT7_22_EvaluateDisconnect：决策函数表驱动单元测试（纯函数，无计时器、
// 无 goroutine）。全部断言精确值，不用 Sleep。
func TestT7_22_EvaluateDisconnect(t *testing.T) {
	r3Setup(t) // 固定代次：快照 generation 须等于当前代次
	gen := atomic.LoadUint64(&backendGeneration)
	now := time.Now().UnixNano()
	sec := func(n int64) int64 { return n * int64(time.Second) }
	window := 30 * time.Second
	cases := []struct {
		name       string
		snap       *watchdogState
		disconnect int64
		at         int64
		window     time.Duration
		wantCancel bool
		wantRearm  time.Duration
	}{
		// 正常：gap 0.5s，live 30s → cancel（同时是 live == window 的边界）。
		{"normal cancel", &watchdogState{generation: gen, lastTickAt: now - sec(0) - int64(500*time.Millisecond)}, now - sec(30), now, window, true, 0},
		// 边界下 1ns：live = window - 1ns → defer，重武装 = 1ns。
		{"boundary below window", &watchdogState{generation: gen, lastTickAt: now - int64(500*time.Millisecond)}, now - sec(30) + 1, now, window, false, time.Nanosecond},
		// 刚恢复：gap 300s，live 25s → defer，重武装 = max(30-25, 10) = 10s。
		{"just resumed gap 300s live 25s", &watchdogState{generation: gen, lastTickAt: now - sec(300)}, now - sec(323), now, window, false, 10 * time.Second},
		// 恢复前已接近窗口：live 29s，gap 300s → defer，重武装 10s。
		{"near window before resume", &watchdogState{generation: gen, lastTickAt: now - sec(300)}, now - sec(327), now, window, false, 10 * time.Second},
		// 看门狗饿死 6s：gap 6s → defer（安全方向），重武装 = max(30-(-3), 10) = 33s。
		{"watchdog starved 6s", &watchdogState{generation: gen, lastTickAt: now - sec(6)}, now - sec(1), now, window, false, 33 * time.Second},
		// live 不足且 gap 小：defer，重武装 = 剩余时间 20s。
		{"live short gap small", &watchdogState{generation: gen, lastTickAt: now - int64(500*time.Millisecond)}, now - sec(10), now, window, false, 20 * time.Second},
		// nil 快照 → defer，重武装取最小重连机会 10s。
		{"nil snapshot", nil, now - sec(30), now, window, false, 10 * time.Second},
		// 代次不匹配 → defer，重武装取最小重连机会 10s。
		{"generation mismatch", &watchdogState{generation: gen + 1, lastTickAt: now - int64(500*time.Millisecond)}, now - sec(30), now, window, false, 10 * time.Second},
		// 已累计挂起 5s：live = 35 - 5 = 30s → cancel（累计扣除生效）。
		{"suspended total deducted", &watchdogState{generation: gen, lastTickAt: now - int64(500*time.Millisecond), suspendedTotal: sec(5)}, now - sec(35), now, window, true, 0},
		// gap 恰好等于阈值 5s：gap > threshold 不成立，走 liveElapsed 分支；
		// trailing = 3s，live = 33 - 3 = 30s → cancel。
		{"gap exactly threshold", &watchdogState{generation: gen, lastTickAt: now - sec(5)}, now - sec(33), now, window, true, 0},
		// T7-23 的集成数值（窗口 2s）：gap 6s、live -2s → defer，重武装 =
		// max(2-(-2), minPostResumeOpportunity=10s) = 10s（生产默认值）；
		// T7-23 集成测试把 minPostResumeOpportunity 覆盖为 500ms，
		// 同样数值在那里得重武装 4s。
		{"t7-23 numbers window 2s", &watchdogState{generation: gen, lastTickAt: now - sec(6)}, now - sec(2), now, 2 * time.Second, false, 10 * time.Second},
		// 设计示例的 300s 陈旧快照（窗口 2s）：live = 2 - 298 = -296s，
		// 重武装 = max(2+296, 10) = 298s——公式原值；正因如此 T7-23 集成
		// 测试不用 300s（有界时间内等不到重武装到期），见 T7-23 注释。
		{"design 300s stale window 2s", &watchdogState{generation: gen, lastTickAt: now - sec(300)}, now - sec(2), now, 2 * time.Second, false, 298 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotCancel, gotRearm := evaluateDisconnect(tc.at, tc.snap, tc.disconnect, tc.window)
			if gotCancel != tc.wantCancel || gotRearm != tc.wantRearm {
				t.Fatalf("evaluateDisconnect = (cancel=%v, rearm=%v), want (cancel=%v, rearm=%v)",
					gotCancel, gotRearm, tc.wantCancel, tc.wantRearm)
			}
		})
	}
}

// TestT7_23_StaleSnapshotDefersThenCancels：计时器回调在快照陈旧时推迟、
// 快照新鲜后取消（集成）。窗口用测试 var 缩短；hook 注入陈旧快照；
// 计时器到期 → 未取消且已重武装；注入新鲜快照 → 重武装到期 → 取消。
//
// 与设计示例的偏差（已论证，见交付报告）：设计写“lastTickAt = now − 300s”，
// 但按 §11.5.3 公式，300s 陈旧在窗口 2s 下的重武装为 298s（T7-22 已锁定
// 该原值），有界时间内等不到重武装到期；且本批未启动看门狗，任何注入的
// 新鲜快照在 5s 后都会再次变陈旧，重武装必须短于 5s 才能在第二次评估时
// 仍被视为新鲜。本测试用陈旧 4s（首次到期 gap 6s > 5s 阈值，同一 defer
// 分支、同一重武装机制），重武装 4s，第二次评估时 gap 4s ≤ 5s 且
// liveElapsed 4s ≥ 窗口 2s → 取消。
func TestT7_23_StaleSnapshotDefersThenCancels(t *testing.T) {
	r3Setup(t)
	setTestGracePeriod(t, 2*time.Second)
	setTestMinPostResumeOpportunity(t, 500*time.Millisecond)
	var rearmCount atomic.Int32
	testHookSet(&testTimerRearmHook, func(rearm time.Duration) { rearmCount.Add(1) })
	t.Cleanup(func() { testHookSet(&testTimerRearmHook, nil) })
	_, wsURL := r3Server(t)
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	startSyntheticTask(t, sess, longTaskRun)
	pollSessionRunning(t, sess, 10*time.Second)
	// 注入陈旧快照（lastTickAt = now − 4s），在断线前注入。
	testHookSet(&testWatchdogSnapshotOverride, &watchdogState{
		generation:     atomic.LoadUint64(&backendGeneration),
		lastTickAt:     time.Now().UnixNano() - int64(4*time.Second),
		suspendedTotal: 0,
	})
	t.Cleanup(func() { testHookSet(&testWatchdogSnapshotOverride, nil) })
	cA.Close()
	pollDisconnectState(t, sess, stateDisconnectPending, 10*time.Second)
	// 计时器到期 → 未取消且已重武装恰好一次。
	pollUntil(t, "rearm count == 1", 10*time.Second, 50*time.Millisecond, func() bool {
		return rearmCount.Load() == 1
	})
	st, running, _ := snapshotDisconnectState(t, sess)
	if !running || st != stateDisconnectPending {
		t.Fatalf("after stale-snapshot expiry: state=%d running=%v, want disconnectPending/running", int(st), running)
	}
	// 注入新鲜快照（模拟看门狗 tick）→ 重武装到期 → 取消。
	testHookSet(&testWatchdogSnapshotOverride, &watchdogState{
		generation:     atomic.LoadUint64(&backendGeneration),
		lastTickAt:     time.Now().UnixNano(),
		suspendedTotal: 0,
	})
	pollTaskEnded(t, sess, 15*time.Second)
	st2, _, timer := snapshotDisconnectState(t, sess)
	if st2 != stateCancelled {
		t.Fatalf("state = %d, want cancelled after fresh snapshot", int(st2))
	}
	if timer != nil {
		t.Fatal("disconnectTimer not nil after cancel")
	}
	if got := rearmCount.Load(); got != 1 {
		t.Fatalf("rearm count = %d, want 1 (only the stale-snapshot defer rearms)", got)
	}
}

// TestT7_25_DecisionIndependentOfWatchdogOrder：决策不依赖看门狗执行顺序
// （顺序无关性证明）。本批未启动看门狗（无 ticker、无 goroutine）：
// 先以有界观测确认快照不自动前进，再用陈旧快照同步调用回调的决策路径，
// 决策为 defer——证明回调不需要看门狗先执行。
func TestT7_25_DecisionIndependentOfWatchdogOrder(t *testing.T) {
	r3Setup(t)
	setTestGracePeriod(t, 30*time.Second) // 窗口长，保证同步调用期间原计时器不触发
	// 无看门狗在运行：快照在 1.2s（> tick 间隔 1s）内不自动前进。
	//（有界否定观测：若有 ticker 在跑，lastTickAt 必在 1s 内前进。）
	snap0 := watchdogSnapshot.Load()
	if snap0 == nil {
		t.Fatal("watchdog snapshot nil")
	}
	time.Sleep(1200 * time.Millisecond)
	snap1 := watchdogSnapshot.Load()
	if snap1.lastTickAt != snap0.lastTickAt {
		t.Fatal("watchdog snapshot advanced without a watchdog (unexpected ticker running)")
	}
	_, wsURL := r3Server(t)
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	startSyntheticTask(t, sess, longTaskRun)
	pollSessionRunning(t, sess, 10*time.Second)
	cA.Close()
	pollDisconnectState(t, sess, stateDisconnectPending, 10*time.Second)
	// 注入陈旧快照（lastTickAt = now − 300s），同步调用本次武装的回调：
	// 不等待计时器触发、不依赖任何看门狗 goroutine。
	testHookSet(&testWatchdogSnapshotOverride, &watchdogState{
		generation:     atomic.LoadUint64(&backendGeneration),
		lastTickAt:     time.Now().UnixNano() - int64(300*time.Second),
		suspendedTotal: 0,
	})
	t.Cleanup(func() { testHookSet(&testWatchdogSnapshotOverride, nil) })
	var rearmCount atomic.Int32
	testHookSet(&testTimerRearmHook, func(rearm time.Duration) { rearmCount.Add(1) })
	t.Cleanup(func() { testHookSet(&testTimerRearmHook, nil) })
	taskID, epoch, gen, at, timer := snapshotDisconnectIdentity(t, sess)
	if timer == nil {
		t.Fatal("disconnectTimer nil before synchronous callback")
	}
	sess.onDisconnectTimer(timer, taskID, epoch, gen, at)
	// 决策为 defer：未取消、已重武装恰好一次、状态仍为 disconnectPending。
	if got := rearmCount.Load(); got != 1 {
		t.Fatalf("rearm count = %d, want 1 (stale snapshot must defer)", got)
	}
	st, running, _ := snapshotDisconnectState(t, sess)
	if !running || st != stateDisconnectPending {
		t.Fatalf("after synchronous stale-snapshot decision: state=%d running=%v, want disconnectPending/running", int(st), running)
	}
	// 清理：主动取消（停重武装计时器与原计时器），避免残留阻塞后续测试。
	sess.stopTask()
	pollTaskEnded(t, sess, 10*time.Second)
}

// TestT7_26_ReadLoopArmAndClaim：真实读循环错误分支武装 + Defer A no-op +
// 认领成功。必须走真实 r3Server 与客户端 Close，不得只直接调用 helper。
// 证明：读循环错误分支确实调用 helper 并成功武装；Defer A 的 helper 调用
// 是无副作用的幂等 no-op（计时器指针不变）；其后同一 pageId 的新连接 B
// 认领成功，任务恢复运行、计时器已停。
func TestT7_26_ReadLoopArmAndClaim(t *testing.T) {
	r3Setup(t)
	setTestGracePeriod(t, 30*time.Second) // 窗口长，保证测试期间计时器不触发
	armGate := newDisconnectTestGate()
	testHookSet(&testDisconnectArmGate, armGate)
	deferGate := newDisconnectTestGate()
	testHookSet(&testDeferAEntryGate, deferGate)
	var releaseArmOnce, releaseDeferOnce sync.Once
	releaseArm := func() { releaseArmOnce.Do(func() { close(armGate.release) }) }
	releaseDefer := func() { releaseDeferOnce.Do(func() { close(deferGate.release) }) }
	t.Cleanup(func() {
		testHookSet(&testDisconnectArmGate, nil)
		testHookSet(&testDeferAEntryGate, nil)
		releaseArm()
		releaseDefer()
	})
	_, wsURL := r3Server(t)
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register("test-t7-26-page")
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	startSyntheticTask(t, sess, longTaskRun)
	pollSessionRunning(t, sess, 10*time.Second)
	// 真实断开：客户端 Close → 服务端读循环读到错误 → 错误分支武装。
	cA.Close()
	// 武装门到达：错误分支的 armDisconnectTimer 已通过归属检查、持有
	// taskMutex、尚未转换状态。此时 wsClosed 已置位（错误分支先标记），
	// 可在 wsMutex 内读到；门内无法在 taskMutex 内读状态（会阻塞至门释放），
	// “仍是 running”由门位置（转换之前）+ 释放后的状态变迁共同证明。
	select {
	case <-armGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for testDisconnectArmGate (read-loop error branch did not arm)")
	}
	conn, closed := sess.wsSnapshot()
	if !closed {
		t.Fatal("wsClosed == false when arm gate reached, want true (read loop marks death first)")
	}
	if conn == nil {
		t.Fatal("session ws is nil")
	}
	// 释放武装门 → 武装完成。
	releaseArm()
	pollDisconnectState(t, sess, stateDisconnectPending, 10*time.Second)
	_, _, timer1 := snapshotDisconnectState(t, sess)
	if timer1 == nil {
		t.Fatal("disconnectTimer nil after read-loop arm")
	}
	// Defer A 随后执行（安全网）：其 helper 调用必须是无副作用的 no-op。
	select {
	case <-deferGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for testDeferAEntryGate")
	}
	releaseDefer()
	pollLoopExited(t, cA.connectionID, 10*time.Second)
	// Defer A 的 arm 若重复武装会替换计时器指针：500ms 有界稳定窗口内
	// 轮询指针与状态，任何变化即失败。
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		st, _, timer2 := snapshotDisconnectState(t, sess)
		if st != stateDisconnectPending {
			t.Fatalf("state = %d after Defer A, want disconnectPending", int(st))
		}
		if timer2 != timer1 {
			t.Fatal("disconnectTimer replaced by Defer A arm, want same pointer (idempotent no-op)")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// 补全（C1）：Defer A no-op 之后，同一 pageId 的新连接 B 认领成功。
	cB, _ := dialClaimer(t, wsURL, "test-t7-26-page")
	taskID := sess.taskIDSnapshot()
	epoch := currentBackendEpoch()
	cB.claim(taskID, epoch, "test-t7-26-page")
	cB.waitResumeAccepted(10*time.Second, taskID, epoch)
	st2, running2, timer3 := snapshotDisconnectState(t, sess)
	if !running2 || st2 != stateRunning || timer3 != nil {
		t.Fatalf("after claim: state=%d running=%v timerSet=%v, want running/true/false",
			int(st2), running2, timer3 != nil)
	}
	// 清理：B 的 stop_task 取消已认领任务，避免 30s 计时器残留。
	closeClaimedTask(t, sess, cB)
}

// TestT7_B1_R1_PublishBarrierFirstArm：首次武装的发布完成屏障（Batch 1 修正回归）。
// 场景：发布线程在 AfterFunc 已返回、Store 尚未发生时被发布门暂停；timer 已到期，
// 回调已启动但必须停在 ready 等待处，不得提前 Load/V6/进入回调主体；释放发布线程后，
// 回调 Load 到的指针必须与 s.disconnectTimer 同一指针，V6 通过后按设计取消任务。
// 四阶段以门/hook 区分：回调已启动（started hook）、被屏障阻塞（入口门 300ms 有界
// 否定检查）、指针已发布（loaded hook 与字段直接比较）、V6 已执行（阶段门后任务取消）。
func TestT7_B1_R1_PublishBarrierFirstArm(t *testing.T) {
	r3Setup(t)
	setTestGracePeriod(t, 100*time.Millisecond)
	publishGate := newDisconnectTestGate()
	testHookSet(&testTimerPublishGate, publishGate)
	var releasePublishOnce sync.Once
	releasePublish := func() { releasePublishOnce.Do(func() { close(publishGate.release) }) }
	var startedOnce sync.Once
	startedCh := make(chan struct{})
	testHookSet(&testTimerCallbackStartedHook, func() { startedOnce.Do(func() { close(startedCh) }) })
	entryGate := newDisconnectTestGate()
	testHookSet(&testTimerCallbackEntryGate, entryGate)
	var releaseEntryOnce sync.Once
	releaseEntry := func() { releaseEntryOnce.Do(func() { close(entryGate.release) }) }
	phaseGate := newDisconnectTestGate()
	testHookSet(&testTimerCallbackPhaseGate, phaseGate)
	var releasePhaseOnce sync.Once
	releasePhase := func() { releasePhaseOnce.Do(func() { close(phaseGate.release) }) }
	loadedCh := make(chan *time.Timer, 4)
	testHookSet(&testTimerCallbackLoadedHook, func(myTimer *time.Timer) { loadedCh <- myTimer })
	var rearmCount atomic.Int32
	testHookSet(&testTimerRearmHook, func(rearm time.Duration) { rearmCount.Add(1) })
	t.Cleanup(func() {
		testHookSet(&testTimerPublishGate, nil)
		testHookSet(&testTimerCallbackStartedHook, nil)
		testHookSet(&testTimerCallbackEntryGate, nil)
		testHookSet(&testTimerCallbackPhaseGate, nil)
		testHookSet(&testTimerCallbackLoadedHook, nil)
		testHookSet(&testTimerRearmHook, nil)
		releasePublish()
		releaseEntry()
		releasePhase()
	})
	_, wsURL := r3Server(t)
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	startSyntheticTask(t, sess, longTaskRun)
	pollSessionRunning(t, sess, 10*time.Second)
	conn, _ := sess.wsSnapshot()
	if conn == nil {
		t.Fatal("session ws is nil")
	}
	// 在独立 goroutine 中调用生产武装 helper：发布门阻塞调用线程（持有 taskMutex），
	// 测试 goroutine 必须保持可调度以完成观测与释放。
	armDone := make(chan bool, 1)
	go func() { armDone <- sess.armDisconnectTimer(conn) }()
	// 阶段 1：发布线程已到达 AfterFunc 之后、Store 之前。
	select {
	case <-publishGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for testTimerPublishGate (arm did not reach publish point)")
	}
	// 阶段 2：timer 已到期、回调已启动（started hook 已触发）。
	select {
	case <-startedCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for callback start (timer did not fire while publish paused)")
	}
	// 阶段 3：有界否定检查——回调不得进入 onDisconnectTimer（未执行 Load/V6/主体）。
	// ready 仍未关闭（发布门未释放），回调不可能通过屏障；300ms 是给回调 goroutine
	// 被调度到 ready 等待处的时间，不是猜测时序（见 T7-03/T7-04 的有界否定检查先例）。
	select {
	case <-entryGate.entered:
		t.Fatal("callback entered onDisconnectTimer before publish completed (barrier bypassed)")
	case <-time.After(300 * time.Millisecond):
	}
	// 阶段 4：释放发布线程 → Store → 字段赋值 → close(ready)。
	releasePublish()
	select {
	case armed := <-armDone:
		if !armed {
			t.Fatal("armDisconnectTimer returned false")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for armDisconnectTimer to return")
	}
	_, _, armedTimer := snapshotDisconnectState(t, sess)
	if armedTimer == nil {
		t.Fatal("disconnectTimer nil after arm")
	}
	// 阶段 5：回调通过屏障 → Load → 入口门。
	select {
	case <-entryGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for callback entry after publish release")
	}
	// Load 返回的指针必须与字段同一指针（直接比较，非经 V6 推断）。
	select {
	case loaded := <-loadedCh:
		if loaded != armedTimer {
			t.Fatalf("loaded timer %p != s.disconnectTimer %p", loaded, armedTimer)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for loaded-timer observation")
	}
	// 注入新鲜快照（武装后、V6 前）→ 阶段 2 重算必为 cancel（窗口 100ms 已过）。
	injectFreshWatchdogSnapshot(t)
	releaseEntry()
	// 阶段门：回调暂停于 V6 之前；此时状态仍为 disconnectPending、字段仍为 armedTimer。
	select {
	case <-phaseGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for phase gate")
	}
	st, _, timerBeforeV6 := snapshotDisconnectState(t, sess)
	if st != stateDisconnectPending || timerBeforeV6 != armedTimer {
		t.Fatalf("before V6: state=%d timer==armed:%v, want disconnectPending/true", int(st), timerBeforeV6 == armedTimer)
	}
	releasePhase()
	// V6 通过 → cancel：任务结束、状态 cancelled、计时器字段 nil、零次重武装。
	pollTaskEnded(t, sess, 10*time.Second)
	st2, _, timer2 := snapshotDisconnectState(t, sess)
	if st2 != stateCancelled {
		t.Fatalf("state = %d, want cancelled", int(st2))
	}
	if timer2 != nil {
		t.Fatal("disconnectTimer not nil after cancel")
	}
	if got := rearmCount.Load(); got != 0 {
		t.Fatalf("rearm count = %d, want 0 (cancel path must not rearm)", got)
	}
}

// TestT7_B1_R2_PublishBarrierRearm：重武装的发布完成屏障（Batch 1 修正回归）。
// 场景：首次回调进入 defer 分支重武装；重武装发布线程在 AfterFunc 已返回、Store
// 尚未发生时被发布门暂停；新 timer 已到期，新回调已启动但必须停在自己的 ready
// 等待处，不得提前 Load/V6；释放后新回调 Load 到的指针必须等于 s.disconnectTimer，
// V6 通过后再次 defer；旧回调不得覆盖新 timer 或错误取消任务。
// 陈旧度 1.6s 的选取：首次评估 liveElapsed = 2s − 1.6s = 0.4s（与触发时延无关，
// 因 gap 与经过时间同步增长而抵消），窗口 500ms → defer、重武装约 100ms；
// 与 T7-23 的 4s 陈旧（重武装 4s）同分支但更快。
// 确定性说明：两个发布门分别控制首次武装（publishGateA）与重武装（publishGateB）；
// 第一代回调在阶段门 phaseGate1 暂停期间，测试有不受时序限制的时间换入
// publishGateB，第二代回调在 ready2 暂停期间换入 phaseGate2——没有任何门需要在
// 回调飞行途中抢装。
func TestT7_B1_R2_PublishBarrierRearm(t *testing.T) {
	r3Setup(t)
	setTestGracePeriod(t, 500*time.Millisecond)
	setTestMinPostResumeOpportunity(t, 100*time.Millisecond)
	var rearmCount atomic.Int32
	testHookSet(&testTimerRearmHook, func(rearm time.Duration) { rearmCount.Add(1) })
	var loadsMu sync.Mutex
	var loads []*time.Timer
	testHookSet(&testTimerCallbackLoadedHook, func(myTimer *time.Timer) {
		loadsMu.Lock()
		loads = append(loads, myTimer)
		loadsMu.Unlock()
	})
	loadsLen := func() int {
		loadsMu.Lock()
		defer loadsMu.Unlock()
		return len(loads)
	}
	loadsAt := func(i int) *time.Timer {
		loadsMu.Lock()
		defer loadsMu.Unlock()
		return loads[i]
	}
	var startedCount atomic.Int32
	started2Ch := make(chan struct{})
	var started2Once sync.Once
	testHookSet(&testTimerCallbackStartedHook, func() {
		if startedCount.Add(1) == 2 {
			started2Once.Do(func() { close(started2Ch) })
		}
	})
	publishGateA := newDisconnectTestGate()
	testHookSet(&testTimerPublishGate, publishGateA)
	var releasePublishAOnce sync.Once
	releasePublishA := func() { releasePublishAOnce.Do(func() { close(publishGateA.release) }) }
	publishGateB := newDisconnectTestGate()
	var releasePublishBOnce sync.Once
	releasePublishB := func() { releasePublishBOnce.Do(func() { close(publishGateB.release) }) }
	phaseGate1 := newDisconnectTestGate()
	testHookSet(&testTimerCallbackPhaseGate, phaseGate1)
	var releasePhase1Once sync.Once
	releasePhase1 := func() { releasePhase1Once.Do(func() { close(phaseGate1.release) }) }
	phaseGate2 := newDisconnectTestGate()
	var releasePhase2Once sync.Once
	releasePhase2 := func() { releasePhase2Once.Do(func() { close(phaseGate2.release) }) }
	t.Cleanup(func() {
		testHookSet(&testTimerRearmHook, nil)
		testHookSet(&testTimerCallbackLoadedHook, nil)
		testHookSet(&testTimerCallbackStartedHook, nil)
		testHookSet(&testTimerPublishGate, nil)
		testHookSet(&testTimerCallbackPhaseGate, nil)
		releasePublishA()
		releasePublishB()
		releasePhase1()
		releasePhase2()
	})
	_, wsURL := r3Server(t)
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	startSyntheticTask(t, sess, longTaskRun)
	pollSessionRunning(t, sess, 10*time.Second)
	// 注入陈旧快照（断线前）：首次回调进入 defer 分支（见函数注释的算术）。
	injectStaleWatchdogSnapshot(t, 1600*time.Millisecond)
	// 真实断开：客户端 Close → 服务端读循环错误分支武装；首次发布暂停于 publishGateA。
	cA.Close()
	select {
	case <-publishGateA.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for first publish gate (read-loop arm did not reach publish point)")
	}
	releasePublishA()
	pollDisconnectState(t, sess, stateDisconnectPending, 10*time.Second)
	_, _, timer1 := snapshotDisconnectState(t, sess)
	if timer1 == nil {
		t.Fatal("disconnectTimer nil after arm")
	}
	// 换入重武装发布门：第一代回调停在 phaseGate1（V6 之前），不可能在换入前到达重武装发布点。
	testHookSet(&testTimerPublishGate, publishGateB)
	// 第一代回调：入口 → 阶段 1 → 阶段门（V6 之前暂停）。
	select {
	case <-phaseGate1.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for first callback phase gate")
	}
	// 第一代回调已 Load：loads[0] 必须等于首次武装的 timer。
	pollUntil(t, "first callback loaded timer", 10*time.Second, 20*time.Millisecond, func() bool {
		return loadsLen() >= 1
	})
	if got := loadsAt(0); got != timer1 {
		t.Fatalf("first loaded timer %p != armed timer %p", got, timer1)
	}
	releasePhase1()
	// 第一代回调进入阶段 3 defer：重武装发布线程到达 AfterFunc 之后、Store 之前。
	select {
	case <-publishGateB.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for rearm publish gate (first callback did not reach rearm publish point)")
	}
	// 此时重武装尚未完成：hook 未触发（不读 taskMutex——发布门持有期间第一代回调
	// 持有 taskMutex，测试不得在门内读状态；字段未变的结论由“唯一写者被阻塞”保证，
	// 见释放后的直接验证）。
	if got := rearmCount.Load(); got != 0 {
		t.Fatalf("rearm count = %d before publish release, want 0", got)
	}
	// 第二代回调已启动（新 timer 约 100ms 到期），停在自己的 ready 等待处。
	select {
	case <-started2Ch:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for second callback start")
	}
	// 有界否定检查（300ms 内轮询）：第二代回调不得 Load（loads 仍为 1），
	// 不得执行 V6，不得完成重武装（rearm 仍为 0）。
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if loadsLen() != 1 {
			t.Fatalf("second callback loaded timer before publish completed (loads=%d)", loadsLen())
		}
		if got := rearmCount.Load(); got != 0 {
			t.Fatalf("rearm completed before publish release (count=%d)", got)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// 在释放发布门之前换入第二代回调的阶段门：第二代回调仍停在 ready2，尚未读阶段门变量。
	testHookSet(&testTimerCallbackPhaseGate, phaseGate2)
	// 释放发布线程 → Store(T2) → 字段=T2 → close(ready2) → 第一代回调完成重武装。
	releasePublishB()
	pollUntil(t, "rearm count == 1", 10*time.Second, 20*time.Millisecond, func() bool {
		return rearmCount.Load() == 1
	})
	// 第二代回调：通过屏障 → Load → 阶段 1 → 阶段门（V6 之前暂停）。
	select {
	case <-phaseGate2.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for second callback phase gate")
	}
	// 第二代回调 Load 的指针必须等于字段（此时字段仍为 T2，第二代回调尚未重武装）。
	pollUntil(t, "second callback loaded timer", 10*time.Second, 20*time.Millisecond, func() bool {
		return loadsLen() >= 2
	})
	_, _, timer2 := snapshotDisconnectState(t, sess)
	if timer2 == nil || timer2 == timer1 {
		t.Fatalf("s.disconnectTimer after rearm = %p, want a new timer != first %p", timer2, timer1)
	}
	if got := loadsAt(1); got != timer2 {
		t.Fatalf("second loaded timer %p != s.disconnectTimer %p", got, timer2)
	}
	st, running, _ := snapshotDisconnectState(t, sess)
	if !running || st != stateDisconnectPending {
		t.Fatalf("after rearm publish: state=%d running=%v, want disconnectPending/running", int(st), running)
	}
	releasePhase2()
	// 第二代回调 V6 通过 → 再次 defer（重武装恰好第二次）：证明 V6 通过且旧回调
	// 未覆盖新 timer、未错误取消任务。
	pollUntil(t, "rearm count == 2", 10*time.Second, 20*time.Millisecond, func() bool {
		return rearmCount.Load() == 2
	})
	st2, running2, timer3 := snapshotDisconnectState(t, sess)
	if !running2 || st2 != stateDisconnectPending {
		t.Fatalf("after second callback defer: state=%d running=%v, want disconnectPending/running", int(st2), running2)
	}
	if timer3 == nil || timer3 == timer2 {
		t.Fatalf("s.disconnectTimer after second defer = %p, want a newer timer != %p", timer3, timer2)
	}
	// 清理：主动取消（停当前计时器），避免残留阻塞后续测试。
	sess.stopTask()
	pollTaskEnded(t, sess, 10*time.Second)
	st3, _, timer4 := snapshotDisconnectState(t, sess)
	if st3 != stateCancelled {
		t.Fatalf("state = %d, want cancelled after stopTask", int(st3))
	}
	if timer4 != nil {
		t.Fatal("disconnectTimer not nil after stopTask")
	}
}

// ===== S7 Batch 2：看门狗运行生命周期测试（v7.10 §11.5.7/§15）=====

// setTestWatchdogTickInterval 覆盖 watchdogTickInterval（生产 1s），测试结束时恢复。
// 注意：间隔在 NewTicker 创建时捕获，须在启动看门狗之前覆盖（T7-24/T7-29/T7-39/T7-40/T7-41）。
func setTestWatchdogTickInterval(t *testing.T, d time.Duration) {
	t.Helper()
	old := watchdogTickInterval
	watchdogTickInterval = d
	t.Cleanup(func() { watchdogTickInterval = old })
}

// snapshotWatchdog 一次性读真实快照的三个字段（锁外原子读）。
func snapshotWatchdog() (gen uint64, lastTickAt, suspendedTotal int64) {
	snap := watchdogSnapshot.Load()
	if snap == nil {
		return 0, 0, 0
	}
	return snap.generation, snap.lastTickAt, snap.suspendedTotal
}

// storeRealWatchdogSnapshot 直接向真实快照指针 Store（tick 可见）。
// 与 injectStaleWatchdogSnapshot 的区别：后者只设 testWatchdogSnapshotOverride，
// 只有回调的 loadWatchdogSnapshot 能看到；tick 读真实指针，看不到注入。
// 需要“看门狗 tick 观测到一次挂起”的测试（T7-39/T7-40）必须用本函数。
func storeRealWatchdogSnapshot(t *testing.T, snap *watchdogState) {
	t.Helper()
	watchdogSnapshot.Store(snap)
}

// stableGoroutineCount 等 goroutine 数稳定（连续两次读数相同，间隔 200ms），
// 返回稳定值；超时即失败。用于 -race 下探测 goroutine 退出等瞬态波动——
// 不是猜测时序，而是观测到静止后再取基线/断言：若有 goroutine 在窗口内
// 启动或退出，两次读数必不同，继续等；只有窗口内无变化才返回。
func stableGoroutineCount(t *testing.T, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	prev := runtime.NumGoroutine()
	for time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
		cur := runtime.NumGoroutine()
		if cur == prev {
			return cur
		}
		prev = cur
	}
	t.Fatalf("goroutine count did not stabilize within %v (last %d)", timeout, prev)
	return prev // 不可达
}

// TestT7_24_WatchdogLifecycle：看门狗生命周期与代次隔离（§11.5.7/§15）。
// 断言：恰好一个看门狗（NumGoroutine 增量恰好 +1 且稳定）；多次武装不增加
// goroutine；重复启动被拒绝（started=false，不启动第二个）；停止后 done 确
// 实关闭、goroutine 退出；旧实例完全退出后可以重新启动；resetBackendGeneration
// 重置快照（suspendedTotal=0）且不停止看门狗（之后仍 tick）；重复停止安全。
func TestT7_24_WatchdogLifecycle(t *testing.T) {
	r3Setup(t)
	setTestWatchdogTickInterval(t, 20*time.Millisecond)
	_, wsURL := r3Server(t)
	// 一个会话、一个运行任务（连接保持打开，直接武装不经过读循环关闭）。
	// 多次武装用顺序循环完成：单任务槽一次只允许一个任务运行（startTaskNamed
	// 拒绝第二个并发任务），因此 3 次武装是“启动→武装→取消→再启动”的循环，
	// 每次武装后断言 goroutine 数不变。
	c := r3Dial(t, wsURL)
	c.readInit(10 * time.Second)
	// 等三条探测消息到齐再取基线（同 T42 的做法）：探测 goroutine 在 -race 下
	// 完成较慢，若基线取在其退出之前，之后退出会抵消看门狗的 +1，使“恰好 +1”
	// 断言抖动。waitForAll 证明三条探测已完成（消息已发），再由
	// stableGoroutineCount 等尾部 goroutine 退出后取稳态基线。
	c.waitForAll(20*time.Second, "proxy_country_result", "speed_url_status", "version_info")
	sess := snapshotServerSession(t, c.connectionID)
	if sess == nil {
		t.Fatal("no server session")
	}
	startSyntheticTask(t, sess, longTaskRun)
	pollSessionRunning(t, sess, 10*time.Second)
	// 稳态基线：连接已建立、任务已运行、探测 goroutine 退出后再计数
	// （-race 下探测完成较慢，固定 100ms 不够；等计数稳定再取基线）。
	baseGoroutines := stableGoroutineCount(t, 5*time.Second)
	// 启动看门狗：必须成功；cleanup 兜底停止（幂等，见 stopWatchdog）。
	stop, done, started := startWatchdog()
	if !started {
		t.Fatal("startWatchdog returned started=false on first start")
	}
	t.Cleanup(func() { stopWatchdog(stop, done) })
	// 轮询 lastTickAt 前进（带超时）：证明 ticking。
	_, firstTick, _ := snapshotWatchdog()
	pollUntil(t, "watchdog lastTickAt advances", 5*time.Second, 20*time.Millisecond, func() bool {
		_, at, _ := snapshotWatchdog()
		return at > firstTick
	})
	// NumGoroutine 增量恰好 +1：等计数稳定在基线 +1（瞬时波动不算）。
	if got := stableGoroutineCount(t, 5*time.Second); got != baseGoroutines+1 {
		t.Fatalf("goroutine count = %d, want baseline+1 = %d", got, baseGoroutines+1)
	}
	// 重复启动被拒绝：不启动第二个，不增加 goroutine。
	stop2, done2, started2 := startWatchdog()
	if started2 || stop2 != nil || done2 != nil {
		t.Fatalf("second startWatchdog: started=%v stopNil=%v doneNil=%v, want false/true/true", started2, stop2 == nil, done2 == nil)
	}
	if got := stableGoroutineCount(t, 5*time.Second); got != baseGoroutines+1 {
		t.Fatalf("after duplicate start: goroutine count = %d, want %d", got, baseGoroutines+1)
	}
	// 多次武装（直接调生产 helper，连接保持打开）：不增加 goroutine。
	// 每轮：直接武装 → 断言计数不变 → 取消任务（停计时器）→ 再启动下一轮。
	for i := 0; i < 3; i++ {
		conn, _ := sess.wsSnapshot()
		if conn == nil {
			t.Fatalf("round %d: session ws is nil", i)
		}
		if !sess.armDisconnectTimer(conn) {
			t.Fatalf("round %d: armDisconnectTimer returned false", i)
		}
		if got := stableGoroutineCount(t, 5*time.Second); got != baseGoroutines+1 {
			t.Fatalf("round %d: goroutine count = %d, want %d (arm must not spawn a goroutine)", i, got, baseGoroutines+1)
		}
		if i < 2 {
			sess.cancelTask(false)
			pollTaskEnded(t, sess, 10*time.Second)
			startSyntheticTask(t, sess, longTaskRun)
			pollSessionRunning(t, sess, 10*time.Second)
		}
	}
	// resetBackendGeneration：快照重置（代次新、累计清零），看门狗继续 tick。
	// 注意：reset 同时取消孤儿任务并关闭旧连接（步骤 3a/3b），worker 与读循环
	// 退出会改变 goroutine 数——此处只断言快照字段与继续 tick，不再断言计数；
	// 计数基线在下面重新取。
	genBefore := atomic.LoadUint64(&backendGeneration)
	resetBackendGeneration()
	genAfter := atomic.LoadUint64(&backendGeneration)
	if genAfter != genBefore+1 {
		t.Fatalf("generation = %d, want %d", genAfter, genBefore+1)
	}
	pollUntil(t, "snapshot reset after generation bump", 5*time.Second, 20*time.Millisecond, func() bool {
		gen, _, suspended := snapshotWatchdog()
		return gen == genAfter && suspended == 0
	})
	// 重置后仍 tick：lastTickAt 继续前进。
	_, tickAfterReset, _ := snapshotWatchdog()
	pollUntil(t, "watchdog still ticking after reset", 5*time.Second, 20*time.Millisecond, func() bool {
		_, at, _ := snapshotWatchdog()
		return at > tickAfterReset
	})
	// 停止：done 关闭；重复停止安全（第二次立即返回）。
	stopWatchdog(stop, done)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("watchdog done not closed after stop")
	}
	stopWatchdog(stop, done) // 重复调用必须安全返回（done 已关闭）
	// 旧实例完全退出后 goroutine 数回落：重新取基线（reset 已取消任务、关闭连接；
	// 等计数稳定，worker 与读循环退出后再取）。
	baseAfterReset := stableGoroutineCount(t, 5*time.Second)
	// 重启：必须成功，且再次 tick；计数为新基线 +1。
	stop3, done3, started3 := startWatchdog()
	if !started3 {
		t.Fatal("startWatchdog returned started=false after full stop")
	}
	t.Cleanup(func() { stopWatchdog(stop3, done3) })
	_, tickBefore, _ := snapshotWatchdog()
	pollUntil(t, "restarted watchdog ticks", 5*time.Second, 20*time.Millisecond, func() bool {
		_, at, _ := snapshotWatchdog()
		return at > tickBefore
	})
	if got := stableGoroutineCount(t, 5*time.Second); got != baseAfterReset+1 {
		t.Fatalf("after restart: goroutine count = %d, want %d", got, baseAfterReset+1)
	}
	stopWatchdog(stop3, done3)
	if got := stableGoroutineCount(t, 5*time.Second); got != baseAfterReset {
		t.Fatalf("after restart stop: goroutine count = %d, want %d (watchdog goroutine leaked?)", got, baseAfterReset)
	}
	// 收尾：取消残留任务（reset 已取消孤儿；此处为防御性清理）。
	sess.cancelTask(false)
}

// TestT7_29_OldGenerationTickCannotOverwriteResetSnapshot：旧代次 tick 不能
// 覆盖重置后的新快照（§11.5.4，CAS + 代次标识；重点）。
// 步骤：启动看门狗（短间隔）→ 设 testWatchdogTickGate → 等 tick 到达门
// （已读旧快照、未提交）→ 执行 resetBackendGeneration（新快照、新代次、
// 累计清零）→ 释放门 → 旧 tick 的 CAS 失败后重读新快照提交（contribution≈0）
// → 断言新快照代次标识与累计值正确，且后续新代次 tick 正常更新。
// 反向验证：把 CAS 改回普通 Store 后重跑，测试须稳定失败（旧 tick 的 Store
// 必然覆盖新快照）。
func TestT7_29_OldGenerationTickCannotOverwriteResetSnapshot(t *testing.T) {
	r3Setup(t)
	setTestWatchdogTickInterval(t, 20*time.Millisecond)
	stop, done, started := startWatchdog()
	if !started {
		t.Fatal("startWatchdog returned started=false")
	}
	t.Cleanup(func() { stopWatchdog(stop, done) })
	// 等一个 tick 先完成（快照新鲜，lastTickAt 前进），确认看门狗在运行。
	_, firstTick, _ := snapshotWatchdog()
	pollUntil(t, "watchdog ticking before gate", 5*time.Second, 20*time.Millisecond, func() bool {
		_, at, _ := snapshotWatchdog()
		return at > firstTick
	})
	tickGate := newDisconnectTestGate()
	testHookSet(&testWatchdogTickGate, tickGate)
	var releaseOnce sync.Once
	releaseTick := func() { releaseOnce.Do(func() { close(tickGate.release) }) }
	t.Cleanup(func() {
		releaseTick()
		testHookSet(&testWatchdogTickGate, nil)
	})
	// 等 tick 到达门：已 Load 旧快照、未 CAS。
	select {
	case <-tickGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for testWatchdogTickGate (tick did not reach Load/CAS window)")
	}
	// 旧 tick 被门暂停期间执行 reset：新快照、新代次、累计清零。
	genBefore := atomic.LoadUint64(&backendGeneration)
	resetBackendGeneration()
	genAfter := atomic.LoadUint64(&backendGeneration)
	if genAfter != genBefore+1 {
		t.Fatalf("generation = %d, want %d", genAfter, genBefore+1)
	}
	// 释放旧 tick：其 CAS 必然失败（指针已换），重试后基于新快照提交。
	// 释放是异步的：静置 200ms 让被释放的迭代完成提交，再单次读断言。
	// （200ms 是 10 倍 tick 间隔的静置，不是竞态时序猜测——被释放的迭代
	// 已可运行，提交在毫秒级完成；且提交后的快照在两种情形下都稳定：
	// 正向重试后代次/累计不变，反向 Store 后快照被冻结、后续 tick 因代次
	// 不匹配而直接返回。因此单次读即有定论，不必轮询。）
	releaseTick()
	time.Sleep(200 * time.Millisecond)
	gen, _, suspended := snapshotWatchdog()
	if gen != genAfter || suspended != 0 {
		t.Fatalf("snapshot after old tick retry: generation=%d suspendedTotal=%d, want generation=%d suspendedTotal=0", gen, suspended, genAfter)
	}
	// 后续新代次 tick 正常更新：lastTickAt 继续前进。
	_, tickAfter, _ := snapshotWatchdog()
	pollUntil(t, "new-generation ticks advance", 5*time.Second, 20*time.Millisecond, func() bool {
		_, at, _ := snapshotWatchdog()
		return at > tickAfter
	})
	stopWatchdog(stop, done)
}

// TestT7_39_WatchdogTickVsTimerCallback：看门狗 tick 与 timer callback 竞态
// （§15，重点）。场景：回调阶段 1 用新鲜快照算出 cancel 提议；阶段 1 与
// 阶段 2 之间发生一次 300 秒挂起（测试向真实快照指针 Store 陈旧快照，
// tick 在暂停期间完成一次 CAS 更新，累计约 298 秒）；回调阶段 2 必须用新鲜
// 快照重算，不得采用阶段 1 的陈旧结论。
// 与设计文字顺序的核对：设计先写“注入陈旧快照”，但若陈旧快照在阶段 1 之前
// 就对回调可见，阶段 1 必为 defer（gap 300s > 5s 阈值），反向验证（去掉
// 阶段 2 重算）将无从失败——测试无法证明阶段 2 的存在。因此陈旧快照在阶段 1
// 完成之后、阶段 2 之前引入，这正是 §9B.2 要关闭的竞态窗口（阶段 1 与阶段 2
// 之间发生挂起）。其余场景要素（300s、tick 的 CAS 更新、defer + 重武装
// ≥10s、suspendedTotal 累计正确）与设计一致。
// 反向验证：去掉阶段 2 的新鲜快照重算（只用阶段 1 的结论）后重跑，测试须
// 稳定失败（回调误取消）。
func TestT7_39_WatchdogTickVsTimerCallback(t *testing.T) {
	r3Setup(t)
	setTestGracePeriod(t, 100*time.Millisecond)
	setTestWatchdogTickInterval(t, 10*time.Millisecond)
	stop, done, started := startWatchdog()
	if !started {
		t.Fatal("startWatchdog returned started=false")
	}
	t.Cleanup(func() { stopWatchdog(stop, done) })
	// 门：回调阶段门（阶段 1 后、阶段 2 前）+ tick 门（Load 与 CAS 之间）。
	// 先装门、再触发事件；cleanup 单函数内按序释放（先放门、再停看门狗）。
	phaseGate := newDisconnectTestGate()
	testHookSet(&testTimerCallbackPhaseGate, phaseGate)
	var releasePhaseOnce sync.Once
	releasePhase := func() { releasePhaseOnce.Do(func() { close(phaseGate.release) }) }
	tickGate := newDisconnectTestGate()
	testHookSet(&testWatchdogTickGate, tickGate)
	var releaseTickOnce sync.Once
	releaseTick := func() { releaseTickOnce.Do(func() { close(tickGate.release) }) }
	t.Cleanup(func() {
		releaseTick()
		releasePhase()
		testHookSet(&testTimerCallbackPhaseGate, nil)
		testHookSet(&testWatchdogTickGate, nil)
	})
	// 重武装记录：次数 + 时长（testTimerRearmHook 在 defer 成功重武装后调用）。
	var rearmMu sync.Mutex
	var rearms []time.Duration
	testHookSet(&testTimerRearmHook, func(rearm time.Duration) {
		rearmMu.Lock()
		rearms = append(rearms, rearm)
		rearmMu.Unlock()
	})
	t.Cleanup(func() { testHookSet(&testTimerRearmHook, nil) })
	_, wsURL := r3Server(t)
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	startSyntheticTask(t, sess, longTaskRun)
	pollSessionRunning(t, sess, 10*time.Second)
	cA.Close()
	pollDisconnectState(t, sess, stateDisconnectPending, 10*time.Second)
	// 阶段 1：回调到期，用新鲜快照算出 cancel 提议（窗口 100ms 已过、
	// liveElapsed ≥ 窗口），随后停在阶段门。
	select {
	case <-phaseGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for callback phase gate")
	}
	// tick 门：等一次 tick 到达门（已 Load 新鲜快照、未提交）。
	select {
	case <-tickGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for watchdog tick gate")
	}
	// 阶段 1 与阶段 2 之间引入 300 秒挂起：向真实快照指针 Store 陈旧快照。
	// （tick 读真实指针，不读回调的 override 注入——见 watchdogTickOnce 注释。）
	gen := atomic.LoadUint64(&backendGeneration)
	storeRealWatchdogSnapshot(t, &watchdogState{
		generation:     gen,
		lastTickAt:     time.Now().UnixNano() - int64(300*time.Second),
		suspendedTotal: 0,
	})
	// 先放 tick：旧迭代的 CAS 失败（指针已换）→ 重读陈旧快照 → 累计约 298 秒
	// 提交。轮询确认累计完成，再放回调。
	releaseTick()
	pollUntil(t, "tick accumulates 300s suspend", 10*time.Second, 20*time.Millisecond, func() bool {
		_, _, suspended := snapshotWatchdog()
		return suspended >= int64(290*time.Second)
	})
	releasePhase()
	// 阶段 2 用新鲜快照重算：defer + 重武装恰好一次，时长 ≥10 秒；任务未取消。
	pollUntil(t, "rearm count == 1", 10*time.Second, 20*time.Millisecond, func() bool {
		rearmMu.Lock()
		defer rearmMu.Unlock()
		return len(rearms) == 1
	})
	rearmMu.Lock()
	rearm := rearms[0]
	rearmMu.Unlock()
	if rearm < 10*time.Second {
		t.Fatalf("rearm = %v, want >= 10s (fresh snapshot must defer, not cancel)", rearm)
	}
	st, running, _ := snapshotDisconnectState(t, sess)
	if !running || st != stateDisconnectPending {
		t.Fatalf("after fresh-snapshot re-evaluation: state=%d running=%v, want disconnectPending/running", int(st), running)
	}
	// 快照的 suspendedTotal 累计正确（回调只 Load，不 Store——回调不得覆盖它）。
	_, _, suspended := snapshotWatchdog()
	if suspended < int64(290*time.Second) || suspended > int64(300*time.Second) {
		t.Fatalf("suspendedTotal = %d ns, want within [290s, 300s]", suspended)
	}
	// 收尾：取消任务（停重武装计时器，约 298s，不得留到后续测试）。
	sess.cancelTask(false)
	pollTaskEnded(t, sess, 10*time.Second)
	stopWatchdog(stop, done)
}

// TestT7_40_SuspendResumeDefersAndRearms：iOS 挂起与恢复的端到端
// （§15，重点；Batch 2 只实现 watchdog defer 与 timer 重武装部分）。
// 场景：任务运行中注入挂起（向真实快照指针 Store 陈旧快照；看门狗 tick 被
// 测试门暂停，模拟挂起期间无 tick）；计时器到期 → defer + 重武装（下限
// 10 秒最小重连机会）；恢复 tick（释放门，tick 累计挂起）→ 任务仍不取消。
// 数值设计：窗口 2s，陈旧度 6s。按公式 liveElapsed = 2 − 0 − (6+2−2) ≈
// −4s → rearm = max(2−(−4), 10s) = 10s（下限绑定）；去掉下限时重武装约
// 7s，约 7 秒后重武装到期重算即取消——反向验证据此失败。
// 与设计文字（300s）的偏差说明：300s 陈旧 + 真实武装（disconnectAt 新鲜）
// 时 liveElapsed ≈ −296s，重武装 ≈ 298s——无论有无下限都远大于 10s，
// 下限永不绑定，反向验证无从失败。6s 与 300s 走同一 defer 分支
// （gap > 5s 阈值）与同一重武装机制（T7-23 以 4s 代 300s 的先例）；300s
// 量级的累计已由 T7-39 覆盖（重武装约 298s、suspendedTotal 约 298s）。
// 认领部分不属 Batch 2：设计要求的“重武装窗口内认领成功”与“认领后计时
// 器被停”依赖尚未实现的认领 handler，本测试到“恢复后任务仍存活、重武装
// 已发生”为止，不提前实施 handler。
// 反向验证：去掉 10 秒最小重连机会后重跑，测试须稳定失败（任务在恢复后
// 约 7 秒即被取消；本测试在 8 秒处断言存活）。
func TestT7_40_SuspendResumeDefersAndRearms(t *testing.T) {
	r3Setup(t)
	setTestGracePeriod(t, 2*time.Second)
	setTestWatchdogTickInterval(t, 10*time.Millisecond)
	stop, done, started := startWatchdog()
	if !started {
		t.Fatal("startWatchdog returned started=false")
	}
	t.Cleanup(func() { stopWatchdog(stop, done) })
	tickGate := newDisconnectTestGate()
	testHookSet(&testWatchdogTickGate, tickGate)
	var releaseTickOnce sync.Once
	releaseTick := func() { releaseTickOnce.Do(func() { close(tickGate.release) }) }
	t.Cleanup(func() {
		releaseTick()
		testHookSet(&testWatchdogTickGate, nil)
	})
	var rearmMu sync.Mutex
	var rearms []time.Duration
	testHookSet(&testTimerRearmHook, func(rearm time.Duration) {
		rearmMu.Lock()
		rearms = append(rearms, rearm)
		rearmMu.Unlock()
	})
	t.Cleanup(func() { testHookSet(&testTimerRearmHook, nil) })
	_, wsURL := r3Server(t)
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register("test-t7-40-page")
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	startSyntheticTask(t, sess, longTaskRun)
	pollSessionRunning(t, sess, 10*time.Second)
	// 暂停 tick（模拟挂起期间无 tick）：等一次 tick 到达门（已 Load 新鲜快照）。
	select {
	case <-tickGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for watchdog tick gate")
	}
	// 注入 6 秒挂起：向真实快照指针 Store 陈旧快照（tick 被门暂停，看不到它）。
	gen := atomic.LoadUint64(&backendGeneration)
	storeRealWatchdogSnapshot(t, &watchdogState{
		generation:     gen,
		lastTickAt:     time.Now().UnixNano() - int64(6*time.Second),
		suspendedTotal: 0,
	})
	cA.Close()
	pollDisconnectState(t, sess, stateDisconnectPending, 10*time.Second)
	// 计时器到期 → defer + 重武装恰好一次，时长恰好 10s（下限绑定）；任务未取消。
	pollUntil(t, "rearm count == 1", 10*time.Second, 20*time.Millisecond, func() bool {
		rearmMu.Lock()
		defer rearmMu.Unlock()
		return len(rearms) == 1
	})
	rearmMu.Lock()
	rearm := rearms[0]
	rearmMu.Unlock()
	if rearm != 10*time.Second {
		t.Fatalf("rearm = %v, want exactly 10s (minPostResumeOpportunity floor must bind)", rearm)
	}
	st, running, _ := snapshotDisconnectState(t, sess)
	if !running || st != stateDisconnectPending {
		t.Fatalf("after suspend expiry: state=%d running=%v, want disconnectPending/running", int(st), running)
	}
	// 恢复看门狗 tick：释放门；被暂停的迭代重读陈旧快照，累计挂起后提交。
	releaseTick()
	pollUntil(t, "tick accumulates suspend after resume", 10*time.Second, 20*time.Millisecond, func() bool {
		_, _, suspended := snapshotWatchdog()
		return suspended >= int64(3*time.Second)
	})
	_, _, suspended := snapshotWatchdog()
	if suspended < int64(3*time.Second) || suspended > int64(9*time.Second) {
		t.Fatalf("suspendedTotal after resume = %v, want within [3s, 9s]", time.Duration(suspended))
	}
	// 恢复后任务仍存活：等过无下限时的重武装时刻（约 7s）再断言，证明 10s 下限
	// 真正给了重连机会（无下限时任务在此刻已被取消——见反向验证）。
	// 8s 介于无下限取消点（约 7s）与有下限重武装到期点（10s）之间。
	time.Sleep(8 * time.Second)
	st2, running2, _ := snapshotDisconnectState(t, sess)
	if !running2 || st2 != stateDisconnectPending {
		t.Fatalf("8s after resume: state=%d running=%v, want disconnectPending/running", int(st2), running2)
	}
	// 补全（C1）：重武装窗口内（10s 下限未到）认领成功，任务恢复运行，
	// 重武装计时器已停；任务在重武装到期点之后仍存活。
	cB, _ := dialClaimer(t, wsURL, "test-t7-40-page")
	taskID := sess.taskIDSnapshot()
	epoch := currentBackendEpoch()
	cB.claim(taskID, epoch, "test-t7-40-page")
	cB.waitResumeAccepted(10*time.Second, taskID, epoch)
	st3, running3, timer3 := snapshotDisconnectState(t, sess)
	if !running3 || st3 != stateRunning || timer3 != nil {
		t.Fatalf("after claim in rearm window: state=%d running=%v timerSet=%v, want running/true/false",
			int(st3), running3, timer3 != nil)
	}
	// 任务在重武装到期点（约 arm+12s）之后仍存活：再等 3s（已过到期点）。
	time.Sleep(3 * time.Second)
	st4, running4, _ := snapshotDisconnectState(t, sess)
	if !running4 || st4 != stateRunning {
		t.Fatalf("after rearm expiry: state=%d running=%v, want running/true", int(st4), running4)
	}
	// 收尾：B 的 stop_task 取消已认领任务（停计时器，不得留到后续测试）。
	closeClaimedTask(t, sess, cB)
	stopWatchdog(stop, done)
}

// TestT7_41_OldGenerationPollution：旧 generation 污染——reset 与旧任务
// tick/计时器并发隔离（§15，重点）。任务 A 在 disconnectPending（计时器已
// 武装）；执行 resetBackendGeneration（新代次、新 epoch）；旧计时器回调与
// 旧看门狗 tick 并发执行（通道屏障同步）；随后启动新任务 B（新代次）。
// 断言：旧计时器回调的身份复核失败，不取消新任务 B；旧 tick 的 CAS 失败后
// 重读新快照，不覆盖新快照（suspendedTotal=0，generation=新代次）；新任
// 务 B 正常运行，其计时器字段不受旧回调影响。
// 说明：reset 的 collectOrphan 同时把 A 的状态同步置为 cancelled（§9.1 清理
// 点 3），因此旧回调在阶段 2 首先因 V1（状态非待定）失败退出；V4/V5
// （epoch/generation）是同一复核的纵深防线（若状态不知何故仍为待定，它们
// 同样失败）。测试断言结果（不取消 B、B 不受影响、快照不被覆盖），不绑定
// 首先失败的是哪一项 V 检查。
// 反向验证：去掉快照的 generation 字段（tick 用普通 Store）后重跑，测试须
// 稳定失败（旧 tick 覆盖新快照）。
func TestT7_41_OldGenerationPollution(t *testing.T) {
	r3Setup(t)
	setTestGracePeriod(t, 200*time.Millisecond)
	setTestWatchdogTickInterval(t, 10*time.Millisecond)
	stop, done, started := startWatchdog()
	if !started {
		t.Fatal("startWatchdog returned started=false")
	}
	t.Cleanup(func() { stopWatchdog(stop, done) })
	// 门：回调阶段门 + tick 门；先装门、再触发事件。
	phaseGate := newDisconnectTestGate()
	testHookSet(&testTimerCallbackPhaseGate, phaseGate)
	var releasePhaseOnce sync.Once
	releasePhase := func() { releasePhaseOnce.Do(func() { close(phaseGate.release) }) }
	tickGate := newDisconnectTestGate()
	testHookSet(&testWatchdogTickGate, tickGate)
	var releaseTickOnce sync.Once
	releaseTick := func() { releaseTickOnce.Do(func() { close(tickGate.release) }) }
	t.Cleanup(func() {
		releaseTick()
		releasePhase()
		testHookSet(&testTimerCallbackPhaseGate, nil)
		testHookSet(&testWatchdogTickGate, nil)
	})
	_, wsURL := r3Server(t)
	// 任务 A：运行 → 断线武装（200ms 窗口）；回调到期后停在阶段门（阶段 1 已算出 cancel 提议）。
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	sessA := snapshotServerSession(t, cA.connectionID)
	if sessA == nil {
		t.Fatal("no server session for A")
	}
	startSyntheticTask(t, sessA, longTaskRun)
	pollSessionRunning(t, sessA, 10*time.Second)
	cA.Close()
	pollDisconnectState(t, sessA, stateDisconnectPending, 10*time.Second)
	select {
	case <-phaseGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for callback phase gate (A)")
	}
	// 旧 tick 到达门（已 Load 重置前快照、未提交）。
	select {
	case <-tickGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for watchdog tick gate")
	}
	// reset：新代次、新 epoch、新快照（累计清零）；A 的状态同步置 cancelled、
	// 计时器停（回调已在飞行中，停表不阻止它继续——正确性不依赖 Stop）。
	genBefore := atomic.LoadUint64(&backendGeneration)
	resetBackendGeneration()
	genAfter := atomic.LoadUint64(&backendGeneration)
	if genAfter != genBefore+1 {
		t.Fatalf("generation = %d, want %d", genAfter, genBefore+1)
	}
	// 并发释放两者：旧 tick 的 CAS 失败 → 重读新快照；旧回调的阶段 2 复核失败。
	// 释放是异步的：静置 200ms 让被释放的迭代完成提交，再单次读断言
	// （静置理由同 T7-29；提交后的快照在两种情形下都稳定）。
	releaseTick()
	releasePhase()
	time.Sleep(200 * time.Millisecond)
	gen, _, suspended := snapshotWatchdog()
	if gen != genAfter || suspended != 0 {
		t.Fatalf("snapshot after old tick retry: generation=%d suspendedTotal=%d, want generation=%d suspendedTotal=0", gen, suspended, genAfter)
	}
	// A 已取消（reset 的孤儿清理），计时器字段 nil。
	pollTaskEnded(t, sessA, 10*time.Second)
	stA, _, timerA := snapshotDisconnectState(t, sessA)
	if stA != stateCancelled {
		t.Fatalf("task A state = %d, want cancelled after reset", int(stA))
	}
	if timerA != nil {
		t.Fatal("task A disconnectTimer not nil after reset")
	}
	// 新任务 B（新代次）：正常运行，计时器字段不受旧回调影响。
	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	sessB := snapshotServerSession(t, cB.connectionID)
	if sessB == nil {
		t.Fatal("no server session for B")
	}
	startSyntheticTask(t, sessB, longTaskRun)
	pollSessionRunning(t, sessB, 10*time.Second)
	stB, _, timerB := snapshotDisconnectState(t, sessB)
	if stB != stateRunning {
		t.Fatalf("task B state = %d, want running", int(stB))
	}
	if timerB != nil {
		t.Fatal("task B disconnectTimer not nil (old callback must not touch new task)")
	}
	// 快照仍为新代次、累计仍为 0（旧 tick 未污染）。
	gen, _, suspended = snapshotWatchdog()
	if gen != genAfter || suspended != 0 {
		t.Fatalf("snapshot after B start: generation=%d suspendedTotal=%d, want generation=%d suspendedTotal=0", gen, suspended, genAfter)
	}
	// 收尾：取消 B；停看门狗。
	sessB.cancelTask(false)
	pollTaskEnded(t, sessB, 10*time.Second)
	stopWatchdog(stop, done)
}

// TestT7_B2_R1_CrossInstanceStopRace：跨实例停止竞态回归测试（Batch 2 独立
// 审查发现；定点修正）。
// 旧实现用全局 watchdogStopClosed 保护关闭、新实例启动时重置它：旧实例的
// 延迟停止调用（已通过 done 检查、尚未 CAS）在新实例启动后恢复，CAS 再次
// 成功，再次关闭已关闭的旧 stop channel → panic。
// 修正后关闭保护绑定实例（实例持有的 sync.Once，经 stop 通道查注册表）：
// 旧实例的延迟停止调用永远解析到旧实例的 Once（已执行过，本次为空操作），
// 新实例的启动与旧实例的停止保护互不影响。
// 步骤（全程确定性同步，无 Sleep 猜测调度；超时仅作防永久阻塞保护）：
// 1. 启动实例 A；2. 启动 A 的停止调用 A1，门将其精确暂停在 done 检查之后、
// 关闭步骤之前；3. 清除门钩子（A1 已持有门指针，不受影响），A2 同步停止 A
// （关闭 stopA、等 doneA）；4. 确认 A 已退出；5. 启动实例 B，确认运行中
// （stopB 未关闭、tick 前进）；6. 释放 A1；7. 确认 A1 安全返回、B 未被触碰
// （stopB/doneB 未关闭、仍在 tick）；8. 正常停止 B。
// 反向验证：把 start/stop 改回旧实现（全局标志 + 启动重置，门位置不变）
// 后重跑，A1 释放后必定 panic（close of closed channel），测试进程崩溃。
func TestT7_B2_R1_CrossInstanceStopRace(t *testing.T) {
	r3Setup(t)
	setTestWatchdogTickInterval(t, 20*time.Millisecond)

	// 实例 A。
	stopA, doneA, started := startWatchdog()
	if !started {
		t.Fatal("startWatchdog for A returned started=false")
	}
	var stopB, doneB chan struct{}
	stopGate := newDisconnectTestGate()
	testHookSet(&testStopWatchdogGate, stopGate)
	var releaseStopOnce sync.Once
	releaseStop := func() { releaseStopOnce.Do(func() { close(stopGate.release) }) }
	// 单一 cleanup，按正确顺序收尾：先放门（否则门内阻塞的停止调用永不
	// 返回），再停 B、停 A。成功路径上各步均为空操作（done 已关闭即返回）。
	t.Cleanup(func() {
		releaseStop()
		testHookSet(&testStopWatchdogGate, nil)
		if stopB != nil {
			stopWatchdog(stopB, doneB)
		}
		stopWatchdog(stopA, doneA)
	})

	// A1：A 的延迟停止调用。门将其暂停在 done 检查之后、关闭步骤之前。
	a1Done := make(chan struct{})
	go func() {
		defer close(a1Done)
		stopWatchdog(stopA, doneA)
	}()
	select {
	case <-stopGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for testStopWatchdogGate (A1 did not reach stop window)")
	}
	// 清除门钩子：A1 已持有门指针、在 pass() 内阻塞，不受清除影响；后续
	// 的停止调用（A2、B 的停止）不再经过门。
	testHookSet(&testStopWatchdogGate, nil)

	// A2：A 的第二个停止调用，同步执行——必须关闭 stopA 并等 A 完全退出。
	stopWatchdog(stopA, doneA)
	select {
	case <-doneA:
	default:
		t.Fatal("doneA not closed after A2 stopped instance A")
	}

	// 实例 B：启动成功，且正在运行（stop 未关闭、tick 前进）。
	stopB, doneB, started = startWatchdog()
	if !started {
		t.Fatal("startWatchdog for B returned started=false")
	}
	select {
	case <-stopB:
		t.Fatal("B stop channel closed before any stop call")
	default:
	}
	_, bTickBefore, _ := snapshotWatchdog()
	pollUntil(t, "instance B ticking", 5*time.Second, 20*time.Millisecond, func() bool {
		_, at, _ := snapshotWatchdog()
		return at > bTickBefore
	})

	// 释放 A1：旧实例的延迟停止调用现在执行关闭步骤。
	releaseStop()
	select {
	case <-a1Done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for delayed stopper A1 to return")
	}

	// B 未被 A1 触碰：stopB/doneB 未关闭，且仍在 tick。
	select {
	case <-stopB:
		t.Fatal("B stop channel was closed by old instance stopper A1")
	default:
	}
	select {
	case <-doneB:
		t.Fatal("B done closed before B was stopped")
	default:
	}
	_, bTickAfter, _ := snapshotWatchdog()
	pollUntil(t, "instance B still ticking after A1 released", 5*time.Second, 20*time.Millisecond, func() bool {
		_, at, _ := snapshotWatchdog()
		return at > bTickAfter
	})

	// 正常停止 B：生命周期完整结束。
	stopWatchdog(stopB, doneB)
	select {
	case <-doneB:
	default:
		t.Fatal("doneB not closed after stopping B")
	}
}

// ===== S7 C1 认领测试 helpers =====

// claimFixture 是认领测试的通用 A 侧装置：A 连接 + 运行中的合成前台任务。
type claimFixture struct {
	wsURL  string
	cA     *r3Client
	sess   *appSession
	taskID string
	epoch  string
	pageID string
}

// setupClaimTask 建立认领测试基线：A 注册 pageId 并启动一个合成长任务。
// grace 为测试用宽限（disconnectGracePeriod 的覆盖值）。
func setupClaimTask(t *testing.T, grace time.Duration) *claimFixture {
	t.Helper()
	r3Setup(t)
	setTestGracePeriod(t, grace)
	_, wsURL := r3Server(t)
	pageID := "test-claim-page"
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageID)
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	startSyntheticTask(t, sess, longTaskRun)
	pollSessionRunning(t, sess, 10*time.Second)
	return &claimFixture{
		wsURL:  wsURL,
		cA:     cA,
		sess:   sess,
		taskID: sess.taskIDSnapshot(),
		epoch:  currentBackendEpoch(),
		pageID: pageID,
	}
}

// armByDisconnect 关闭 A 的客户端连接，等待服务端进入 disconnectPending。
func (f *claimFixture) armByDisconnect(t *testing.T) {
	t.Helper()
	f.cA.Close()
	pollDisconnectState(t, f.sess, stateDisconnectPending, 10*time.Second)
}

// dialClaimer 以同一 pageId 建立认领者连接 B，返回客户端与服务端 session。
func dialClaimer(t *testing.T, wsURL, pageID string) (*r3Client, *appSession) {
	t.Helper()
	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register(pageID)
	sess := snapshotServerSession(t, cB.connectionID)
	if sess == nil {
		t.Fatal("no server session for claimer")
	}
	return cB, sess
}

// claim 发送 resume_foreground_task。
func (c *r3Client) claim(taskID, epoch, pageID string) {
	c.t.Helper()
	c.send("resume_foreground_task", map[string]interface{}{"taskId": taskID, "backendEpoch": epoch, "pageId": pageID})
}

// waitResumeAccepted 等待 resume_accepted 并核验 taskId/epoch。
func (c *r3Client) waitResumeAccepted(timeout time.Duration, taskID, epoch string) {
	c.t.Helper()
	msg := c.waitFor("resume_accepted", timeout)
	data, _ := msg["data"].(map[string]interface{})
	if got, _ := data["taskId"].(string); got != taskID {
		c.t.Fatalf("resume_accepted taskId = %q, want %q", got, taskID)
	}
	if got, _ := data["backendEpoch"].(string); got != epoch {
		c.t.Fatalf("resume_accepted backendEpoch = %q, want %q", got, epoch)
	}
}

// waitResumeRejected 等待 resume_rejected 并核验 reason。
func (c *r3Client) waitResumeRejected(timeout time.Duration, wantReason string) {
	c.t.Helper()
	msg := c.waitFor("resume_rejected", timeout)
	data, _ := msg["data"].(map[string]interface{})
	if got, _ := data["reason"].(string); got != wantReason {
		c.t.Fatalf("resume_rejected reason = %q, want %q", got, wantReason)
	}
}

// snapshotTaskPageId 在 taskMutex 内读 taskPageId。
func snapshotTaskPageId(t *testing.T, sess *appSession) string {
	t.Helper()
	sess.taskMutex.Lock()
	defer sess.taskMutex.Unlock()
	return sess.taskPageId
}

// TestT7_01_ClaimWithinWindow：真实认领 handler 路径（§15，重点）。
// 目的：端到端验证认领协议——断线武装后，新连接认领成功，任务恢复运行。
// 初始状态：A 上运行中的合成前台任务（taskPageId == 注册 pageId）。
// 并发控制：真实断开（客户端 Close → 读循环错误分支武装）；认领经真实 WS。
// 预期：B 收到 resume_accepted；状态 running；计时器已停；连接已换绑到 B；
// 随后 B 的 stop_task 能取消本任务（证明局部 session 重定向生效）。
// 能捕获的错误：认领 handler 未注册/解析失败、状态未转换、计时器未停、
// 换绑失败（旧连接残留）、重定向失效（stop_task 取消不到任务）。
func TestT7_01_ClaimWithinWindow(t *testing.T) {
	f := setupClaimTask(t, 30*time.Second)
	if got := snapshotTaskPageId(t, f.sess); got != f.pageID {
		t.Fatalf("taskPageId = %q, want %q", got, f.pageID)
	}
	f.armByDisconnect(t)
	cB, sessB := dialClaimer(t, f.wsURL, f.pageID)
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	st, running, timer := snapshotDisconnectState(t, f.sess)
	if !running || st != stateRunning {
		t.Fatalf("after claim: state=%d running=%v, want running/true", int(st), running)
	}
	if timer != nil {
		t.Fatal("disconnectTimer != nil after claim, want nil (timer stopped)")
	}
	// 换绑验证：认领后任务 session 的 ws 必须是 B 的服务端 socket（与 B 的
	// 新 session 的 ws 同一指针；客户端的 *websocket.Conn 与服务端的不是
	// 同一对象，不得跨端比较指针）。
	conn, closed := f.sess.wsSnapshot()
	if closed || conn == nil {
		t.Fatal("claimed session ws is closed/nil after claim")
	}
	connB, _ := sessB.wsSnapshot()
	if conn != connB {
		t.Fatal("claimed session ws != claimer server-side socket (rebind failed)")
	}
	// 重定向验证：B 发 stop_task 应取消本任务（局部 session 已指向认领后的 session）。
	closeClaimedTask(t, f.sess, cB)
}

// TestT7_30_TaskPageIdAtomicCommit：taskPageId 与双锁原子写入（§15，重点）。
// 目的：证明 beginTask 在同一 backgroundTaskMutex 持有内读 pageId、取
// taskMutex、写身份字段——register_page 写 pageId 与本提交互斥，不存在
// “读到旧值、提交时已是新值”的第三种情况；且 taskPageId 不随连接重注册而变。
// 初始状态：A 已注册 pageId P1，未启动任务。
// 并发控制：testBeginTaskCommitGate 暂停 beginTask（四把锁全持有、身份未写）；
// 门内发 register_page（新 pageId P2），断言 ack 在门释放前不到达（互斥证明）。
// 预期：门内重注册被阻塞；提交后 taskPageId == P1；重注册完成后 loopEntry.pageId
// == P2 但 taskPageId 仍是 P1。
// 能捕获的错误：beginTask 未取 backgroundTaskMutex（重注册立即完成）、
// taskPageId 写在锁外（读到 P2）、taskPageId 随重注册被改写。
// 反向验证：去掉 beginTask 的 backgroundTaskMutex 取锁后重跑，门内 ack 断言须
// 稳定失败（重注册不再阻塞——证明该锁是互斥的 operative 机制）。
func TestT7_30_TaskPageIdAtomicCommit(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageID := "test-t7-30-page"
	pageID2 := "test-t7-30-page-2"
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageID)
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	gate := newDisconnectTestGate()
	testHookSet(&testBeginTaskCommitGate, gate)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate.release) }) }
	t.Cleanup(func() {
		testHookSet(&testBeginTaskCommitGate, nil)
		release()
	})
	// 另一 goroutine 启动任务（beginTask 将停在门内）。
	go startSyntheticTask(t, sess, longTaskRun)
	select {
	case <-gate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for testBeginTaskCommitGate")
	}
	// 门内重注册：register_page 取 backgroundTaskMutex——beginTask 正持有，
	// 故 ack 在 300ms 内不得到达；若 beginTask 未持该锁，ack 会立即到达。
	cA.send("register_page", map[string]interface{}{"pageId": pageID2})
	if _, ok := cA.waitForTimeout("register_page_ack", 300*time.Millisecond); ok {
		t.Fatal("register_page ack arrived while beginTask held backgroundTaskMutex (locks not mutually exclusive)")
	}
	// 释放门 → beginTask 提交。
	release()
	pollSessionRunning(t, sess, 10*time.Second)
	if got := snapshotTaskPageId(t, sess); got != pageID {
		t.Fatalf("taskPageId = %q, want %q (pageId read inside the atomic commit)", got, pageID)
	}
	// 重注册随后完成：ack 到达，loopEntry.pageId 变为 P2；taskPageId 仍是 P1。
	cA.waitFor("register_page_ack", 10*time.Second)
	if le, ok := snapshotLoopState(t, cA.connectionID); !ok || le.pageId != pageID2 {
		t.Fatalf("loopEntry.pageId = %q, want %q", le.pageId, pageID2)
	}
	if got := snapshotTaskPageId(t, sess); got != pageID {
		t.Fatalf("taskPageId changed after re-register: %q, want %q", got, pageID)
	}
	sess.stopTask()
	pollTaskEnded(t, sess, 10*time.Second)
}

// TestT7_47_IdempotentReclaim：任务取消、完成后的停止行为 + 同连接重复认领幂等
// （§15；C-1 授权；决策 5/7）。
// 子例 (a)：任务在 disconnectPending 期间被主动取消 → 旧页重试认领 →
// already-cancelled，重试立即停止。取消经 session.stopTask 直接调用——
// 网络 stop_task 到不了 disconnectPending 任务（其读循环已退出，新连接的
// session 无任务），直接调用与 handler 调的是同一 cancelTask 路径。
// 子例 (b)：任务在窗口内自然完成 → 旧页重试认领 → task-not-found；随后启动
// 新任务 → 新任务为全新 taskId，旧任务日志不被复用。
// 子例 (c)：同连接重复认领幂等（C-1）：任务仍在运行、连接仍是 A，A 自己再认领
// → resume_accepted；不重复创建任务、不重复绑定、不改变状态。
// 能捕获的错误：终态后仍可认领（窗口重开）、幂等认领返回 not-pending、
// 幂等认领改变状态/计时器/连接、新任务复用旧 taskId/日志。
func TestT7_47_IdempotentReclaim(t *testing.T) {
	// 子例 (c)：同连接重复认领幂等。
	f := setupClaimTask(t, 30*time.Second)
	wsBefore, _ := f.sess.wsSnapshot()
	f.cA.claim(f.taskID, f.epoch, f.pageID)
	f.cA.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	st, running, timer := snapshotDisconnectState(t, f.sess)
	if !running || st != stateRunning || timer != nil {
		t.Fatalf("after idempotent reclaim: state=%d running=%v timerSet=%v, want running/true/false", int(st), running, timer != nil)
	}
	// 幂等成功不得改变连接：session 的 ws 仍是认领前的服务端 socket。
	// （客户端的 *websocket.Conn 与服务端的不是同一对象，不得跨端比较指针。）
	conn, _ := f.sess.wsSnapshot()
	if conn != wsBefore {
		t.Fatal("session ws changed after idempotent reclaim")
	}
	// 子例 (a)：disconnectPending 期间主动取消 → 重试认领 → already-cancelled。
	f2 := setupClaimTask(t, 30*time.Second)
	f2.armByDisconnect(t)
	f2.sess.stopTask()
	pollTaskEnded(t, f2.sess, 10*time.Second)
	cB, _ := dialClaimer(t, f2.wsURL, f2.pageID)
	cB.claim(f2.taskID, f2.epoch, f2.pageID)
	cB.waitResumeRejected(10*time.Second, "already-cancelled")
	// 子例 (b)：窗口内自然完成 → 重试认领 → task-not-found；新任务全新 taskId。
	r3Setup(t)
	setTestGracePeriod(t, 30*time.Second)
	_, wsURL := r3Server(t)
	pageID := "test-claim-page"
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageID)
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	done := make(chan struct{})
	run := func(ctx context.Context, s *appSession) {
		select {
		case <-done:
		case <-ctx.Done():
		}
	}
	startSyntheticTask(t, sess, run)
	pollSessionRunning(t, sess, 10*time.Second)
	oldTaskID := sess.taskIDSnapshot()
	epoch := currentBackendEpoch()
	cA.Close()
	pollDisconnectState(t, sess, stateDisconnectPending, 10*time.Second)
	close(done)
	pollTaskEnded(t, sess, 10*time.Second)
	st2, _, _ := snapshotDisconnectState(t, sess)
	if st2 != stateCompleted {
		t.Fatalf("after natural complete in window: state=%d, want completed", int(st2))
	}
	cB2, sessB := dialClaimer(t, wsURL, pageID)
	cB2.claim(oldTaskID, epoch, pageID)
	cB2.waitResumeRejected(10*time.Second, "task-not-found")
	startSyntheticTask(t, sessB, longTaskRun)
	pollSessionRunning(t, sessB, 10*time.Second)
	if newTaskID := sessB.taskIDSnapshot(); newTaskID == oldTaskID {
		t.Fatalf("new task reused old taskId %q", oldTaskID)
	}
	if log := sessB.resultLog.Load(); log == nil || log.taskID != sessB.taskIDSnapshot() {
		t.Fatal("new task did not get a fresh result log (old log reused)")
	}
	sessB.stopTask()
	pollTaskEnded(t, sessB, 10*time.Second)
}

// TestTaskStarted_IdentityAndOrder：task_started 身份正确性及发送顺序（O-1）。
// 目的：后端在前台任务成功创建后、worker 启动之前、第一条结果消息之前发送
// task_started {taskId, backendEpoch}，身份来自实际创建的任务。
// 初始状态：A 已注册，未启动任务；任务的 worker 会发一行 test_result。
// 并发控制：无并发——顺序由 startTaskNamed 的程序顺序保证（先发 task_started，
// 后 safeGo 启动 worker）；测试在客户端侧按到达顺序断言。
// 预期：客户端收到的 task_started 与 test_result 的第一个必须是 task_started，
// 且 taskId/epoch 与服务端实际任务一致；随后才收到第一条结果行。
// 能捕获的错误：task_started 未发送、身份与实际任务不符（重新生成/错任务）、
// 结果行先于 task_started 到达。
func TestTaskStarted_IdentityAndOrder(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register("test-task-started-page")
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	run := func(ctx context.Context, s *appSession) {
		s.sendTaskResult(ctx, "test_result", map[string]interface{}{"ip": "1.2.3.4"})
		<-ctx.Done()
	}
	startSyntheticTask(t, sess, run)
	pollSessionRunning(t, sess, 10*time.Second)
	taskID := sess.taskIDSnapshot()
	epoch := currentBackendEpoch()
	// 顺序断言：直接读消息流，task_started 与 test_result 的第一个必须是
	// task_started（waitFor 会跳过不匹配类型，无法断言顺序，故此处不用它）。
	deadline := time.After(10 * time.Second)
started:
	for {
		select {
		case msg, ok := <-cA.msgs:
			if !ok {
				t.Fatal("connection closed while waiting for task_started")
			}
			typ, _ := msg["type"].(string)
			if typ == "test_result" {
				t.Fatal("test_result arrived before task_started (order violated)")
			}
			if typ == "task_started" {
				data, _ := msg["data"].(map[string]interface{})
				if got, _ := data["taskId"].(string); got != taskID {
					t.Fatalf("task_started taskId = %q, want %q (actual created task)", got, taskID)
				}
				if got, _ := data["backendEpoch"].(string); got != epoch {
					t.Fatalf("task_started backendEpoch = %q, want %q", got, epoch)
				}
				break started
			}
		case <-deadline:
			t.Fatal("timeout waiting for task_started")
		}
	}
	row := cA.waitFor("test_result", 10*time.Second)
	rowData, _ := row["data"].(map[string]interface{})
	if _, ok := rowData["seq"]; !ok {
		t.Fatal("test_result row has no seq")
	}
	sess.stopTask()
	pollTaskEnded(t, sess, 10*time.Second)
}

// o1AssertTaskStartedFirst 从客户端消息流中读到 task_started 为止，断言它是
// task_started 与 test_result 两者中的第一个，且身份与期望一致。
// 直接读消息流（waitFor 会跳过不匹配类型，无法断言顺序）。
func o1AssertTaskStartedFirst(t *testing.T, c *r3Client, taskID, epoch string) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case msg, ok := <-c.msgs:
			if !ok {
				t.Fatal("connection closed while waiting for task_started")
			}
			typ, _ := msg["type"].(string)
			if typ == "test_result" {
				t.Fatal("test_result arrived before task_started (order violated)")
			}
			if typ == "task_started" {
				data, _ := msg["data"].(map[string]interface{})
				if got, _ := data["taskId"].(string); got != taskID {
					t.Fatalf("task_started taskId = %q, want %q (actual created task)", got, taskID)
				}
				if got, _ := data["backendEpoch"].(string); got != epoch {
					t.Fatalf("task_started backendEpoch = %q, want %q", got, epoch)
				}
				return
			}
		case <-deadline:
			t.Fatal("timeout waiting for task_started")
		}
	}
}

// TestO1_TaskStartedIdentityConsistent：O-1 定点修正后的正常身份一致性（测试 A）。
// 目的：task_started 的 taskId/backendEpoch 与 beginTask 实际创建的任务身份
// 完全一致，且先于第一条结果消息；两条任务入口（startTaskNamed / runTaskSync）
// 均覆盖。
// 并发控制：无并发——顺序由程序顺序保证（publishTaskStarted 在 worker 启动
// 之前、第一条结果消息之前）；测试在客户端侧按到达顺序断言。
// 预期：客户端收到的 task_started 与 test_result 的第一个必须是 task_started，
// 且身份与服务端实际任务一致。
// 能捕获的错误：快照身份与实际任务不符、结果行先于 task_started 到达、
// 某条入口未发送 task_started。
func TestO1_TaskStartedIdentityConsistent(t *testing.T) {
	// 入口 (a)：startTaskNamed（经 startSyntheticTask）。
	t.Run("startTaskNamed", func(t *testing.T) {
		r3Setup(t)
		_, wsURL := r3Server(t)
		cA := r3Dial(t, wsURL)
		cA.readInit(10 * time.Second)
		cA.register("test-o1-identity-page")
		sess := snapshotServerSession(t, cA.connectionID)
		if sess == nil {
			t.Fatal("no server session for A")
		}
		run := func(ctx context.Context, s *appSession) {
			s.sendTaskResult(ctx, "test_result", map[string]interface{}{"ip": "1.2.3.4"})
			<-ctx.Done()
		}
		startSyntheticTask(t, sess, run)
		pollSessionRunning(t, sess, 10*time.Second)
		taskID := sess.taskIDSnapshot()
		epoch := currentBackendEpoch()
		o1AssertTaskStartedFirst(t, cA, taskID, epoch)
		row := cA.waitFor("test_result", 10*time.Second)
		rowData, _ := row["data"].(map[string]interface{})
		if _, ok := rowData["seq"]; !ok {
			t.Fatal("test_result row has no seq")
		}
		sess.stopTask()
		pollTaskEnded(t, sess, 10*time.Second)
	})
	// 入口 (b)：runTaskSync（同步执行体；另起 goroutine 调用，执行体发一行
	// 结果后立即返回。taskID 在 endTask 后仍保留、epoch 无 reset 故不变，
	// 故任务结束后读取仍是创建时的身份。）
	t.Run("runTaskSync", func(t *testing.T) {
		r3Setup(t)
		_, wsURL := r3Server(t)
		cA := r3Dial(t, wsURL)
		cA.readInit(10 * time.Second)
		cA.register("test-o1-identity-page")
		sess := snapshotServerSession(t, cA.connectionID)
		if sess == nil {
			t.Fatal("no server session for A")
		}
		syncDone := make(chan error, 1)
		go func() {
			syncDone <- sess.runTaskSync(func(ctx context.Context, s *appSession) {
				s.sendTaskResult(ctx, "test_result", map[string]interface{}{"ip": "1.2.3.4"})
			})
		}()
		select {
		case err := <-syncDone:
			if err != nil {
				t.Fatalf("runTaskSync returned error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for runTaskSync to return")
		}
		taskID := sess.taskIDSnapshot()
		epoch := currentBackendEpoch()
		o1AssertTaskStartedFirst(t, cA, taskID, epoch)
		row := cA.waitFor("test_result", 10*time.Second)
		rowData, _ := row["data"].(map[string]interface{})
		if _, ok := rowData["seq"]; !ok {
			t.Fatal("test_result row has no seq")
		}
		pollTaskEnded(t, sess, 10*time.Second)
	})
}

// o1RunResetInterleave 执行一次 reset 交错场景（O-1 测试 B 的单入口版本）：
// beginTask 成功返回后暂停在 task_started 发送之前，执行 resetBackendGeneration
// 推进 epoch，再释放门；随后断言旧任务的 task_started 未被发布（无混合身份、
// 无失效事件），并验证 reset 后的新任务不受旧事件/旧快照污染。
// useSync==false 用 startTaskNamed，true 用 runTaskSync。
func o1RunResetInterleave(t *testing.T, useSync bool) {
	t.Helper()
	r3Setup(t)
	_, wsURL := r3Server(t)
	pageID := "test-o1-reset-page"
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageID)
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	// 门 + 完成信号 + 负载观测（均在 beginTask 返回后、发送决策前才发布，
	// 不受 r3Setup 的 resetTestHooks 清理影响；门内不持有任何业务锁，
	// 故测试可在门内执行 resetBackendGeneration）。
	gate := newDisconnectTestGate()
	testHookSet(&testTaskStartedGate, gate)
	done := make(chan struct{}, 4)
	testHookSet(&testTaskStartedDoneHook, func() { done <- struct{}{} })
	payloads := make(chan [2]string, 4)
	testHookSet(&testTaskStartedPayloadHook, func(taskID, epoch string) { payloads <- [2]string{taskID, epoch} })
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate.release) }) }
	t.Cleanup(func() {
		testHookSet(&testTaskStartedGate, nil)
		testHookSet(&testTaskStartedDoneHook, nil)
		testHookSet(&testTaskStartedPayloadHook, nil)
		release()
	})
	epochBefore := currentBackendEpoch()
	// 启动旧任务：beginTask 成功后停在发布门内。
	syncDone := make(chan error, 1)
	if useSync {
		go func() {
			syncDone <- sess.runTaskSync(func(ctx context.Context, s *appSession) {})
		}()
	} else {
		go startSyntheticTask(t, sess, longTaskRun)
	}
	select {
	case <-gate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for testTaskStartedGate")
	}
	oldTaskID := sess.taskIDSnapshot()
	// 门内执行 reset：epoch 推进、旧任务被取消、旧连接被关闭。
	resetBackendGeneration()
	epochAfter := currentBackendEpoch()
	if epochAfter == epochBefore {
		t.Fatal("resetBackendGeneration did not advance backendEpoch")
	}
	// 释放门 → 发布决策执行（正式实现应抑制发送）。
	release()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for task_started publish decision")
	}
	// 断言 1：未发布任何负载（更不可能是旧 taskId + 新 epoch 的混合身份）。
	// done 在负载 hook 之后触发，故此处非阻塞读取即为最终结论；反向验证时
	// 此处必 fail（旧实现会记录混合身份）。
	select {
	case p := <-payloads:
		t.Fatalf("stale task_started published after reset: taskId=%q backendEpoch=%q (old task %q, epoch before %q after %q)", p[0], p[1], oldTaskID, epochBefore, epochAfter)
	default:
	}
	// 断言 2：客户端侧同样收不到旧任务的 task_started（2 秒缺席窗口）。
	cA.waitForAbsent("task_started", 2*time.Second)
	// 旧任务已结束（异步入口：worker 因 ctx 取消而退出；同步入口：
	// runTaskSync 已返回）。
	if useSync {
		select {
		case err := <-syncDone:
			if err != nil {
				t.Fatalf("runTaskSync returned error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for runTaskSync to return")
		}
	} else {
		pollTaskEnded(t, sess, 10*time.Second)
	}
	// 断言 3：reset 后的新任务不受旧事件/旧快照污染——新连接、新任务，
	// task_started 身份为全新 taskId + 新 epoch，且与旧 taskId 不同。
	cB := r3Dial(t, wsURL)
	cB.readInit(10 * time.Second)
	cB.register("test-o1-reset-new-page")
	sessB := snapshotServerSession(t, cB.connectionID)
	if sessB == nil {
		t.Fatal("no server session for B")
	}
	if useSync {
		go func() {
			syncDone <- sessB.runTaskSync(func(ctx context.Context, s *appSession) {
				s.sendTaskResult(ctx, "test_result", map[string]interface{}{"ip": "1.2.3.4"})
			})
		}()
	} else {
		run := func(ctx context.Context, s *appSession) {
			s.sendTaskResult(ctx, "test_result", map[string]interface{}{"ip": "1.2.3.4"})
			<-ctx.Done()
		}
		startSyntheticTask(t, sessB, run)
	}
	// 新任务的发布同样经过门（已释放，立即通过）并触发 done 与负载 hook。
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for new task publish decision")
	}
	select {
	case p := <-payloads:
		if p[0] == oldTaskID {
			t.Fatalf("new task_started reuses old taskId %q (stale snapshot pollution)", oldTaskID)
		}
		if p[1] != epochAfter {
			t.Fatalf("new task_started backendEpoch = %q, want reset epoch %q", p[1], epochAfter)
		}
		// 客户端侧核验收到的 task_started 与负载一致。
		msg := cB.waitFor("task_started", 10*time.Second)
		data, _ := msg["data"].(map[string]interface{})
		if got, _ := data["taskId"].(string); got != p[0] {
			t.Fatalf("new task_started taskId = %q, want %q", got, p[0])
		}
		if got, _ := data["backendEpoch"].(string); got != p[1] {
			t.Fatalf("new task_started backendEpoch = %q, want %q", got, p[1])
		}
	default:
		t.Fatal("new task_started was not published (payload hook not called)")
	}
	if useSync {
		select {
		case err := <-syncDone:
			if err != nil {
				t.Fatalf("new runTaskSync returned error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for new runTaskSync to return")
		}
		pollTaskEnded(t, sessB, 10*time.Second)
	} else {
		pollSessionRunning(t, sessB, 10*time.Second)
		sessB.stopTask()
		pollTaskEnded(t, sessB, 10*time.Second)
	}
}

// TestO1_TaskStartedResetInterleave：O-1 定点修正的 reset 交错测试（测试 B）。
// 目的：beginTask 成功返回后、task_started 发送之前发生 resetBackendGeneration
// 时，不发布旧 taskId + 新 backendEpoch 的混合身份，也不发送已失效任务的
// task_started 事件；reset 后的新任务不受旧事件/旧快照污染。
// 两条任务入口（startTaskNamed / runTaskSync）各跑一次完整场景。
// 并发控制：门 + done hook + 负载 hook 全部确定性同步，不依赖 Sleep 猜测时序。
// 预期：门内 reset 后释放门，负载 hook 全程不触发，客户端 2 秒内收不到
// task_started；新任务的 task_started 为全新 taskId + 新 epoch。
// 能捕获的错误：发送时重读拼接导致的混合身份、失效任务事件仍发布、
// 新任务复用旧 taskId/旧快照。
func TestO1_TaskStartedResetInterleave(t *testing.T) {
	t.Run("startTaskNamed", func(t *testing.T) { o1RunResetInterleave(t, false) })
	t.Run("runTaskSync", func(t *testing.T) { o1RunResetInterleave(t, true) })
}

// TestO1_TaskStartedIdentityValidInterleave：O-1 第二轮定点修正的确定性交错
// 测试。目的：taskStartedIdentityValid 的 epoch 检查必须与任务状态、日志
// 指针的检查在同一个 taskMutex 临界区内完成；上一轮“先无锁读 epoch，再取
// 锁读状态”的顺序存在竞态——对 reset 2a 清理覆盖不到的 session（如 CLI 的
// bare session），reset 推进 epoch 时不清理它，发布方先读旧 epoch、随后取
// 锁看到旧任务仍 running、日志仍非 nil，便错误发布已失效的旧任务。
// 并发控制：裸 session（不在 connLoopState 中，reset 2a 覆盖不到——生产对
// 应 CLI 会话）+ 两道确定性门。验证门停在 taskStartedIdentityValid 的临界
// 区入口（已取 taskMutex、尚未读 epoch）；epoch 门停在 reset 发布新 epoch
// 之后、执行后续步骤之前。时序由门与通道同步，不用 Sleep 猜测。
// 预期：正式实现在持有 taskMutex 时读到新 epoch，拒绝旧身份，不触发
// payload hook、不发送 task_started；reset 完成后旧任务可正常结束。
// 能捕获的错误：epoch 检查在锁外（反向验证时本测试稳定失败，旧任务被发布）。
// 注意：验证门持有 taskMutex，测试在门内不得取该锁读状态；步骤 6 的
// isTaskRunning 直接读字段是例外——最后一次写入在 beginTask（本 goroutine
// 程序顺序在前），门内 goroutine 与 reset 均不写该字段（reset 覆盖不到本
// session，已由前置断言保证），无并发写，-race 下安全；resultLog 为原子
// 指针，无锁读本来就是合法访问。
func TestO1_TaskStartedIdentityValidInterleave(t *testing.T) {
	r3Setup(t)
	// 1. 裸 session：不在 connLoopState 中——reset 2a 的孤儿收集覆盖不到它，
	//    这正是本轮竞态的前提。生产对应 CLI 会话（cli.go 的 bare appSession
	//    经 runTaskSync 跑任务）；前置断言保证该前提成立。
	sess := &appSession{connectionID: "test-o1r2-valid", backendGeneration: atomic.LoadUint64(&backendGeneration)}
	if _, ok := snapshotLoopState(t, sess.connectionID); ok {
		t.Fatal("test session unexpectedly present in connLoopState (reset would clean it, race not constructible)")
	}
	epochBefore := currentBackendEpoch()
	// 2. 安装门与 hook（r3Setup 之后发布，不受 resetTestHooks 清理影响）。
	validGate := newDisconnectTestGate()
	testHookSet(&testTaskStartedValidGate, validGate)
	resetGate := newDisconnectTestGate()
	testHookSet(&testResetEpochGate, resetGate)
	done := make(chan struct{}, 1)
	testHookSet(&testTaskStartedDoneHook, func() { done <- struct{}{} })
	payloads := make(chan [2]string, 1)
	testHookSet(&testTaskStartedPayloadHook, func(taskID, epoch string) { payloads <- [2]string{taskID, epoch} })
	var releaseValidOnce, releaseResetOnce sync.Once
	releaseValid := func() { releaseValidOnce.Do(func() { close(validGate.release) }) }
	releaseReset := func() { releaseResetOnce.Do(func() { close(resetGate.release) }) }
	t.Cleanup(func() {
		testHookSet(&testTaskStartedValidGate, nil)
		testHookSet(&testResetEpochGate, nil)
		testHookSet(&testTaskStartedDoneHook, nil)
		testHookSet(&testTaskStartedPayloadHook, nil)
		releaseValid()
		releaseReset()
	})
	// 3. 创建任务 A，捕获旧身份快照；确认任务正在运行。
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	ident, ok := sess.beginTask(cancel, "o1r2 task", "official", nil)
	if !ok {
		t.Fatal("beginTask failed")
	}
	if ident.taskID == "" || ident.backendEpoch != epochBefore {
		t.Fatalf("beginTask snapshot = %+v, want non-empty taskID and epoch %q", ident, epochBefore)
	}
	st, running, _ := snapshotDisconnectState(t, sess)
	if !running || st != stateRunning {
		t.Fatalf("after beginTask: state=%d running=%v, want running/stateRunning", int(st), running)
	}
	// 4. 发布 goroutine：进入 taskStartedIdentityValid 后停在临界区入口门内
	//    （已取 taskMutex、尚未读 epoch）。
	go sess.publishTaskStarted(ident)
	select {
	case <-validGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for testTaskStartedValidGate")
	}
	// 5. reset goroutine：发布新 epoch 后停在 epoch 门内（epoch 已是新值，
	//    后续步骤尚未执行；本 session 不在清理覆盖内，不会被清理）。
	resetDone := make(chan struct{})
	go func() {
		defer close(resetDone)
		resetBackendGeneration()
	}()
	select {
	case <-resetGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for testResetEpochGate")
	}
	epochAfter := currentBackendEpoch()
	if epochAfter == epochBefore {
		t.Fatal("resetBackendGeneration did not advance backendEpoch")
	}
	// 6. 中间态确认：epoch 已是新值，但旧任务仍 running、resultLog 仍非 nil——
	//    这正是旧检查顺序会误判的状态组合（见本测试头注释的直接读字段说明）。
	if !sess.isTaskRunning {
		t.Fatal("old task isTaskRunning == false while reset is mid-flight (want true)")
	}
	if sess.resultLog.Load() == nil {
		t.Fatal("old task resultLog == nil while reset is mid-flight (want non-nil)")
	}
	// 7. 释放验证门 → 正式实现在持有 taskMutex 时读到新 epoch，拒绝旧身份。
	releaseValid()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for task_started publish decision")
	}
	// 8. 断言：未发布任何负载——reset 后的旧身份不得以任何形式发布（本轮的
	//    错误不是混合身份，而是已失效旧身份的发布；反向验证时此处必 fail）。
	select {
	case p := <-payloads:
		t.Fatalf("stale task_started published after reset: taskId=%q backendEpoch=%q (old task %q, epoch before %q after %q)", p[0], p[1], ident.taskID, epochBefore, epochAfter)
	default:
	}
	// 9. 释放 reset 门，等待 reset 正常完成；随后结束旧任务并确认清理。
	//    （reset 不清理本 session——这正是竞态前提——故由测试直接结束任务；
	//    断言任务可正常结束、无残留运行态。）
	releaseReset()
	select {
	case <-resetDone:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for resetBackendGeneration to complete")
	}
	sess.endTask()
	pollTaskEnded(t, sess, 10*time.Second)
}

// TestT7_F1_SweeperRetention：sweeper 保留运行任务的 session 条目（F-1，阻断级）。
// 目的：认领成功后，旧连接的 exited 条目是运行任务的唯一可见引用；sweeper
// 删除它会使任务从 resetBackendGeneration 的孤儿收集范围中消失（僵尸任务）。
// 三部分：(A) 过期删除路径 + reset 可见性；(B) 容量淘汰路径跳过运行任务；
// (C) 无安全淘汰候选时允许超过软上限。
// 并发控制：直接调 sweepOnceLocked（不等待 60s tick）；保留期用 var 覆盖。
// 预期：(A) 保留期缩短后 sweep 不删认领任务的条目；reset 能取消该任务并清日志；
// (B) 容量超限时只淘汰无运行任务的条目，认领任务的条目保留；
// (C) 全部候选都有运行任务时不删任何条目，长度仍超上限。
// 能捕获的错误：守卫缺失（条目被删 → reset 看不到任务 → 任务不取消）、
// 容量淘汰误删运行任务、为满足上限删运行任务的唯一引用。
// 反向验证：去掉保留守卫后重跑，(A) 须稳定失败（条目被删、reset 后任务仍运行——
// 证明守卫是 reset 可见性的 operative 机制）。
func TestT7_F1_SweeperRetention(t *testing.T) {
	t.Cleanup(func() { connLoopStateRetention = 5 * time.Minute })
	// (A) 过期删除路径 + reset 可见性。
	f := setupClaimTask(t, 30*time.Second)
	f.armByDisconnect(t)
	cB, _ := dialClaimer(t, f.wsURL, f.pageID)
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	oldConnID := f.cA.connectionID
	connLoopStateRetention = time.Millisecond
	backgroundTaskMutex.Lock()
	sweepOnceLocked()
	backgroundTaskMutex.Unlock()
	if _, ok := snapshotLoopState(t, oldConnID); !ok {
		t.Fatal("(A) connLoopState entry for the claimed task's old connection was deleted by sweep (F-1 guard missing)")
	}
	resetBackendGeneration()
	pollTaskEnded(t, f.sess, 10*time.Second)
	st, _, _ := snapshotDisconnectState(t, f.sess)
	if st != stateCancelled {
		t.Fatalf("(A) after reset: state=%d, want cancelled", int(st))
	}
	if log := f.sess.resultLog.Load(); log != nil {
		t.Fatal("(A) resultLog not cleared by reset (orphan cleanup missed the claimed task)")
	}
	// (B) 容量淘汰路径跳过运行任务。
	f2 := setupClaimTask(t, 30*time.Second)
	f2.armByDisconnect(t)
	cB2, _ := dialClaimer(t, f2.wsURL, f2.pageID)
	cB2.claim(f2.taskID, f2.epoch, f2.pageID)
	cB2.waitResumeAccepted(10*time.Second, f2.taskID, f2.epoch)
	oldConnID2 := f2.cA.connectionID
	connLoopStateRetention = 5 * time.Minute
	now := time.Now()
	backgroundTaskMutex.Lock()
	for i := 0; i < 1001; i++ {
		connLoopState[fmt.Sprintf("fake-cap-%d", i)] = loopEntry{state: loopStateExited, exitedAt: now, session: nil}
	}
	before := len(connLoopState)
	sweepOnceLocked()
	after := len(connLoopState)
	backgroundTaskMutex.Unlock()
	if after > 1000 {
		t.Fatalf("(B) after capacity sweep: len=%d, want <= 1000 (before=%d)", after, before)
	}
	if _, ok := snapshotLoopState(t, oldConnID2); !ok {
		t.Fatal("(B) claimed task's entry was evicted by capacity sweep (F-1 guard missing on capacity path)")
	}
	// (C) 无安全淘汰候选时允许超过软上限。
	f3 := setupClaimTask(t, 30*time.Second)
	f3.armByDisconnect(t)
	cB3, _ := dialClaimer(t, f3.wsURL, f3.pageID)
	cB3.claim(f3.taskID, f3.epoch, f3.pageID)
	cB3.waitResumeAccepted(10*time.Second, f3.taskID, f3.epoch)
	oldConnID3 := f3.cA.connectionID
	runningSess := &appSession{connectionID: "fake-running-conn", isTaskRunning: true}
	backgroundTaskMutex.Lock()
	for i := 0; i < 1001; i++ {
		connLoopState[fmt.Sprintf("fake-run-%d", i)] = loopEntry{state: loopStateExited, exitedAt: now, session: runningSess}
	}
	before3 := len(connLoopState)
	sweepOnceLocked()
	after3 := len(connLoopState)
	backgroundTaskMutex.Unlock()
	if after3 != before3 {
		t.Fatalf("(C) sweep deleted %d entries with no safe candidate, want 0 (before=%d after=%d)", before3-after3, before3, after3)
	}
	if after3 <= 1000 {
		t.Fatalf("(C) len=%d after sweep, want > 1000 (soft cap may be exceeded when no safe candidate)", after3)
	}
	if _, ok := snapshotLoopState(t, oldConnID3); !ok {
		t.Fatal("(C) claimed task's entry was deleted with no safe candidate")
	}
	// 收尾：停止仍运行的任务（B/C 两部分）、关闭所有认领连接并等服务端
	// 循环退出，避免测试结束的 cleanup（恢复 disconnectGracePeriod）与仍在
	// 武装的读循环竞争（-race）。任务已停则武装在状态检查处返回，不读 grace；
	// 循环退出则 Defer C 已等 ping goroutine 退出，读操作先行于 cleanup。
	f2.sess.stopTask()
	pollTaskEnded(t, f2.sess, 10*time.Second)
	f3.sess.stopTask()
	pollTaskEnded(t, f3.sess, 10*time.Second)
	cB.Close()
	cB2.Close()
	cB3.Close()
	// 只等最后夹具（C）的循环退出：A/B 夹具的条目已被后来的 r3Setup/reset
	// 删除，pollLoopExited 会永远超时；它们的循环早已随 reset 关闭而退出，
	// 且任务已停（武装在状态检查处返回，不读 grace），故无需等待。
	pollLoopExited(t, cB3.connectionID, 10*time.Second)
}

// ============ S7 C1 批量 2：计时器 / 认领竞争测试 ============

// TestT7_16_ReadLoopArmAndClaimCount
// 目的：真实读循环错误分支武装恰好一次，且 Defer A 的重复武装是 no-op；
// 其后认领成功。初始：运行任务。并发控制：arm-count hook 计数（非 Sleep 猜测）。
// 捕获：读循环错误分支外的重复武装 / Defer A 误取消已恢复任务。
func TestT7_16_ReadLoopArmAndClaimCount(t *testing.T) {
	f := setupClaimTask(t, 30*time.Second)
	var armMu sync.Mutex
	armCount := 0
	testHookSet(&testArmCountHook, func() {
		armMu.Lock()
		armCount++
		armMu.Unlock()
	})
	t.Cleanup(func() { testHookSet(&testArmCountHook, nil) })
	f.armByDisconnect(t)
	// 武装计数恰好为 1。
	pollUntil(t, "arm count == 1", 10*time.Second, 20*time.Millisecond, func() bool {
		armMu.Lock()
		defer armMu.Unlock()
		return armCount == 1
	})
	// 500ms 稳定窗口内计数仍为 1（Defer A 的重复武装是 no-op）。
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		armMu.Lock()
		c := armCount
		armMu.Unlock()
		if c != 1 {
			t.Fatalf("arm count changed to %d, want exactly 1 (Defer A double-arm?)", c)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// 其后认领成功，任务恢复。
	cB, _ := dialClaimer(t, f.wsURL, f.pageID)
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	st, running, timer := snapshotDisconnectState(t, f.sess)
	if !running || st != stateRunning || timer != nil {
		t.Fatalf("after claim: state=%d running=%v timerSet=%v, want running/true/false",
			int(st), running, timer != nil)
	}
	closeClaimedTask(t, f.sess, cB)
}

// TestT7_17_ClaimBeforeDeferA
// 目的：Defer A 暂停在入口门期间认领成功；释放 Defer A 后其武装是 no-op，
// 任务仍在运行（归属已变）。初始：运行任务。并发控制：testDeferAEntryGate。
// 捕获：Defer A 在认领后误武装 / 误取消已恢复任务。
func TestT7_17_ClaimBeforeDeferA(t *testing.T) {
	f := setupClaimTask(t, 30*time.Second)
	deferGate := newDisconnectTestGate()
	testHookSet(&testDeferAEntryGate, deferGate)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(deferGate.release) }) }
	t.Cleanup(func() { testHookSet(&testDeferAEntryGate, nil); release() })
	f.armByDisconnect(t)
	// 等 Defer A 到达入口门（读循环已退出、Defer A 暂停在门内）。
	select {
	case <-deferGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for testDeferAEntryGate")
	}
	// 门内认领成功。
	cB, _ := dialClaimer(t, f.wsURL, f.pageID)
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	// 释放 Defer A：其 arm 因归属已变（s.ws 已是 B 的 socket）而是 no-op。
	release()
	pollLoopExited(t, f.cA.connectionID, 10*time.Second)
	// 任务仍在运行（B 的连接上），计时器已停。
	st, running, timer := snapshotDisconnectState(t, f.sess)
	if !running || st != stateRunning || timer != nil {
		t.Fatalf("after Defer A released: state=%d running=%v timerSet=%v, want running/true/false",
			int(st), running, timer != nil)
	}
	closeClaimedTask(t, f.sess, cB)
}

// TestT7_18_TimerWins
// 目的：计时器回调先进入阶段 3（已转换到 cancelling、尚未调 cancelFn）；
// 此时认领必须被拒 already-cancelled。初始：运行任务，grace 100ms，
// watchdog 快照新鲜（回调到达时判定取消而非 defer）。
// 并发控制：testTimerCallbackGate（阶段 3 门）。捕获：认领与 cancelling
// 状态的决议点互斥失效（认领抢占已进入 cancelling 的任务）。
func TestT7_18_TimerWins(t *testing.T) {
	r3Setup(t)
	setTestGracePeriod(t, 100*time.Millisecond)
	cbGate := newDisconnectTestGate()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(cbGate.release) }) }
	t.Cleanup(func() { testHookSet(&testTimerCallbackGate, nil); release() })
	f := setupClaimTask(t, 100*time.Millisecond)
	testHookSet(&testTimerCallbackGate, cbGate)
	injectFreshWatchdogSnapshot(t)
	f.armByDisconnect(t)
	// 等回调到达阶段 3 门（已转换到 cancelling、cancelFn 尚未调用）。
	select {
	case <-cbGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for testTimerCallbackGate")
	}
	st, _, _ := snapshotDisconnectState(t, f.sess)
	if st != stateCancelling {
		t.Fatalf("state=%d at callback gate, want cancelling", int(st))
	}
	// 此时认领必须被拒 already-cancelled。
	cB, _ := dialClaimer(t, f.wsURL, f.pageID)
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeRejected(10*time.Second, "already-cancelled")
	// 释放回调 → cancel 执行 → 任务结束为 cancelled。
	release()
	pollTaskEnded(t, f.sess, 10*time.Second)
	st2, _, _ := snapshotDisconnectState(t, f.sess)
	if st2 != stateCancelled {
		t.Fatalf("after timer cancel: state=%d, want cancelled", int(st2))
	}
}

// TestT7_19_ClaimWins
// 目的：回调入口门暂停回调（取 taskMutex 之前）；门内认领成功；释放回调后，
// 回调的 V1 失败退出，任务不受影响。初始：运行任务，grace 100ms。
// 并发控制：testTimerCallbackEntryGate。捕获：旧回调在认领后误取消已恢复任务。
func TestT7_19_ClaimWins(t *testing.T) {
	r3Setup(t)
	setTestGracePeriod(t, 100*time.Millisecond)
	entryGate := newDisconnectTestGate()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(entryGate.release) }) }
	t.Cleanup(func() { testHookSet(&testTimerCallbackEntryGate, nil); release() })
	f := setupClaimTask(t, 100*time.Millisecond)
	testHookSet(&testTimerCallbackEntryGate, entryGate)
	f.armByDisconnect(t)
	// 等回调到达入口门（已启动、尚未取 taskMutex）。
	select {
	case <-entryGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for testTimerCallbackEntryGate")
	}
	// 门内认领成功。
	cB, _ := dialClaimer(t, f.wsURL, f.pageID)
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	// 释放回调 → 阶段 2 的 V1 失败退出；500ms 有界窗口内任务仍运行、无计时器。
	release()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		st, running, timer := snapshotDisconnectState(t, f.sess)
		if !running || st != stateRunning || timer != nil {
			t.Fatalf("after callback released: state=%d running=%v timerSet=%v, want running/true/false",
				int(st), running, timer != nil)
		}
		time.Sleep(20 * time.Millisecond)
	}
	closeClaimedTask(t, f.sess, cB)
}

// TestT7_20_TimerHoldsCancelFn
// 目的：计时器到期时，回调持有非 nil 的 cancelFn；释放后任务经 cancel 路径
// 结束为 cancelled（非自然完成）。初始：运行任务，grace 100ms，watchdog
// 快照新鲜。并发控制：testTimerCallbackGate。捕获：回调丢失 cancelFn
// （任务悬而不决）/ 取消路径未执行。
func TestT7_20_TimerHoldsCancelFn(t *testing.T) {
	r3Setup(t)
	setTestGracePeriod(t, 100*time.Millisecond)
	cbGate := newDisconnectTestGate()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(cbGate.release) }) }
	t.Cleanup(func() { testHookSet(&testTimerCallbackGate, nil); release() })
	f := setupClaimTask(t, 100*time.Millisecond)
	testHookSet(&testTimerCallbackGate, cbGate)
	injectFreshWatchdogSnapshot(t)
	f.armByDisconnect(t)
	select {
	case <-cbGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for testTimerCallbackGate")
	}
	// 阶段 3 门内：回调已转换到 cancelling，taskCancel（cancelFn）必须非 nil。
	f.sess.taskMutex.Lock()
	cancelFn := f.sess.taskCancel
	f.sess.taskMutex.Unlock()
	if cancelFn == nil {
		t.Fatal("at callback gate: taskCancel is nil, timer holds no cancelFn")
	}
	st, _, _ := snapshotDisconnectState(t, f.sess)
	if st != stateCancelling {
		t.Fatalf("state=%d at callback gate, want cancelling", int(st))
	}
	// 释放回调 → cancel 执行 → 任务结束为 cancelled。
	release()
	pollTaskEnded(t, f.sess, 10*time.Second)
	st2, _, _ := snapshotDisconnectState(t, f.sess)
	if st2 != stateCancelled {
		t.Fatalf("after timer cancel: state=%d, want cancelled", int(st2))
	}
}

// TestT7_21_TimerClaimRace
// 目的：计时器回调与认领同时放行，循环 20 轮；每轮胜者唯一且结果一致
// （认领胜 → running + resume_accepted；回调胜 → cancelled + already-cancelled）。
// 初始：每轮新建运行任务，grace 100ms，watchdog 快照新鲜。
// 并发控制：testTimerCallbackEntryGate + testClaimEntryGate 同时放行。
// 捕获：双重认领 / 终态回退 / 响应与最终状态不一致。
func TestT7_21_TimerClaimRace(t *testing.T) {
	for round := 0; round < 20; round++ {
		r3Setup(t)
		setTestGracePeriod(t, 100*time.Millisecond)
		cbEntry := newDisconnectTestGate()
		claimEntry := newDisconnectTestGate()
		var releaseOnce sync.Once
		release := func() {
			releaseOnce.Do(func() {
				close(cbEntry.release)
				close(claimEntry.release)
			})
		}
		t.Cleanup(func() {
			testHookSet(&testTimerCallbackEntryGate, nil)
			testHookSet(&testClaimEntryGate, nil)
			release()
		})
		f := setupClaimTask(t, 100*time.Millisecond)
		testHookSet(&testClaimEntryGate, claimEntry)
		testHookSet(&testTimerCallbackEntryGate, cbEntry)
		injectFreshWatchdogSnapshot(t)
		f.armByDisconnect(t)
		// 等回调到达入口门。
		select {
		case <-cbEntry.entered:
		case <-time.After(10 * time.Second):
			t.Fatalf("round %d: timeout waiting for testTimerCallbackEntryGate", round)
		}
		// B 认领（handler 将停在认领入口门）；响应在 goroutine 中收取。
		cB, _ := dialClaimer(t, f.wsURL, f.pageID)
		claimDone := make(chan string, 1)
		go func() {
			cB.claim(f.taskID, f.epoch, f.pageID)
			typ, msg := cB.waitForAny(10*time.Second, "resume_accepted", "resume_rejected")
			if typ == "resume_accepted" {
				claimDone <- "accepted"
				return
			}
			data, _ := msg["data"].(map[string]interface{})
			reason, _ := data["reason"].(string)
			claimDone <- "rejected:" + reason
		}()
		// 等认领到达入口门。
		select {
		case <-claimEntry.entered:
		case <-time.After(10 * time.Second):
			t.Fatalf("round %d: timeout waiting for testClaimEntryGate", round)
		}
		// 同时放行两门：回调与认领竞争 taskMutex。
		release()
		var outcome string
		select {
		case outcome = <-claimDone:
		case <-time.After(10 * time.Second):
			t.Fatalf("round %d: timeout waiting for claim outcome", round)
		}
		// 一致性断言：二者必居其一，互斥。
		switch outcome {
		case "accepted":
			st, running, timer := snapshotDisconnectState(t, f.sess)
			if !running || st != stateRunning || timer != nil {
				t.Fatalf("round %d: claim accepted but state=%d running=%v timerSet=%v",
					round, int(st), running, timer != nil)
			}
			closeClaimedTask(t, f.sess, cB)
		case "rejected:already-cancelled":
			pollTaskEnded(t, f.sess, 10*time.Second)
			st, _, _ := snapshotDisconnectState(t, f.sess)
			if st != stateCancelled {
				t.Fatalf("round %d: claim rejected already-cancelled but final state=%d, want cancelled",
					round, int(st))
			}
		default:
			t.Fatalf("round %d: unexpected claim outcome %q", round, outcome)
		}
	}
}

// TestT7_27_DeferThenClaim
// 目的：回调暂停在阶段门（阶段 1 已完成、阶段 2 尚未开始）；门内认领成功；
// 释放回调后阶段 2 的 V1 失败退出，任务不受影响。初始：运行任务，
// grace 100ms，watchdog 快照新鲜。并发控制：testTimerCallbackPhaseGate。
// 捕获：阶段 1 的过期提案在认领后仍被执行。
func TestT7_27_DeferThenClaim(t *testing.T) {
	r3Setup(t)
	setTestGracePeriod(t, 100*time.Millisecond)
	phaseGate := newDisconnectTestGate()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(phaseGate.release) }) }
	t.Cleanup(func() { testHookSet(&testTimerCallbackPhaseGate, nil); release() })
	f := setupClaimTask(t, 100*time.Millisecond)
	testHookSet(&testTimerCallbackPhaseGate, phaseGate)
	injectFreshWatchdogSnapshot(t)
	f.armByDisconnect(t)
	// 等回调到达阶段门（阶段 1 已完成、阶段 2 尚未开始）。
	select {
	case <-phaseGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for testTimerCallbackPhaseGate")
	}
	// 门内认领成功。
	cB, _ := dialClaimer(t, f.wsURL, f.pageID)
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	// 释放回调 → 阶段 2 的 V1 失败退出；500ms 有界窗口内任务仍运行、无计时器。
	release()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		st, running, timer := snapshotDisconnectState(t, f.sess)
		if !running || st != stateRunning || timer != nil {
			t.Fatalf("after phase gate released: state=%d running=%v timerSet=%v, want running/true/false",
				int(st), running, timer != nil)
		}
		time.Sleep(20 * time.Millisecond)
	}
	closeClaimedTask(t, f.sess, cB)
}

// TestT7_28_DeferThenTaskSwitch
// 目的：(a) 回调暂停在阶段门期间，旧任务被取消、新任务在同一 session 上
// 启动；释放回调后其 V3（taskID 不符）失败退出，不取消新任务。
// (b) 计时器取消旧任务后，同一 session 上新任务正常启动。
// 初始：运行任务，grace 100ms。并发控制：testTimerCallbackPhaseGate。
// 捕获：旧回调误取消新任务 / 任务切换后计时器残留。
func TestT7_28_DeferThenTaskSwitch(t *testing.T) {
	// (a) 阶段门暂停 + 旧任务取消 + 同 session 新任务 + 释放回调。
	r3Setup(t)
	setTestGracePeriod(t, 100*time.Millisecond)
	phaseGate := newDisconnectTestGate()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(phaseGate.release) }) }
	t.Cleanup(func() { testHookSet(&testTimerCallbackPhaseGate, nil); release() })
	f := setupClaimTask(t, 100*time.Millisecond)
	testHookSet(&testTimerCallbackPhaseGate, phaseGate)
	oldTaskID := f.taskID
	injectFreshWatchdogSnapshot(t)
	f.armByDisconnect(t)
	select {
	case <-phaseGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for testTimerCallbackPhaseGate")
	}
	// 门内：取消旧任务，同一 session 上启动新任务。
	f.sess.stopTask()
	pollTaskEnded(t, f.sess, 10*time.Second)
	f.sess.taskMutex.Lock()
	stOld, _ := f.sess.disconnectState, f.sess.taskID
	f.sess.taskMutex.Unlock()
	if stOld != stateCancelled {
		t.Fatalf("after stopTask: state=%d, want cancelled", int(stOld))
	}
	startSyntheticTask(t, f.sess, longTaskRun)
	pollSessionRunning(t, f.sess, 10*time.Second)
	newTaskID := f.sess.taskIDSnapshot()
	if newTaskID == oldTaskID {
		t.Fatalf("new task id %q equals old task id", newTaskID)
	}
	// 释放旧回调 → V3 失败退出；新任务不受影响。
	release()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		st, running, timer := snapshotDisconnectState(t, f.sess)
		if !running || st != stateRunning || timer != nil {
			t.Fatalf("after old callback released: state=%d running=%v timerSet=%v, want running/true/false",
				int(st), running, timer != nil)
		}
		if f.sess.taskIDSnapshot() != newTaskID {
			t.Fatalf("task id changed after old callback released")
		}
		time.Sleep(20 * time.Millisecond)
	}
	f.sess.stopTask()
	pollTaskEnded(t, f.sess, 10*time.Second)

	// (b) 计时器取消旧任务后，同一 session 上新任务正常启动。
	r3Setup(t)
	setTestGracePeriod(t, 100*time.Millisecond)
	f2 := setupClaimTask(t, 100*time.Millisecond)
	injectFreshWatchdogSnapshot(t)
	f2.armByDisconnect(t)
	pollTaskEnded(t, f2.sess, 10*time.Second)
	st2, _, _ := snapshotDisconnectState(t, f2.sess)
	if st2 != stateCancelled {
		t.Fatalf("(b) after timer cancel: state=%d, want cancelled", int(st2))
	}
	startSyntheticTask(t, f2.sess, longTaskRun)
	pollSessionRunning(t, f2.sess, 10*time.Second)
	st3, running3, timer3 := snapshotDisconnectState(t, f2.sess)
	if !running3 || st3 != stateRunning || timer3 != nil {
		t.Fatalf("(b) new task: state=%d running=%v timerSet=%v, want running/true/false",
			int(st3), running3, timer3 != nil)
	}
	f2.sess.stopTask()
	pollTaskEnded(t, f2.sess, 10*time.Second)
}

// TestT7_34_TimerClaimStopRace
// 目的：计时器回调、认领、stop_task 三方同时放行；最终状态必为 cancelled，
// 任务恰好结束一次，认领响应与最终状态一致（accepted 或 already-cancelled）。
// 初始：运行任务，grace 100ms，watchdog 快照新鲜。
// 并发控制：testTimerCallbackEntryGate + testClaimEntryGate + test 控制的
// stop 释放通道，三方同时放行。捕获：三方竞争中的终态回退 / 双重结束 /
// 响应与状态不一致。
func TestT7_34_TimerClaimStopRace(t *testing.T) {
	r3Setup(t)
	setTestGracePeriod(t, 100*time.Millisecond)
	cbEntry := newDisconnectTestGate()
	claimEntry := newDisconnectTestGate()
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(cbEntry.release)
			close(claimEntry.release)
		})
	}
	t.Cleanup(func() {
		testHookSet(&testTimerCallbackEntryGate, nil)
		testHookSet(&testClaimEntryGate, nil)
		release()
	})
	f := setupClaimTask(t, 100*time.Millisecond)
	testHookSet(&testClaimEntryGate, claimEntry)
	testHookSet(&testTimerCallbackEntryGate, cbEntry)
	injectFreshWatchdogSnapshot(t)
	f.armByDisconnect(t)
	// 等回调到达入口门。
	select {
	case <-cbEntry.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for testTimerCallbackEntryGate")
	}
	// B 认领（handler 将停在认领入口门）；响应在 goroutine 中收取。
	cB, _ := dialClaimer(t, f.wsURL, f.pageID)
	claimDone := make(chan string, 1)
	go func() {
		cB.claim(f.taskID, f.epoch, f.pageID)
		typ, msg := cB.waitForAny(10*time.Second, "resume_accepted", "resume_rejected")
		if typ == "resume_accepted" {
			claimDone <- "accepted"
			return
		}
		data, _ := msg["data"].(map[string]interface{})
		reason, _ := data["reason"].(string)
		claimDone <- "rejected:" + reason
	}()
	// 等认领到达入口门。
	select {
	case <-claimEntry.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for testClaimEntryGate")
	}
	// stop_task（直接调任务 session 的 stopTask，与 handler 同一路）
	// 在 test 控制的通道后等待，与两门同时放行。
	stopRelease := make(chan struct{})
	stopDone := make(chan struct{})
	go func() {
		<-stopRelease
		f.sess.stopTask()
		close(stopDone)
	}()
	// 同时放行三方。
	release()
	close(stopRelease)
	// 等三方全部 settle。
	var outcome string
	select {
	case outcome = <-claimDone:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for claim outcome")
	}
	select {
	case <-stopDone:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for stopTask")
	}
	pollTaskEnded(t, f.sess, 10*time.Second)
	// 最终状态必为 cancelled；认领响应与之相容。
	st, _, _ := snapshotDisconnectState(t, f.sess)
	if st != stateCancelled {
		t.Fatalf("final state=%d, want cancelled (outcome=%q)", int(st), outcome)
	}
	if outcome != "accepted" && outcome != "rejected:already-cancelled" {
		t.Fatalf("unexpected claim outcome %q", outcome)
	}
}

// ============ S7 C1 批量 3：行为 / 边界 / 半开测试 ============

// TestT7_05_OldConnectionLateMessages
// 目的：旧连接断开后，其读循环已退出，迟到消息无法到达服务端；旧连接直接
// 再认领 → already-claimed；会话向旧连接的发送被归属复核丢弃。
// 初始：运行任务，B 已认领。并发控制：直接 in-package 调用（读循环已退出，
// 网络消息无法到达，故不用网络路径）。捕获：旧连接污染 / 旧连接重复认领。
func TestT7_05_OldConnectionLateMessages(t *testing.T) {
	f := setupClaimTask(t, 30*time.Second)
	// 捕获旧连接的服务端 socket（认领前）。
	oldConn, _ := f.sess.wsSnapshot()
	if oldConn == nil {
		t.Fatal("old server conn is nil before claim")
	}
	f.armByDisconnect(t)
	cB, _ := dialClaimer(t, f.wsURL, f.pageID)
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	// (1) 旧连接的读循环已退出：任何迟到消息都无法到达服务端。
	pollLoopExited(t, f.cA.connectionID, 10*time.Second)
	// (2) 旧连接直接再认领 → already-claimed（C-2 预检：任务已在 B 的连接上）。
	backgroundTaskMutex.Lock()
	v := findClaimCandidate(claimRequest{TaskID: f.taskID, BackendEpoch: f.epoch, PageID: f.pageID}, oldConn, f.sess)
	backgroundTaskMutex.Unlock()
	if v.action != claimActionReject || v.reason != "already-claimed" {
		t.Fatalf("old-conn re-claim: action=%q reason=%q, want reject/already-claimed", v.action, v.reason)
	}
	// (3) 会话向旧连接的发送被丢弃（send-time 归属复核：s.ws 已是 B 的 socket）。
	if ok := f.sess.sendWSMessageToConn(oldConn, "test_probe", map[string]interface{}{"x": 1}); ok {
		t.Fatal("sendWSMessageToConn to old conn returned true, want false (dropped)")
	}
	closeClaimedTask(t, f.sess, cB)
}

// TestT7_06_DeferAAfterClaim
// 目的：Defer A 暂停在入口门期间认领成功；释放 Defer A 后其 arm 是 no-op，
// 旧连接条目退出但任务仍在 B 上运行；第三连接认领 → already-claimed。
// 初始：运行任务。并发控制：testDeferAEntryGate。捕获：Defer A 在认领后误
// 取消已恢复任务 / 旧连接条目残留导致重复认领。
func TestT7_06_DeferAAfterClaim(t *testing.T) {
	f := setupClaimTask(t, 30*time.Second)
	deferGate := newDisconnectTestGate()
	testHookSet(&testDeferAEntryGate, deferGate)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(deferGate.release) }) }
	t.Cleanup(func() { testHookSet(&testDeferAEntryGate, nil); release() })
	f.armByDisconnect(t)
	select {
	case <-deferGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for testDeferAEntryGate")
	}
	// 门内认领成功。
	cB, _ := dialClaimer(t, f.wsURL, f.pageID)
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	// 释放 Defer A：其 arm 是 no-op；旧连接条目退出，但任务仍在 B 上运行。
	release()
	pollLoopExited(t, f.cA.connectionID, 10*time.Second)
	st, running, timer := snapshotDisconnectState(t, f.sess)
	if !running || st != stateRunning || timer != nil {
		t.Fatalf("after Defer A released: state=%d running=%v timerSet=%v, want running/true/false",
			int(st), running, timer != nil)
	}
	// 第三连接 C 认领同一任务 → already-claimed。
	cC, _ := dialClaimer(t, f.wsURL, f.pageID)
	cC.claim(f.taskID, f.epoch, f.pageID)
	cC.waitResumeRejected(10*time.Second, "already-claimed")
	closeClaimedTask(t, f.sess, cB)
}

// TestT7_07_TwoDisconnectsReclaim
// 目的：认领成功后再次断开 → 重新武装（新窗口）；失败的认领不续期
// （disconnectAt 不变）；第二次认领成功。初始：运行任务。
// 并发控制：门 + 轮询（无任意 Sleep 猜测调度）。捕获：重武装失败 /
// 失败认领续期窗口 / 第二次认领认到旧窗口。
func TestT7_07_TwoDisconnectsReclaim(t *testing.T) {
	f := setupClaimTask(t, 30*time.Second)
	f.armByDisconnect(t)
	// 第一次认领成功。
	cB, _ := dialClaimer(t, f.wsURL, f.pageID)
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	// B 再次断开 → 重新武装（新窗口）。
	cB.Close()
	pollDisconnectState(t, f.sess, stateDisconnectPending, 10*time.Second)
	_, _, timer := snapshotDisconnectState(t, f.sess)
	if timer == nil {
		t.Fatal("disconnectTimer nil after second disconnect")
	}
	_, _, _, at2, _ := snapshotDisconnectIdentity(t, f.sess)
	if at2 == 0 {
		t.Fatal("disconnectAt == 0 after second disconnect, want new window start")
	}
	// 失败的认领不续期：错误 pageId 的认领失败后 disconnectAt 不变。
	cC, _ := dialClaimer(t, f.wsURL, "wrong-page")
	cC.claim(f.taskID, f.epoch, "wrong-page")
	cC.waitResumeRejected(10*time.Second, "task-not-found")
	_, _, _, at3, _ := snapshotDisconnectIdentity(t, f.sess)
	if at3 != at2 {
		t.Fatalf("disconnectAt changed after failed claim: %d -> %d, want unchanged", at2, at3)
	}
	// 第二次认领成功。
	cD, _ := dialClaimer(t, f.wsURL, f.pageID)
	cD.claim(f.taskID, f.epoch, f.pageID)
	cD.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	st, running, timer2 := snapshotDisconnectState(t, f.sess)
	if !running || st != stateRunning || timer2 != nil {
		t.Fatalf("after second claim: state=%d running=%v timerSet=%v, want running/true/false",
			int(st), running, timer2 != nil)
	}
	closeClaimedTask(t, f.sess, cD)
}

// TestT7_11_WindowExpiryThenNewTask
// 目的：窗口到期 → 任务被取消；新连接 B 上新任务可启动（槽位已释放）；
// 第三连接用旧 taskId 认领 → already-cancelled，新任务不受影响。
// 初始：运行任务，grace 100ms，watchdog 快照新鲜。并发控制：轮询任务结束。
// 捕获：窗口到期后旧任务仍占槽位 / 旧认领认到新任务。
func TestT7_11_WindowExpiryThenNewTask(t *testing.T) {
	r3Setup(t)
	setTestGracePeriod(t, 100*time.Millisecond)
	f := setupClaimTask(t, 100*time.Millisecond)
	oldTaskID := f.taskID
	injectFreshWatchdogSnapshot(t)
	f.armByDisconnect(t)
	// 窗口到期 → 任务被取消。
	pollTaskEnded(t, f.sess, 10*time.Second)
	st, _, _ := snapshotDisconnectState(t, f.sess)
	if st != stateCancelled {
		t.Fatalf("after window expiry: state=%d, want cancelled", int(st))
	}
	// 新连接 B 上启动新任务（槽位已释放）。
	cB, sessB := dialClaimer(t, f.wsURL, f.pageID)
	startSyntheticTask(t, sessB, longTaskRun)
	pollSessionRunning(t, sessB, 10*time.Second)
	newTaskID := sessB.taskIDSnapshot()
	if newTaskID == oldTaskID {
		t.Fatalf("new task id %q equals old task id", newTaskID)
	}
	// 第三连接 C 用旧 taskId 认领 → already-cancelled（旧任务不在新任务上）。
	cC, _ := dialClaimer(t, f.wsURL, f.pageID)
	cC.claim(oldTaskID, f.epoch, f.pageID)
	cC.waitResumeRejected(10*time.Second, "already-cancelled")
	// 新任务不受影响，仍在运行。
	st2, running2, _ := snapshotDisconnectState(t, sessB)
	if !running2 || st2 != stateRunning {
		t.Fatalf("new task: state=%d running=%v, want running/true", int(st2), running2)
	}
	closeClaimedTask(t, sessB, cB)
}

// TestT7_12_SlotReleasedExactlyOnce
// 目的：认领后任务完成 → 槽位恰好释放一次：第二个任务可启动，第三个并发
// 启动被拒绝；第二个任务结束后第四个任务又可启动（槽位未泄漏）。
// 初始：运行任务，B 已认领。并发控制：直接 beginTask 返回值（不猜测调度）。
// 捕获：槽位泄漏（释放 0 次）/ 槽位重复释放（释放 2 次，允许两任务并发）。
func TestT7_12_SlotReleasedExactlyOnce(t *testing.T) {
	f := setupClaimTask(t, 30*time.Second)
	f.armByDisconnect(t)
	cB, sessB := dialClaimer(t, f.wsURL, f.pageID)
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	// 认领后任务完成（B 的 stop_task）→ 槽位释放。
	cB.send("stop_task", nil)
	pollTaskEnded(t, f.sess, 10*time.Second)
	// 第二个任务在 B 的 session 上启动。
	startSyntheticTask(t, sessB, longTaskRun)
	pollSessionRunning(t, sessB, 10*time.Second)
	// 第三个任务在 C 的 session 上启动 → 被拒绝（槽位被第二个任务占用）。
	_, sessC := dialClaimer(t, f.wsURL, f.pageID)
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, ok := sessC.beginTask(cancel, "s7 test task", "official", nil); ok {
		t.Fatal("third beginTask succeeded while second task running, want rejection (slot held)")
	}
	// 第二个任务结束后，第四个任务又可启动（槽位恰好释放一次，未泄漏）。
	sessB.stopTask()
	pollTaskEnded(t, sessB, 10*time.Second)
	startSyntheticTask(t, sessC, longTaskRun)
	pollSessionRunning(t, sessC, 10*time.Second)
	sessC.stopTask()
	pollTaskEnded(t, sessC, 10*time.Second)
}

// TestT7_13_StaleWorkerSendDropped
// 目的：旧 worker 持旧 ctx 在任务取消后发一行：必须被 sendTaskResult 的
// 冻结校验丢弃（不 panic、不投递）；新任务的行正常到达新连接。
// 初始：运行任务，B 已认领（会话 socket 存活，使冻结校验成为 operative
// 机制而非死 socket）。并发控制：waitForAbsent 有界窗口。捕获：滞留行
// 泄漏到新任务 / 旧行投递到新连接。
func TestT7_13_StaleWorkerSendDropped(t *testing.T) {
	f := setupClaimTask(t, 30*time.Second)
	f.armByDisconnect(t)
	cB, sessB := dialClaimer(t, f.wsURL, f.pageID)
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	// 捕获旧任务的日志 ctx（滞留 worker 的身份）。
	oldLog := f.sess.resultLog.Load()
	if oldLog == nil {
		t.Fatal("resultLog nil after claim")
	}
	oldCtx := withTaskLog(context.Background(), oldLog)
	// B 取消已认领任务 → endTask 冻结旧日志。
	cB.send("stop_task", nil)
	pollTaskEnded(t, f.sess, 10*time.Second)
	// 滞留 worker 发一行：冻结校验丢弃；500ms 有界窗口内 B 不得收到 test_result。
	f.sess.sendTaskResult(oldCtx, "test_result", map[string]interface{}{"row": 1})
	cB.waitForAbsent("test_result", 500*time.Millisecond)
	// 新任务在 B 上启动，其行正常到达 B（row==2，与滞留行区分）。
	startSyntheticTask(t, sessB, longTaskRun)
	pollSessionRunning(t, sessB, 10*time.Second)
	newLog := sessB.resultLog.Load()
	if newLog == nil {
		t.Fatal("new task resultLog nil")
	}
	newCtx := withTaskLog(context.Background(), newLog)
	sessB.sendTaskResult(newCtx, "test_result", map[string]interface{}{"row": 2})
	msg := cB.waitFor("test_result", 10*time.Second)
	data, _ := msg["data"].(map[string]interface{})
	if got, _ := data["row"].(float64); got != 2 {
		t.Fatalf("new task row = %v, want 2 (stale row must not leak into new task)", got)
	}
	sessB.stopTask()
	pollTaskEnded(t, sessB, 10*time.Second)
}

// TestT7_31_GraceWindowBoundary
// 目的：grace 边界：窗口内（约 4.8s）认领成功；窗口外（约 5.1s，计时器已
// 转换）认领 → already-cancelled。初始：运行任务，grace 5s，watchdog
// 快照新鲜。并发控制：B 预先拨号（认领发送落在边界时刻附近），轮询任务结束。
// 捕获：窗口边界误判（提前取消 / 过期仍可认领）。
func TestT7_31_GraceWindowBoundary(t *testing.T) {
	// 夹具 1：窗口内认领成功（约 4.8s，留 200ms 裕量）。
	r3Setup(t)
	setTestGracePeriod(t, 5*time.Second)
	f := setupClaimTask(t, 5*time.Second)
	injectFreshWatchdogSnapshot(t)
	f.armByDisconnect(t)
	cB, _ := dialClaimer(t, f.wsURL, f.pageID)
	time.Sleep(4800 * time.Millisecond)
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	st, running, timer := snapshotDisconnectState(t, f.sess)
	if !running || st != stateRunning || timer != nil {
		t.Fatalf("in-window claim: state=%d running=%v timerSet=%v, want running/true/false",
			int(st), running, timer != nil)
	}
	closeClaimedTask(t, f.sess, cB)

	// 夹具 2：窗口外认领被拒（约 5.1s，计时器已转换到 cancelling/cancelled）。
	r3Setup(t)
	setTestGracePeriod(t, 5*time.Second)
	f2 := setupClaimTask(t, 5*time.Second)
	injectFreshWatchdogSnapshot(t)
	f2.armByDisconnect(t)
	cB2, _ := dialClaimer(t, f2.wsURL, f2.pageID)
	// 快照须在回调前夕新鲜：无 ticker 时武装后注入的快照到 5s 回调时已陈旧
	// 5s（达挂起阈值），回调会 defer 而非取消；故在 4.9s 时重新注入，
	// 回调（5s）读到约 0.1s 的新鲜快照，判定取消。
	time.Sleep(4700 * time.Millisecond)
	injectFreshWatchdogSnapshot(t)
	pollTaskEnded(t, f2.sess, 10*time.Second)
	cB2.claim(f2.taskID, f2.epoch, f2.pageID)
	cB2.waitResumeRejected(10*time.Second, "already-cancelled")
}

// TestT7_32_NoRenewalOnFailedClaim
// 目的：失败的认领不续期：窗口内错误 pageId 认领失败后 disconnectAt 不变；
// 窗口到期后正确认领 → already-cancelled。初始：运行任务，grace 5s，
// watchdog 快照新鲜。并发控制：snapshotDisconnectIdentity 前后对比。
// 捕获：失败认领续期窗口 / 过期后仍可认领。
func TestT7_32_NoRenewalOnFailedClaim(t *testing.T) {
	r3Setup(t)
	setTestGracePeriod(t, 5*time.Second)
	f := setupClaimTask(t, 5*time.Second)
	injectFreshWatchdogSnapshot(t)
	f.armByDisconnect(t)
	_, _, _, at0, _ := snapshotDisconnectIdentity(t, f.sess)
	if at0 == 0 {
		t.Fatal("disconnectAt == 0 after arm, want window start")
	}
	// 预先拨号（正确 pageId 的 B 与错误 pageId 的 C）。
	cB, _ := dialClaimer(t, f.wsURL, f.pageID)
	cC, _ := dialClaimer(t, f.wsURL, "wrong-page")
	// 约 4s 时错误认领失败 → disconnectAt 不变。
	time.Sleep(4 * time.Second)
	cC.claim(f.taskID, f.epoch, "wrong-page")
	cC.waitResumeRejected(10*time.Second, "task-not-found")
	_, _, _, at1, _ := snapshotDisconnectIdentity(t, f.sess)
	if at1 != at0 {
		t.Fatalf("disconnectAt changed after failed claim: %d -> %d, want unchanged", at0, at1)
	}
	// 窗口到期（5s）后正确认领 → already-cancelled。回调前夕重新注入新鲜
	// 快照（同 T7-31 夹具 2 的原因：武装后注入的快照到 5s 回调时已陈旧）。
	injectFreshWatchdogSnapshot(t)
	pollTaskEnded(t, f.sess, 10*time.Second)
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeRejected(10*time.Second, "already-cancelled")
}

// TestT7_33_SlotHeldDuringPendingAndCancelling
// 目的：(a) disconnectPending 期间槽位被占，新任务启动被拒绝；
// (b) cancelling 期间槽位仍被占，新任务启动同样被拒绝；回调释放后任务
// 结束，此后新任务可启动。初始：运行任务。并发控制：直接 beginTask 返回值
// + 回调阶段 3 门。捕获：槽位在待定/取消中提前释放。
func TestT7_33_SlotHeldDuringPendingAndCancelling(t *testing.T) {
	// (a) disconnectPending 期间槽位被占。
	f := setupClaimTask(t, 30*time.Second)
	f.armByDisconnect(t)
	cB, sessB := dialClaimer(t, f.wsURL, f.pageID)
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, ok := sessB.beginTask(cancel, "s7 test task", "official", nil); ok {
		t.Fatal("(a) beginTask on B succeeded while disconnectPending, want rejection (slot held)")
	}
	// 真实 start_task 同样被拒（error 消息）。
	cB.send("start_task", map[string]interface{}{"threads": 1, "port": 443, "delay": 0, "scanMode": "tcping", "ipType": 4})
	cB.waitFor("error", 10*time.Second)
	// 认领成功后清理。
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	closeClaimedTask(t, f.sess, cB)

	// (b) cancelling 期间槽位仍被占。
	r3Setup(t)
	setTestGracePeriod(t, 100*time.Millisecond)
	f2 := setupClaimTask(t, 100*time.Millisecond)
	cbGate := newDisconnectTestGate()
	testHookSet(&testTimerCallbackGate, cbGate)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(cbGate.release) }) }
	t.Cleanup(func() { testHookSet(&testTimerCallbackGate, nil); release() })
	injectFreshWatchdogSnapshot(t)
	f2.armByDisconnect(t)
	select {
	case <-cbGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for testTimerCallbackGate")
	}
	st, _, _ := snapshotDisconnectState(t, f2.sess)
	if st != stateCancelling {
		t.Fatalf("(b) state=%d at callback gate, want cancelling", int(st))
	}
	_, sessB2 := dialClaimer(t, f2.wsURL, f2.pageID)
	_, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	if _, ok := sessB2.beginTask(cancel2, "s7 test task", "official", nil); ok {
		t.Fatal("(b) beginTask on B succeeded while cancelling, want rejection (slot held)")
	}
	// 释放回调 → 任务结束；此后新任务可启动。
	release()
	pollTaskEnded(t, f2.sess, 10*time.Second)
	startSyntheticTask(t, sessB2, longTaskRun)
	pollSessionRunning(t, sessB2, 10*time.Second)
	sessB2.stopTask()
	pollTaskEnded(t, sessB2, 10*time.Second)
}

// TestT7_35_HalfOpenWriteFailure
// 目的：(a) 写失败半开：底层写注入失败 → writeWSMessageLocked 返回 false、
// wsClosed 置位，但 socket 不关闭、读方向仍工作；半开期间认领 → not-pending；
// 关闭 A 后武装，认领成功。(b) E2-only：不 pong（lastPongAtNano=0，E2 为真）
// 但读循环存活，认领 → not-pending；关闭 A 后武装，认领成功。
// 初始：运行任务。并发控制：blockingTestConn failWrites 注入 + 条目 pageId 轮询。
// 捕获：写失败误关闭/误武装 / E2 单独触发认领。
func TestT7_35_HalfOpenWriteFailure(t *testing.T) {
	// (a) 写失败半开。
	r3Setup(t)
	setTestGracePeriod(t, 30*time.Second)
	setTestPingInterval(t, time.Hour)
	_, wsURL, bl := r3ServerBlocking(t)
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register("test-t7-35-pa")
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	// 注入写失败（先注入、后启动任务）。
	bc := bl.lastConn()
	if bc == nil {
		t.Fatal("no blocking conn for A")
	}
	bc.SetFailWrites(true)
	startSyntheticTask(t, sess, longTaskRun)
	pollSessionRunning(t, sess, 10*time.Second)
	taskID := sess.taskIDSnapshot()
	epoch := currentBackendEpoch()
	// 生产完成证据：直接写一条消息，writeWSMessageLocked 必须返回 false。
	sess.wsMutex.Lock()
	ok := sess.writeWSMessageLocked("test_probe", map[string]interface{}{"x": 1})
	sess.wsMutex.Unlock()
	if ok {
		t.Fatal("writeWSMessageLocked returned true with failWrites, want false")
	}
	// wsClosed 已置位。
	pollUntil(t, "wsClosed after write failure", 10*time.Second, 20*time.Millisecond, func() bool {
		_, closed := sess.wsSnapshot()
		return closed
	})
	// 有界窗口（500ms）：任务仍运行、状态 running、计时器 nil、条目未退出、
	// s.ws 仍是 A 的 socket（半开：写失败不关闭、不武装）。
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		st, running, timer := snapshotDisconnectState(t, sess)
		if !running || st != stateRunning || timer != nil {
			t.Fatalf("half-open window: state=%d running=%v timerSet=%v, want running/true/false",
				int(st), running, timer != nil)
		}
		le, ok2 := snapshotLoopState(t, cA.connectionID)
		if !ok2 || le.state == "exited" {
			t.Fatalf("half-open window: loopEntry state=%q, want not exited", le.state)
		}
		conn, _ := sess.wsSnapshot()
		if conn == nil {
			t.Fatal("half-open window: session ws is nil, want A's socket")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// 读方向证明：A 发 register_page（P2），服务端读到并处理（条目 pageId 变为
	// P2）；再发回 P_A（认领的 pageId 过滤须用 P_A）。ack 因 wsClosed 被丢弃，
	// 故不用 register() 等 ack，改为轮询条目 pageId。
	cA.send("register_page", map[string]interface{}{"pageId": "test-t7-35-p2"})
	pollUntil(t, "loopEntry pageId == P2", 10*time.Second, 20*time.Millisecond, func() bool {
		le, ok2 := snapshotLoopState(t, cA.connectionID)
		return ok2 && le.pageId == "test-t7-35-p2"
	})
	cA.send("register_page", map[string]interface{}{"pageId": "test-t7-35-pa"})
	pollUntil(t, "loopEntry pageId == P_A", 10*time.Second, 20*time.Millisecond, func() bool {
		le, ok2 := snapshotLoopState(t, cA.connectionID)
		return ok2 && le.pageId == "test-t7-35-pa"
	})
	// 半开期间认领 → not-pending（任务在原始 socket 上，C4 有界等待）。
	cB, _ := dialClaimer(t, wsURL, "test-t7-35-pa")
	cB.claim(taskID, epoch, "test-t7-35-pa")
	cB.waitResumeRejected(10*time.Second, "not-pending")
	// 关闭 A → 读循环错误 → 武装 → 认领成功。
	cA.Close()
	pollDisconnectState(t, sess, stateDisconnectPending, 10*time.Second)
	cB.claim(taskID, epoch, "test-t7-35-pa")
	cB.waitResumeAccepted(10*time.Second, taskID, epoch)
	cB.send("stop_task", nil)
	pollTaskEnded(t, sess, 10*time.Second)

	// (b) E2-only：不启动 ponger（lastPongAtNano 保持 0，E2 为真），读循环仍存活。
	r3Setup(t)
	setTestGracePeriod(t, 30*time.Second)
	f := setupClaimTask(t, 30*time.Second)
	// A 仍可正常处理消息（重注册同一 pageId，ack 正常到达）。
	f.cA.register(f.pageID)
	// 认领 → not-pending（E2 单独不触发认领）。
	cB2, _ := dialClaimer(t, f.wsURL, f.pageID)
	cB2.claim(f.taskID, f.epoch, f.pageID)
	cB2.waitResumeRejected(10*time.Second, "not-pending")
	// 关闭 A → 武装 → 认领成功。
	f.armByDisconnect(t)
	cB2.claim(f.taskID, f.epoch, f.pageID)
	cB2.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	cB2.send("stop_task", nil)
	pollTaskEnded(t, f.sess, 10*time.Second)
	// 收尾：关闭两部分的认领连接并等服务端循环退出，避免测试结束的
	// cleanup（恢复 wsPingInterval）与 ping goroutine 的启动读竞争（-race）。
	// cA（a）与 f.cA（b）已在武装时关闭；此处关闭 B 与 B2。
	cB.Close()
	cB2.Close()
	// 只等 (b) 的 B2：(a) 的 cB 条目已被 (b) 的 r3Setup/reset 删除，
	// pollLoopExited 会永远超时；(a) 的连接早已随 reset 关闭而退出。
	pollLoopExited(t, cB2.connectionID, 10*time.Second)
}

// TestT7_36_PageIdMismatch
// 目的：(a1) 不同 pageId 的认领 → task-not-found（不泄露任务存在），A 的
// socket 未关闭；(a2) 任务创建后重注册 → taskPageId 不变，以新 pageId 认领
// → not-owner；(b) 健康连接（ponging）+ 认领 → not-pending，A 不受影响。
// 初始：运行任务。并发控制：条目 pageId 轮询 + taskPageId 快照。捕获：跨页
// 任务存在泄露 / taskPageId 随重注册漂移 / 健康任务被误认领。
func TestT7_36_PageIdMismatch(t *testing.T) {
	// (a1) 不同 pageId 的认领 → task-not-found，A 的 socket 未关闭。
	f := setupClaimTask(t, 30*time.Second)
	cB, _ := dialClaimer(t, f.wsURL, "other-page")
	cB.claim(f.taskID, f.epoch, "other-page")
	cB.waitResumeRejected(10*time.Second, "task-not-found")
	// A 的 socket 未关闭：仍可正常处理消息（register ack 到达）。
	f.cA.register(f.pageID)
	le, ok := snapshotLoopState(t, f.cA.connectionID)
	if !ok || le.state == "exited" {
		t.Fatalf("(a1) A loopEntry state=%q, want not exited", le.state)
	}
	f.sess.stopTask()
	pollTaskEnded(t, f.sess, 10*time.Second)

	// (a2) 任务创建后重注册 → taskPageId 不变，以新 pageId 认领 → not-owner。
	r3Setup(t)
	f2 := setupClaimTask(t, 30*time.Second)
	f2.cA.register("test-t7-36-p2")
	if got := snapshotTaskPageId(t, f2.sess); got != f2.pageID {
		t.Fatalf("(a2) taskPageId = %q after re-register, want %q (must not change)", got, f2.pageID)
	}
	cB2, _ := dialClaimer(t, f2.wsURL, "test-t7-36-p2")
	cB2.claim(f2.taskID, f2.epoch, "test-t7-36-p2")
	cB2.waitResumeRejected(10*time.Second, "not-owner")
	f2.sess.stopTask()
	pollTaskEnded(t, f2.sess, 10*time.Second)

	// (b) 健康连接（ponging）+ 认领 → not-pending，A 不受影响。
	r3Setup(t)
	f3 := setupClaimTask(t, 30*time.Second)
	startPonger(t, f3.cA)
	cB3, _ := dialClaimer(t, f3.wsURL, f3.pageID)
	cB3.claim(f3.taskID, f3.epoch, f3.pageID)
	cB3.waitResumeRejected(10*time.Second, "not-pending")
	f3.cA.register(f3.pageID)
	st, running, timer := snapshotDisconnectState(t, f3.sess)
	if !running || st != stateRunning || timer != nil {
		t.Fatalf("(b) healthy A after rejected claim: state=%d running=%v timerSet=%v, want running/true/false",
			int(st), running, timer != nil)
	}
	f3.sess.stopTask()
	pollTaskEnded(t, f3.sess, 10*time.Second)
}

// TestT7_37_WriteFailureNoCloseAndNilWS
// 目的：(a) 写失败不关闭：socket 未关闭、条目未退出、s.ws 不变、读方向仍工作
// （与 T7-35(a) 同一注入，只断言不关闭，不走到认领）；(b) ws==nil 的运行
// 任务 → not-pending（防御分支，直接 in-package 核验）。
// (c) owner-claim 断言已并入 T7-47(c)，此处不重复。
// 初始：运行任务。并发控制：条目 pageId 轮询 + 直接核验。捕获：写失败误关闭
// / ws==nil 被误判为 already-claimed。
func TestT7_37_WriteFailureNoCloseAndNilWS(t *testing.T) {
	// (a) 写失败不关闭。
	r3Setup(t)
	setTestGracePeriod(t, 30*time.Second)
	setTestPingInterval(t, time.Hour)
	_, wsURL, bl := r3ServerBlocking(t)
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register("test-t7-37-pa")
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	bc := bl.lastConn()
	if bc == nil {
		t.Fatal("no blocking conn for A")
	}
	bc.SetFailWrites(true)
	startSyntheticTask(t, sess, longTaskRun)
	pollSessionRunning(t, sess, 10*time.Second)
	sess.wsMutex.Lock()
	ok := sess.writeWSMessageLocked("test_probe", map[string]interface{}{"x": 1})
	sess.wsMutex.Unlock()
	if ok {
		t.Fatal("writeWSMessageLocked returned true with failWrites, want false")
	}
	pollUntil(t, "wsClosed after write failure", 10*time.Second, 20*time.Millisecond, func() bool {
		_, closed := sess.wsSnapshot()
		return closed
	})
	// socket 未关闭：底层连接仍开、条目未退出、s.ws 仍是 A 的 socket。
	conn, _ := sess.wsSnapshot()
	if conn == nil {
		t.Fatal("(a) session ws is nil after write failure, want A's socket")
	}
	le, ok2 := snapshotLoopState(t, cA.connectionID)
	if !ok2 || le.state == "exited" {
		t.Fatalf("(a) loopEntry state=%q, want not exited", le.state)
	}
	if le.ws != conn {
		t.Fatal("(a) loopEntry ws != session ws after write failure")
	}
	// 读方向：发 register_page（P2），服务端读到并处理。
	cA.send("register_page", map[string]interface{}{"pageId": "test-t7-37-p2"})
	pollUntil(t, "loopEntry pageId == P2", 10*time.Second, 20*time.Millisecond, func() bool {
		le2, ok3 := snapshotLoopState(t, cA.connectionID)
		return ok3 && le2.pageId == "test-t7-37-p2"
	})
	sess.stopTask()
	pollTaskEnded(t, sess, 10*time.Second)

	// (b) ws==nil 的运行任务 → not-pending（防御分支）。
	r3Setup(t)
	f := setupClaimTask(t, 30*time.Second)
	cB, sessB := dialClaimer(t, f.wsURL, f.pageID)
	bConn, _ := sessB.wsSnapshot()
	if bConn == nil {
		t.Fatal("(b) B server conn is nil")
	}
	// 把任务 session 的 ws 置 nil（模拟从未绑定/已被清除的防御情形）。
	oldConn, _ := f.sess.wsSnapshot()
	f.sess.wsMutex.Lock()
	f.sess.ws = nil
	f.sess.wsMutex.Unlock()
	backgroundTaskMutex.Lock()
	f.sess.taskMutex.Lock()
	v := verifyClaimLocked(f.sess, claimRequest{TaskID: f.taskID, BackendEpoch: f.epoch, PageID: f.pageID}, bConn)
	f.sess.taskMutex.Unlock()
	backgroundTaskMutex.Unlock()
	if v.action != claimActionReject || v.reason != "not-pending" {
		t.Fatalf("(b) ws==nil claim: action=%q reason=%q, want reject/not-pending", v.action, v.reason)
	}
	// 恢复 ws，避免影响清理；取消任务。
	f.sess.wsMutex.Lock()
	f.sess.ws = oldConn
	f.sess.wsMutex.Unlock()
	f.sess.stopTask()
	pollTaskEnded(t, f.sess, 10*time.Second)
	// 收尾：关闭两部分的连接并等服务端循环退出，避免测试结束的 cleanup
	// （恢复 wsPingInterval）与 ping goroutine 的启动读竞争（-race）。
	// (a) 的 cA 在任务取消后仍开着；(b) 的 f.cA 与 cB 同样仍开着。
	cA.Close()
	f.cA.Close()
	cB.Close()
	// 只等 (b) 的 f.cA 与 cB：(a) 的 cA 条目已被 (b) 的 r3Setup/reset 删除，
	// pollLoopExited 会永远超时；(a) 的连接早已随 reset 关闭而退出。
	pollLoopExited(t, f.cA.connectionID, 10*time.Second)
	pollLoopExited(t, cB.connectionID, 10*time.Second)
}

// TestT7_38_NaturalDetectionC4
// 目的：(a) 读超时自然检测（C4）：override 在拨号前设置；B 的首次认领 →
// not-pending；超时武装后认领成功。(b) 关闭 socket 自然检测：同上，
// 以关闭 socket 触发。初始：运行任务。并发控制：读超时轮询 / 关闭轮询。
// 捕获：C4 自然检测未触发 / 超时前误认领。
func TestT7_38_NaturalDetectionC4(t *testing.T) {
	// (a) 读超时自然检测。
	r3Setup(t)
	setTestGracePeriod(t, 30*time.Second)
	setReadDeadlineOverride(t, 2*time.Second)
	_, wsURL := r3Server(t)
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register("test-t7-38-pa")
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	startSyntheticTask(t, sess, longTaskRun)
	pollSessionRunning(t, sess, 10*time.Second)
	taskID := sess.taskIDSnapshot()
	epoch := currentBackendEpoch()
	// B1 的首次认领（超时尚未发生）→ not-pending。注意：读超时 override
	// 是全局的，B1 自己的读截止也在拨号后约 2s 到期，故 B1 只用于超时前的
	// 首次认领；超时后的成功认领改用超时后才拨号的 B2（其截止在拨号后 2s，
	// 认领在几十毫秒内完成）。
	cB1, _ := dialClaimer(t, wsURL, "test-t7-38-pa")
	cB1.claim(taskID, epoch, "test-t7-38-pa")
	cB1.waitResumeRejected(10*time.Second, "not-pending")
	// 读超时（约 2s）→ 武装 → B2（超时后拨号）认领成功。
	pollDisconnectState(t, sess, stateDisconnectPending, 10*time.Second)
	cB2, _ := dialClaimer(t, wsURL, "test-t7-38-pa")
	cB2.claim(taskID, epoch, "test-t7-38-pa")
	cB2.waitResumeAccepted(10*time.Second, taskID, epoch)
	closeClaimedTask(t, sess, cB2)

	// (b) 关闭 socket 自然检测。
	r3Setup(t)
	f := setupClaimTask(t, 30*time.Second)
	cB3, _ := dialClaimer(t, f.wsURL, f.pageID)
	cB3.claim(f.taskID, f.epoch, f.pageID)
	cB3.waitResumeRejected(10*time.Second, "not-pending")
	f.armByDisconnect(t)
	cB3.claim(f.taskID, f.epoch, f.pageID)
	cB3.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	closeClaimedTask(t, f.sess, cB3)
}

// TestT7_44_ClaimThenLateRow
// 目的：认领后任务继续，行到达新连接；旧 Defer A 释放后不得杀死已恢复的任务。
// 初始：运行任务，Defer A 暂停在入口门。并发控制：testDeferAEntryGate。
// 捕获：认领后任务行仍发往旧连接 / 旧 Defer A 误杀已恢复任务。
func TestT7_44_ClaimThenLateRow(t *testing.T) {
	f := setupClaimTask(t, 30*time.Second)
	deferGate := newDisconnectTestGate()
	testHookSet(&testDeferAEntryGate, deferGate)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(deferGate.release) }) }
	t.Cleanup(func() { testHookSet(&testDeferAEntryGate, nil); release() })
	f.armByDisconnect(t)
	select {
	case <-deferGate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for testDeferAEntryGate")
	}
	// 门内认领成功。
	cB, _ := dialClaimer(t, f.wsURL, f.pageID)
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	// 任务继续：经当前日志发一行，必须到达新连接 B。
	ctx := withTaskLog(context.Background(), f.sess.resultLog.Load())
	f.sess.sendTaskResult(ctx, "test_result", map[string]interface{}{"row": 1})
	msg := cB.waitFor("test_result", 10*time.Second)
	data, _ := msg["data"].(map[string]interface{})
	if got, _ := data["row"].(float64); got != 1 {
		t.Fatalf("late row on new conn = %v, want 1", got)
	}
	// 释放旧 Defer A：不得杀死已恢复的任务。
	release()
	pollLoopExited(t, f.cA.connectionID, 10*time.Second)
	st, running, timer := snapshotDisconnectState(t, f.sess)
	if !running || st != stateRunning || timer != nil {
		t.Fatalf("after Defer A released: state=%d running=%v timerSet=%v, want running/true/false",
			int(st), running, timer != nil)
	}
	closeClaimedTask(t, f.sess, cB)
}

// TestT7_08_BoundaryRaceSmoke
// 目的：边界竞态冒烟：短 grace（50ms）下武装后立即认领（无门同步，自然竞态）；
// 循环 10 轮，每轮胜者唯一且结果一致（认领胜 → running + resume_accepted；
// 计时器胜 → cancelled + already-cancelled）。-count=10 冒烟时共 100 轮。
// 初始：每轮新建运行任务，grace 50ms，watchdog 快照新鲜。
// 并发控制：无门，自然竞态（与 T7-21 的门同步互补）。捕获：双重认领 /
// 终态回退 / 响应与最终状态不一致。
func TestT7_08_BoundaryRaceSmoke(t *testing.T) {
	for round := 0; round < 10; round++ {
		r3Setup(t)
		setTestGracePeriod(t, 50*time.Millisecond)
		f := setupClaimTask(t, 50*time.Millisecond)
		injectFreshWatchdogSnapshot(t)
		f.armByDisconnect(t)
		// 立即认领（计时器约 50ms 后到期，自然竞态）。
		cB, _ := dialClaimer(t, f.wsURL, f.pageID)
		cB.claim(f.taskID, f.epoch, f.pageID)
		typ, msg := cB.waitForAny(10*time.Second, "resume_accepted", "resume_rejected")
		if typ == "resume_accepted" {
			st, running, timer := snapshotDisconnectState(t, f.sess)
			if !running || st != stateRunning || timer != nil {
				t.Fatalf("round %d: claim accepted but state=%d running=%v timerSet=%v",
					round, int(st), running, timer != nil)
			}
			closeClaimedTask(t, f.sess, cB)
		} else {
			data, _ := msg["data"].(map[string]interface{})
			reason, _ := data["reason"].(string)
			if reason != "already-cancelled" {
				t.Fatalf("round %d: claim rejected with reason %q, want already-cancelled", round, reason)
			}
			pollTaskEnded(t, f.sess, 10*time.Second)
			st, _, _ := snapshotDisconnectState(t, f.sess)
			if st != stateCancelled {
				t.Fatalf("round %d: claim rejected already-cancelled but final state=%d, want cancelled",
					round, int(st))
			}
		}
	}
}

// ===== S7 C2: 前台任务重放查找（S6 A lookup 扩展）测试 =====
// 依据：C2 实施前审查报告 §9.2 测试矩阵（修订 2 版）。
// 夹具说明：setupClaimTask 用 longTaskRun（任务只在取消时结束，终态 cancelled）；
// 可自然完成的测试改用 c2SetupCompletableTask（run 阻塞于 done 通道，
// 关闭 done 即自然结束，终态 completed）。两者不得混用结束方式。

// c2ProduceRows 在任务日志中产生 n 行（seq 从 from 开始编号；调用方保证
// 任务正在运行或处于断线窗口——写失败不回滚，仍入日志）。
func c2ProduceRows(t *testing.T, sess *appSession, from, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		sess.sendTaskResult(t6Ctx(sess), "scan_result", ScanResult{IP: fmt.Sprintf("9.9.9.%d", from+i), Port: 443})
	}
	if got := t6LogLen(sess.resultLog.Load()); got != from+n-1 {
		t.Fatalf("produceRows 后日志行数 = %d, want %d", got, from+n-1)
	}
}

// c2AssertRows 断言读端按序收到 want 的行（不多、不少、不乱序、不重复）。
func c2AssertRows(t *testing.T, r *t6Reader, want []int64) {
	t.Helper()
	got := r.collectRows("scan_result", len(want), 10*time.Second)
	if len(got) != len(want) {
		t.Fatalf("收到 seq = %v, want %v", got, want)
	}
	for i, seq := range want {
		if got[i] != seq {
			t.Fatalf("第 %d 行 seq = %d, want %d", i+1, got[i], seq)
		}
	}
}

// c2ReplaySuccess：指定客户端发 replay_results(lastSeq)，断言收到 wantSeq
// 的行与 replay_complete（through/final）。
func c2ReplaySuccess(t *testing.T, r *t6Reader, c *r3Client, taskID, epoch string, lastSeq int64, reqID string, wantSeq []int64, wantThrough int64, wantFinal bool) {
	t.Helper()
	t6SendReplay(c, taskID, epoch, lastSeq, reqID)
	c2AssertRows(t, r, wantSeq)
	m := r.waitFor("replay_complete", 10*time.Second)
	d := msgData(m)
	if rt, _ := d["replayThroughSeq"].(float64); int64(rt) != wantThrough {
		t.Fatalf("replay_complete.replayThroughSeq = %v, want %d", rt, wantThrough)
	}
	if fin, _ := d["final"].(bool); fin != wantFinal {
		t.Fatalf("replay_complete.final = %v, want %v", fin, wantFinal)
	}
}

// c2AssertRejected：指定客户端发 replay_results(lastSeq=0)，断言在 10s 内
// 收到 replay_rejected 且理由为 want；随后 2s 内不得收到任何 scan_result 行。
// 调用方保证该客户端无未消费的 t6Reader pending 行（否则原始通道读不到
// 已缓冲的行，断言失效）。
func c2AssertRejected(t *testing.T, c *r3Client, taskID, epoch, reqID, want string) {
	t.Helper()
	t6SendReplay(c, taskID, epoch, 0, reqID)
	msg := c.waitFor("replay_rejected", 10*time.Second)
	if got, _ := msgData(msg)["reason"].(string); got != want {
		t.Fatalf("replay_rejected reason = %q, want %q", got, want)
	}
	c.waitForAbsent("scan_result", 2*time.Second)
}

// c2SetupClaimedTask 建立 C2 夹具：A 上运行任务（longTaskRun）→ 断线武装
// → 窗口内产生 5 行 → B 认领成功。返回夹具与认领者客户端。
func c2SetupClaimedTask(t *testing.T) (*claimFixture, *r3Client) {
	t.Helper()
	f := setupClaimTask(t, 30*time.Second)
	f.armByDisconnect(t)
	c2ProduceRows(t, f.sess, 1, 5)
	cB, _ := dialClaimer(t, f.wsURL, f.pageID)
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	return f, cB
}

// c2SetupCompletableTask 建立可自然完成的 C2 夹具：任务的 run 函数阻塞于
// done 通道；测试关闭 done 即让任务自然结束（state → completed，非取消）。
// 返回夹具与 done 通道。调用方不得用 stopTask 结束本夹具的任务
// （run 不监听 ctx，stopTask 无法让它返回）；清理时由 t.Cleanup 关 done。
func c2SetupCompletableTask(t *testing.T) (*claimFixture, chan struct{}) {
	t.Helper()
	r3Setup(t)
	setTestGracePeriod(t, 30*time.Second)
	_, wsURL := r3Server(t)
	pageID := "test-claim-page"
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register(pageID)
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	done := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-done:
		default:
			close(done)
		}
	})
	startSyntheticTask(t, sess, func(ctx context.Context, s *appSession) { <-done })
	pollSessionRunning(t, sess, 10*time.Second)
	return &claimFixture{wsURL: wsURL, cA: cA, sess: sess, taskID: sess.taskIDSnapshot(), epoch: currentBackendEpoch(), pageID: pageID}, done
}

// c2SetupClaimedCompletableTask：可自然完成版的 c2SetupClaimedTask
// （断线武装 → 窗口内 5 行 → B 认领成功）。返回夹具、认领者与 done 通道。
func c2SetupClaimedCompletableTask(t *testing.T) (*claimFixture, *r3Client, chan struct{}) {
	t.Helper()
	f, done := c2SetupCompletableTask(t)
	f.armByDisconnect(t)
	c2ProduceRows(t, f.sess, 1, 5)
	cB, _ := dialClaimer(t, f.wsURL, f.pageID)
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	return f, cB, done
}

// c2CompleteTask 让可自然完成的任务结束：关闭 done → run 返回 → endTask；
// 核验任务结束且 state == completed。
func c2CompleteTask(t *testing.T, f *claimFixture, done chan struct{}) {
	t.Helper()
	close(done)
	pollTaskEnded(t, f.sess, 10*time.Second)
	st, _, _ := snapshotDisconnectState(t, f.sess)
	if st != stateCompleted {
		t.Fatalf("任务自然结束后 state = %d, want completed", int(st))
	}
}

// c2CancelTaskViaStopTask：认领者 B 发 stop_task 取消已认领的任务；
// 核验任务结束且 state == cancelled。
func c2CancelTaskViaStopTask(t *testing.T, f *claimFixture, cB *r3Client) {
	t.Helper()
	cB.send("stop_task", nil)
	pollTaskEnded(t, f.sess, 10*time.Second)
	st, _, _ := snapshotDisconnectState(t, f.sess)
	if st != stateCancelled {
		t.Fatalf("stop_task 后 state = %d, want cancelled", int(st))
	}
}

// c2SetExitedAt 调整指定连接 loopEntry 的 exitedAt（仅测试替身时间字段，
// 相对 now 的偏移 d；d 为负表示过去）。
func c2SetExitedAt(t *testing.T, connID string, d time.Duration) {
	t.Helper()
	backgroundTaskMutex.Lock()
	defer backgroundTaskMutex.Unlock()
	le, ok := connLoopState[connID]
	if !ok {
		t.Fatalf("loopEntry %s missing", connID)
	}
	le.exitedAt = time.Now().Add(d)
	connLoopState[connID] = le
}

// c2SetFrozenAt 调整任务日志的 frozenAt（仅测试替身时间字段，相对 now 的偏移 d）。
func c2SetFrozenAt(t *testing.T, sess *appSession, d time.Duration) {
	t.Helper()
	log := sess.resultLog.Load()
	if log == nil {
		t.Fatal("resultLog is nil")
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	log.frozenAt = time.Now().Add(d)
}

// startBackgroundTaskForC2 在当前代次内直接启动一个后台任务（不调 r3Setup，
// 以免重置已建立的认领夹具）。调用时当前代次不得有运行任务（前台任务须已完成）。
func startBackgroundTaskForC2(t *testing.T, connID string) *appSession {
	t.Helper()
	bg := &appSession{connectionID: connID, backendGeneration: atomic.LoadUint64(&backendGeneration)}
	_, cancel := context.WithCancel(context.Background())
	if _, ok := bg.beginTask(cancel, "c2 bg task", "official", nil); !ok {
		t.Fatal("beginTask for background task failed")
	}
	if !bg.enableBackgroundTask() {
		t.Fatal("enableBackgroundTask failed")
	}
	t.Cleanup(func() {
		cancel()
		bg.endTask()
		clearCompletedBackgroundTaskSession()
	})
	return bg
}

// TestT7_10_ClaimThenReplay（修订版）：认领后重放断线期间的 5 行。
func TestT7_10_ClaimThenReplay(t *testing.T) {
	f, cB := c2SetupClaimedTask(t)
	rB := t6NewReader(t, cB)
	c2ReplaySuccess(t, rB, cB, f.taskID, f.epoch, 0, "req-t7-10", []int64{1, 2, 3, 4, 5}, 5, false)
	closeClaimedTask(t, f.sess, cB)
}

// TestT7_42_ClaimReplayPartial（修订版）：断线前 live 收到 1..2，窗口内产生
// 3..5，认领后以 lastSeq=2 重放，只补发 3..5。
func TestT7_42_ClaimReplayPartial(t *testing.T) {
	f := setupClaimTask(t, 30*time.Second)
	rA := t6NewReader(t, f.cA)
	c2ProduceRows(t, f.sess, 1, 2)
	c2AssertRows(t, rA, []int64{1, 2})
	f.armByDisconnect(t)
	c2ProduceRows(t, f.sess, 3, 3)
	cB, _ := dialClaimer(t, f.wsURL, f.pageID)
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	rB := t6NewReader(t, cB)
	c2ReplaySuccess(t, rB, cB, f.taskID, f.epoch, 2, "req-t7-42", []int64{3, 4, 5}, 5, false)
	closeClaimedTask(t, f.sess, cB)
}

// TestT7_43_ReplayTaskSwitchMidflight（修订版）：重放进行中旧任务结束、
// 新任务启动，旧重放中止（无 replay_complete），新任务日志全新。
func TestT7_43_ReplayTaskSwitchMidflight(t *testing.T) {
	f, cB, done := c2SetupClaimedCompletableTask(t)
	oldCtx := t6Ctx(f.sess) // 绑定旧日志的 ctx（滞留 worker 用）
	oldTaskID := f.taskID
	rB := t6NewReader(t, cB)
	gate := &replayPauseGate{afterRows: 3, entered: make(chan struct{}), release: make(chan struct{})}
	testReplayPauseGate = gate
	t.Cleanup(func() { testReplayPauseGate = nil })
	t6SendReplay(cB, oldTaskID, f.epoch, 0, "req-t7-43")
	select {
	case <-gate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("重放未在 10s 内到达第 3 行暂停点")
	}
	c2AssertRows(t, rB, []int64{1, 2, 3})
	// 旧任务自然结束（close(done) → run 返回 → endTask 冻结日志），
	// 随后同一 session 启动新任务 B（新日志、新 seq 空间）。
	close(done)
	pollTaskEnded(t, f.sess, 10*time.Second)
	startSyntheticTask(t, f.sess, longTaskRun)
	pollSessionRunning(t, f.sess, 10*time.Second)
	newTaskID := f.sess.taskIDSnapshot()
	if newTaskID == oldTaskID {
		t.Fatalf("新任务 taskID 与旧任务相同：%q", newTaskID)
	}
	if logB := f.sess.resultLog.Load(); logB == nil || t6LogLen(logB) != 0 || logB.nextSeq != 1 {
		t.Fatal("新任务日志初始状态异常（应为空日志、nextSeq 从 1 开始）")
	}
	// 恢复旧重放：应立即中止，不再发送 A 的后续行与完成消息。
	close(gate.release)
	if _, ok := cB.waitForTimeout("replay_complete", 2*time.Second); ok {
		t.Fatal("旧重放恢复后收到 replay_complete，want 无")
	}
	// 新任务 B 的 live 行 seq 从 1 开始（旧任务 A 的行未污染新日志）。
	f.sess.sendTaskResult(t6Ctx(f.sess), "scan_result", ScanResult{IP: "5.5.5.1", Port: 443})
	c2AssertRows(t, rB, []int64{1})
	// 旧任务的滞留 worker（旧 ctx）迟到发送：被日志身份校验丢弃，B 收不到新行。
	// （A 的原连接已关闭，无法再发 WS 帧；这是测试内唯一可行的迟到发送路径。）
	f.sess.sendTaskResult(oldCtx, "scan_result", ScanResult{IP: "6.6.6.1", Port: 443})
	cB.waitForAbsent("scan_result", 2*time.Second)
	closeClaimedTask(t, f.sess, cB)
}

// TestT7_48_CompletedTaskReplay：已完成任务（冻结日志、TTL 内）可重放，final=true。
func TestT7_48_CompletedTaskReplay(t *testing.T) {
	f, cB, done := c2SetupClaimedCompletableTask(t)
	c2CompleteTask(t, f, done)
	rB := t6NewReader(t, cB)
	c2ReplaySuccess(t, rB, cB, f.taskID, f.epoch, 0, "req-t7-48", []int64{1, 2, 3, 4, 5}, 5, true)
}

// TestT7_49_CancelledTaskReplayRejected：已取消任务不可重放 → task-not-found。
func TestT7_49_CancelledTaskReplayRejected(t *testing.T) {
	f, cB := c2SetupClaimedTask(t)
	c2CancelTaskViaStopTask(t, f, cB)
	c2AssertRejected(t, cB, f.taskID, f.epoch, "req-t7-49", "task-not-found")
}

// TestT7_50_EpochMismatchAfterReset：代次切换后旧 epoch 被拒绝。
// （a）白盒：对 reset 前取得的 session 直接调 verifyIdentity——代次已变，
// epoch 检查先于日志检查，应回 epoch-mismatch。这是唯一能证明 epoch 比对
// 的路径：reset 已清空 connLoopState（2c）并关闭旧连接（3a），WS 路径上
// 候选已不存在，只能回 task-not-found（见 (b)）。矩阵初版的 WS 路径
// epoch-mismatch 预期据此修正（见实施报告 §5.3）。
// （b）WS 路径（reset 后的真实行为）：新连接发旧身份的重放 → task-not-found。
func TestT7_50_EpochMismatchAfterReset(t *testing.T) {
	f, _ := c2SetupClaimedTask(t)
	oldTaskID, oldEpoch := f.taskID, f.epoch
	resetBackendGeneration()
	f.sess.taskMutex.Lock()
	matched, reason := verifyIdentity(f.sess, oldTaskID, oldEpoch, f.pageID)
	f.sess.taskMutex.Unlock()
	if !matched || reason != "epoch-mismatch" {
		t.Fatalf("verifyIdentity after reset = (%v, %q), want (true, epoch-mismatch)", matched, reason)
	}
	cC, _ := dialClaimer(t, f.wsURL, f.pageID)
	c2AssertRejected(t, cC, oldTaskID, oldEpoch, "req-t7-50-ws", "task-not-found")
}

// TestT7_51_HalfOpenNoReplayBeforeClaim：半开状态下认领前重放不产生任何行；
// 认领成功后重放成功。
func TestT7_51_HalfOpenNoReplayBeforeClaim(t *testing.T) {
	f := setupClaimTask(t, 30*time.Second)
	f.armByDisconnect(t)
	c2ProduceRows(t, f.sess, 1, 5)
	cB, _ := dialClaimer(t, f.wsURL, f.pageID)
	c2AssertRejected(t, cB, f.taskID, f.epoch, "req-t7-51-pre", "task-not-found")
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	rB := t6NewReader(t, cB)
	c2ReplaySuccess(t, rB, cB, f.taskID, f.epoch, 0, "req-t7-51-post", []int64{1, 2, 3, 4, 5}, 5, false)
	closeClaimedTask(t, f.sess, cB)
}

// TestT7_C2_01_BackgroundMismatchFallsThrough（D-3）：后台任务 taskId 不匹配
// 时，前台候选仍可被查找并重放成功。
func TestT7_C2_01_BackgroundMismatchFallsThrough(t *testing.T) {
	f, cB, done := c2SetupClaimedCompletableTask(t)
	c2CompleteTask(t, f, done)
	_ = startBackgroundTaskForC2(t, "c2-01-bg")
	if bg := currentBackgroundTaskSession(); bg == nil || bg.taskIDSnapshot() == f.taskID {
		t.Fatal("后台任务未就绪或 taskId 与前台任务相同（夹具失效）")
	}
	rB := t6NewReader(t, cB)
	c2ReplaySuccess(t, rB, cB, f.taskID, f.epoch, 0, "req-c2-01", []int64{1, 2, 3, 4, 5}, 5, true)
}

// TestT7_C2_02a_CaseA：情形 A（T0=now−6min，T1=now−3min）——D-1 保留条目，重放成功。
func TestT7_C2_02a_CaseA(t *testing.T) {
	f, cB, done := c2SetupClaimedCompletableTask(t)
	c2CompleteTask(t, f, done)
	c2SetExitedAt(t, f.cA.connectionID, -6*time.Minute)
	c2SetFrozenAt(t, f.sess, -3*time.Minute)
	backgroundTaskMutex.Lock()
	sweepOnceLocked()
	backgroundTaskMutex.Unlock()
	if _, ok := snapshotLoopState(t, f.cA.connectionID); !ok {
		t.Fatal("情形 A：A 的 loopEntry 被 sweep 删除，want 保留（D-1）")
	}
	rB := t6NewReader(t, cB)
	c2ReplaySuccess(t, rB, cB, f.taskID, f.epoch, 0, "req-02a", []int64{1, 2, 3, 4, 5}, 5, true)
}

// TestT7_C2_02b_CaseB：情形 B（T0=now−6min，T1≈now，刚完成）——D-1 保留条目，重放成功。
func TestT7_C2_02b_CaseB(t *testing.T) {
	f, cB, done := c2SetupClaimedCompletableTask(t)
	c2CompleteTask(t, f, done)
	c2SetExitedAt(t, f.cA.connectionID, -6*time.Minute)
	backgroundTaskMutex.Lock()
	sweepOnceLocked()
	backgroundTaskMutex.Unlock()
	if _, ok := snapshotLoopState(t, f.cA.connectionID); !ok {
		t.Fatal("情形 B：A 的 loopEntry 被 sweep 删除，want 保留（D-1）")
	}
	rB := t6NewReader(t, cB)
	c2ReplaySuccess(t, rB, cB, f.taskID, f.epoch, 0, "req-02b", []int64{1, 2, 3, 4, 5}, 5, true)
}

// TestT7_C2_02c_CancelledNotRetained：已取消任务（日志已冻结且在 TTL 内，
// 仅 state 阻止保留）——条目被删，重放 task-not-found。
func TestT7_C2_02c_CancelledNotRetained(t *testing.T) {
	f, cB := c2SetupClaimedTask(t)
	c2CancelTaskViaStopTask(t, f, cB)
	c2SetExitedAt(t, f.cA.connectionID, -6*time.Minute)
	c2SetFrozenAt(t, f.sess, -1*time.Minute)
	backgroundTaskMutex.Lock()
	sweepOnceLocked()
	backgroundTaskMutex.Unlock()
	if _, ok := snapshotLoopState(t, f.cA.connectionID); ok {
		t.Fatal("情形 C：A 的 loopEntry 被 sweep 保留，want 删除（cancelled 不得保留）")
	}
	c2AssertRejected(t, cB, f.taskID, f.epoch, "req-02c", "task-not-found")
}

// TestT7_C2_02d_NilLogNotRetained：已完成但日志指针为 nil——条目被删，重放 task-not-found。
func TestT7_C2_02d_NilLogNotRetained(t *testing.T) {
	f, cB, done := c2SetupClaimedCompletableTask(t)
	c2CompleteTask(t, f, done)
	f.sess.resultLog.Store(nil)
	c2SetExitedAt(t, f.cA.connectionID, -6*time.Minute)
	backgroundTaskMutex.Lock()
	sweepOnceLocked()
	backgroundTaskMutex.Unlock()
	if _, ok := snapshotLoopState(t, f.cA.connectionID); ok {
		t.Fatal("情形 D：A 的 loopEntry 被 sweep 保留，want 删除（日志为 nil 不得保留）")
	}
	c2AssertRejected(t, cB, f.taskID, f.epoch, "req-02d", "task-not-found")
}

// TestT7_C2_02e_UnfrozenNotRetained：不一致态（isTaskRunning=false +
// state=completed，但日志未冻结）——条目被删，重放 task-not-found。
// 生产代码中 endTask 先冻结后转状态，不可能出现此状态；本测试只验证
// D-1 守卫对 !frozen 的防御分支。
func TestT7_C2_02e_UnfrozenNotRetained(t *testing.T) {
	f, cB := c2SetupClaimedTask(t)
	f.sess.taskMutex.Lock()
	f.sess.isTaskRunning = false
	f.sess.disconnectState = stateCompleted
	f.sess.taskMutex.Unlock()
	c2SetExitedAt(t, f.cA.connectionID, -6*time.Minute)
	backgroundTaskMutex.Lock()
	sweepOnceLocked()
	backgroundTaskMutex.Unlock()
	if _, ok := snapshotLoopState(t, f.cA.connectionID); ok {
		t.Fatal("情形 E：A 的 loopEntry 被 sweep 保留，want 删除（未冻结不得保留）")
	}
	c2AssertRejected(t, cB, f.taskID, f.epoch, "req-02e", "task-not-found")
	// 清理：取消滞留 worker（run 仍阻塞于 longTaskRun；stopTask 只发取消信号，
	// state 已是 completed 故不变；endTask 见 isTaskRunning==false 直接返回）。
	f.sess.stopTask()
	pollTaskEnded(t, f.sess, 10*time.Second)
}

// TestT7_C2_03_ReplayBeforeClaim：重放先于认领到达 → task-not-found；
// 认领后重放成功（5 行、final=false）。
func TestT7_C2_03_ReplayBeforeClaim(t *testing.T) {
	f := setupClaimTask(t, 30*time.Second)
	f.armByDisconnect(t)
	c2ProduceRows(t, f.sess, 1, 5)
	cB, _ := dialClaimer(t, f.wsURL, f.pageID)
	c2AssertRejected(t, cB, f.taskID, f.epoch, "req-c2-03-pre", "task-not-found")
	cB.claim(f.taskID, f.epoch, f.pageID)
	cB.waitResumeAccepted(10*time.Second, f.taskID, f.epoch)
	rB := t6NewReader(t, cB)
	c2ReplaySuccess(t, rB, cB, f.taskID, f.epoch, 0, "req-c2-03-post", []int64{1, 2, 3, 4, 5}, 5, false)
	closeClaimedTask(t, f.sess, cB)
}

// TestT7_C2_04a_CrossPagePreFilter：跨 page 重放被初筛拒绝 → task-not-found；
// 同页对照组重放成功（5 行）。
func TestT7_C2_04a_CrossPagePreFilter(t *testing.T) {
	f, cB := c2SetupClaimedTask(t)
	cC, _ := dialClaimer(t, f.wsURL, "other-page")
	c2AssertRejected(t, cC, f.taskID, f.epoch, "req-04a", "task-not-found")
	rB := t6NewReader(t, cB)
	c2ReplaySuccess(t, rB, cB, f.taskID, f.epoch, 0, "req-04a-ctrl", []int64{1, 2, 3, 4, 5}, 5, false)
	closeClaimedTask(t, f.sess, cB)
}

// TestT7_C2_04b_WhiteBoxVerifyIdentity：白盒直接调用 verifyIdentity——
// taskPageId 不符 → (matchedTaskID=true, reason="not-owner")；
// 正确 pageId → (true, "")。这是唯一能证明该核验的测试。
func TestT7_C2_04b_WhiteBoxVerifyIdentity(t *testing.T) {
	f := setupClaimTask(t, 30*time.Second)
	f.sess.taskMutex.Lock()
	matched, reason := verifyIdentity(f.sess, f.taskID, f.epoch, "other-page")
	f.sess.taskMutex.Unlock()
	if !matched || reason != "not-owner" {
		t.Fatalf("verifyIdentity(P2) = (%v, %q), want (true, not-owner)", matched, reason)
	}
	f.sess.taskMutex.Lock()
	matched, reason = verifyIdentity(f.sess, f.taskID, f.epoch, f.pageID)
	f.sess.taskMutex.Unlock()
	if !matched || reason != "" {
		t.Fatalf("verifyIdentity(P1) = (%v, %q), want (true, \"\")", matched, reason)
	}
	f.sess.stopTask()
	pollTaskEnded(t, f.sess, 10*time.Second)
}

// TestT7_C2_04c_PreFilterFalseNegative：任务中途重注册——初筛假阴性被文档化。
// A 重注册 P2 后，B（注册 P1）发重放 → task-not-found（初筛拒绝），
// 即使 verifyIdentity 会通过（taskPageId 仍是 P1）。
func TestT7_C2_04c_PreFilterFalseNegative(t *testing.T) {
	f := setupClaimTask(t, 30*time.Second)
	f.cA.register("test-t7-c2-04c-p2")
	if got := snapshotTaskPageId(t, f.sess); got != f.pageID {
		t.Fatalf("重注册后 taskPageId = %q, want %q（任务身份不变）", got, f.pageID)
	}
	if le, ok := snapshotLoopState(t, f.cA.connectionID); !ok || le.pageId != "test-t7-c2-04c-p2" {
		t.Fatalf("重注册后 A 的 loopEntry.pageId = %q, want test-t7-c2-04c-p2", le.pageId)
	}
	cB, _ := dialClaimer(t, f.wsURL, f.pageID)
	c2AssertRejected(t, cB, f.taskID, f.epoch, "req-04c", "task-not-found")
	// 对照：verifyIdentity 对同一身份会通过（taskPageId 仍是 P1）——
	// 证明拒绝确实来自初筛，而非核验层。
	f.sess.taskMutex.Lock()
	matched, reason := verifyIdentity(f.sess, f.taskID, f.epoch, f.pageID)
	f.sess.taskMutex.Unlock()
	if !matched || reason != "" {
		t.Fatalf("verifyIdentity(P1) = (%v, %q), want (true, \"\")（核验层会通过）", matched, reason)
	}
	f.sess.stopTask()
	pollTaskEnded(t, f.sess, 10*time.Second)
}

// TestT7_C2_05_DuplicateReplay：同一重放请求发两次，两次均完整返回
// （5 行 + replay_complete，through=5，final=false），单次内无重复 seq。
func TestT7_C2_05_DuplicateReplay(t *testing.T) {
	f, cB := c2SetupClaimedTask(t)
	rB := t6NewReader(t, cB)
	c2ReplaySuccess(t, rB, cB, f.taskID, f.epoch, 0, "req-c2-05-a", []int64{1, 2, 3, 4, 5}, 5, false)
	c2ReplaySuccess(t, rB, cB, f.taskID, f.epoch, 0, "req-c2-05-b", []int64{1, 2, 3, 4, 5}, 5, false)
	closeClaimedTask(t, f.sess, cB)
}

// TestT7_C2_06_CancelDuringReplay：重放暂停期间取消任务，恢复后重放完成
// （剩余行 + replay_complete final=true），状态保持 cancelled。
// 注意：重放 handler 在 B 的读循环内同步执行（safeHandler，server.go:319），
// 暂停期间 B 的读循环阻塞，WS stop_task 无法被处理；故此处直接调
// f.sess.stopTask()（与 stop_task handler 调用的同一 cancelTask 路径）。
func TestT7_C2_06_CancelDuringReplay(t *testing.T) {
	f, cB := c2SetupClaimedTask(t)
	rB := t6NewReader(t, cB)
	gate := &replayPauseGate{afterRows: 3, entered: make(chan struct{}), release: make(chan struct{})}
	testReplayPauseGate = gate
	t.Cleanup(func() { testReplayPauseGate = nil })
	t6SendReplay(cB, f.taskID, f.epoch, 0, "req-c2-06")
	select {
	case <-gate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("重放未在 10s 内到达第 3 行暂停点")
	}
	c2AssertRows(t, rB, []int64{1, 2, 3})
	// 重放暂停期间直接取消任务（见上注：WS stop_task 在此窗口内无法被处理）。
	f.sess.stopTask()
	pollTaskEnded(t, f.sess, 10*time.Second)
	st, _, _ := snapshotDisconnectState(t, f.sess)
	if st != stateCancelled {
		t.Fatalf("取消后 state = %d, want cancelled", int(st))
	}
	// 恢复重放：已冻结的日志仍可完整重放（剩余行 + final=true）。
	close(gate.release)
	c2AssertRows(t, rB, []int64{4, 5})
	m := rB.waitFor("replay_complete", 10*time.Second)
	d := msgData(m)
	if rt, _ := d["replayThroughSeq"].(float64); int64(rt) != 5 {
		t.Fatalf("replay_complete.replayThroughSeq = %v, want 5", rt)
	}
	if fin, _ := d["final"].(bool); !fin {
		t.Fatalf("replay_complete.final = %v, want true", fin)
	}
	st, _, _ = snapshotDisconnectState(t, f.sess)
	if st != stateCancelled {
		t.Fatalf("重放完成后 state = %d, want cancelled（保持）", int(st))
	}
}

// TestT7_C2_07_ResetDuringReplay：重放暂停期间 reset，恢复后重放中止
// （无 replay_complete，已发的 3 行保留）。
func TestT7_C2_07_ResetDuringReplay(t *testing.T) {
	f, cB := c2SetupClaimedTask(t)
	rB := t6NewReader(t, cB)
	gate := &replayPauseGate{afterRows: 3, entered: make(chan struct{}), release: make(chan struct{})}
	testReplayPauseGate = gate
	t.Cleanup(func() { testReplayPauseGate = nil })
	t6SendReplay(cB, f.taskID, f.epoch, 0, "req-c2-07")
	select {
	case <-gate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("重放未在 10s 内到达第 3 行暂停点")
	}
	c2AssertRows(t, rB, []int64{1, 2, 3})
	resetBackendGeneration()
	close(gate.release)
	if _, ok := cB.waitForTimeout("replay_complete", 2*time.Second); ok {
		t.Fatal("reset 后恢复重放收到 replay_complete，want 无")
	}
}

// TestT7_C2_08_OwnConnectionReplayRejected：原连接直接重放 → task-not-found
// （共享初筛的 reqConn 排除）；已 live 收到的 5 行不受影响，不再补发。
func TestT7_C2_08_OwnConnectionReplayRejected(t *testing.T) {
	f := setupClaimTask(t, 30*time.Second)
	rA := t6NewReader(t, f.cA)
	c2ProduceRows(t, f.sess, 1, 5)
	c2AssertRows(t, rA, []int64{1, 2, 3, 4, 5})
	c2AssertRejected(t, f.cA, f.taskID, f.epoch, "req-c2-08", "task-not-found")
	f.sess.stopTask()
	pollTaskEnded(t, f.sess, 10*time.Second)
}

// TestT7_C2_02f_ExpiredCompletedNotRetained：已完成且日志已冻结、但 frozenAt
// 已超出 resultLogRetention（5 min）——D-1 不再保留条目，sweep 删除，重放 task-not-found。
// 与 02a/02b 的区别：仅 TTL 到期，其余保留条件（completed、日志非 nil、已冻结）全部满足，
// 证明 TTL 是 D-1 保留的必要条件。
func TestT7_C2_02f_ExpiredCompletedNotRetained(t *testing.T) {
	f, cB, done := c2SetupClaimedCompletableTask(t)
	c2CompleteTask(t, f, done)
	// 确认日志已冻结且含 5 行（自然完成路径的 endTask 冻结）。
	log := f.sess.resultLog.Load()
	if log == nil {
		t.Fatal("02f: resultLog is nil after natural completion")
	}
	log.mu.Lock()
	frozen, n := log.frozen, len(log.rows)
	log.mu.Unlock()
	if !frozen {
		t.Fatal("02f: resultLog not frozen after natural completion")
	}
	if n != 5 {
		t.Fatalf("02f: log rows = %d, want 5", n)
	}
	// 两项时间均调到有效期之外：exitedAt 超出 loopEntry retention（5 min），
	// frozenAt 超出 resultLogRetention（5 min）。本测试验证的是过期条目的
	// sweeper 删除行为——仅 TTL 到期，其余保留条件全部满足。
	c2SetExitedAt(t, f.cA.connectionID, -6*time.Minute)
	c2SetFrozenAt(t, f.sess, -6*time.Minute)
	backgroundTaskMutex.Lock()
	sweepOnceLocked()
	backgroundTaskMutex.Unlock()
	if _, ok := snapshotLoopState(t, f.cA.connectionID); ok {
		t.Fatal("02f: A 的 loopEntry 被 sweep 保留，want 删除（日志 TTL 到期不得保留）")
	}
	c2AssertRejected(t, cB, f.taskID, f.epoch, "req-02f", "task-not-found")
}

// TestT7_C3_77a_TaskCompleteIdentity（P0-3 正向）：任务自然完成时，
// task_complete 帧的 data.taskId/data.backendEpoch 与同一任务的
// task_started 完全一致。真实 Go WebSocket 集成测试，不用 JS VM 注入，
// 不新增生产 hook（复用 r3Client.waitFor 模式）。
func TestT7_C3_77a_TaskCompleteIdentity(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register("test-p0-77a-page")
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	run := func(ctx context.Context, s *appSession) {
		s.sendTaskResult(ctx, "test_result", map[string]interface{}{"ip": "1.2.3.4"})
		// 自然返回，不等 ctx.Done()：worker defer 走 task_complete 分支。
	}
	startSyntheticTask(t, sess, run)
	pollSessionRunning(t, sess, 10*time.Second)
	taskID := sess.taskIDSnapshot()
	epoch := currentBackendEpoch()
	started := cA.waitFor("task_started", 10*time.Second)
	startedData, _ := started["data"].(map[string]interface{})
	if got, _ := startedData["taskId"].(string); got != taskID {
		t.Fatalf("task_started taskId = %q, want %q", got, taskID)
	}
	if got, _ := startedData["backendEpoch"].(string); got != epoch {
		t.Fatalf("task_started backendEpoch = %q, want %q", got, epoch)
	}
	complete := cA.waitFor("task_complete", 10*time.Second)
	completeData, _ := complete["data"].(map[string]interface{})
	if completeData == nil {
		t.Fatal("task_complete 缺少 data（P0-3 要求身份载荷）")
	}
	if got, _ := completeData["taskId"].(string); got != taskID {
		t.Fatalf("task_complete taskId = %q, want %q（与 task_started 一致）", got, taskID)
	}
	if got, _ := completeData["backendEpoch"].(string); got != epoch {
		t.Fatalf("task_complete backendEpoch = %q, want %q（与 task_started 一致）", got, epoch)
	}
	pollTaskEnded(t, sess, 10*time.Second)
}

// TestT7_C3_77b_TaskStoppedIdentity（P0-2 正向）：任务被 stop_task 停止时，
// task_stopped 帧的 data.taskId/data.backendEpoch 与同一任务的
// task_started 完全一致。真实 Go WebSocket 集成测试，不用 JS VM 注入，
// 不新增生产 hook。
func TestT7_C3_77b_TaskStoppedIdentity(t *testing.T) {
	r3Setup(t)
	_, wsURL := r3Server(t)
	cA := r3Dial(t, wsURL)
	cA.readInit(10 * time.Second)
	cA.register("test-p0-77b-page")
	sess := snapshotServerSession(t, cA.connectionID)
	if sess == nil {
		t.Fatal("no server session for A")
	}
	run := func(ctx context.Context, s *appSession) {
		s.sendTaskResult(ctx, "test_result", map[string]interface{}{"ip": "1.2.3.4"})
		<-ctx.Done()
	}
	startSyntheticTask(t, sess, run)
	pollSessionRunning(t, sess, 10*time.Second)
	taskID := sess.taskIDSnapshot()
	epoch := currentBackendEpoch()
	started := cA.waitFor("task_started", 10*time.Second)
	startedData, _ := started["data"].(map[string]interface{})
	if got, _ := startedData["taskId"].(string); got != taskID {
		t.Fatalf("task_started taskId = %q, want %q", got, taskID)
	}
	if got, _ := startedData["backendEpoch"].(string); got != epoch {
		t.Fatalf("task_started backendEpoch = %q, want %q", got, epoch)
	}
	sess.stopTask()
	pollTaskEnded(t, sess, 10*time.Second)
	stopped := cA.waitFor("task_stopped", 10*time.Second)
	stoppedData, _ := stopped["data"].(map[string]interface{})
	if stoppedData == nil {
		t.Fatal("task_stopped 缺少 data（P0-2 要求身份载荷）")
	}
	if got, _ := stoppedData["taskId"].(string); got != taskID {
		t.Fatalf("task_stopped taskId = %q, want %q（与 task_started 一致）", got, taskID)
	}
	if got, _ := stoppedData["backendEpoch"].(string); got != epoch {
		t.Fatalf("task_stopped backendEpoch = %q, want %q（与 task_started 一致）", got, epoch)
	}
}

// TestP0_ReplayCompleteEchoesRequestId（P0-1 正向）：replay_results 请求携带
// requestId 时，replay_complete 帧的 data.requestId 与请求中的 requestId
// 完全一致（与 replay_rejected 的回显语义一致）。真实 Go WebSocket 集成
// 测试，不用 JS VM 注入，不新增生产 hook。行补发本身由 T6-02 覆盖，
// 本测试只在 T6-02 流程上追加回显断言。
func TestP0_ReplayCompleteEchoesRequestId(t *testing.T) {
	bg, _ := t6BG(t, "t6-p0-echo")
	taskID := bg.taskIDSnapshot()
	_, wsURL := r3Server(t)
	c1 := r3Dial(t, wsURL)
	r1 := t6NewReader(t, c1)
	epoch := t6Follow(t, r1)
	for i := 0; i < 5; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("7.7.7.%d", i+1), Port: 443})
	}
	if got := r1.collectRows("scan_result", 5, 10*time.Second); got[4] != 5 {
		t.Fatalf("前 5 行 seq = %v, want 1..5", got)
	}
	c1.Close()
	time.Sleep(200 * time.Millisecond)
	for i := 5; i < 10; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("7.7.7.%d", i+1), Port: 443})
	}
	c2 := r3Dial(t, wsURL)
	r2 := t6NewReader(t, c2)
	if epoch2 := t6Follow(t, r2); epoch2 != epoch {
		t.Fatalf("重连后 epoch 变化")
	}
	const requestID = "req-p0-echo-001"
	t6SendReplay(c2, taskID, epoch, 5, requestID)
	if got := r2.collectRows("scan_result", 5, 10*time.Second); got[0] != 6 || got[4] != 10 {
		t.Fatalf("补发行 seq = %v, want 6..10", got)
	}
	complete := r2.waitFor("replay_complete", 10*time.Second)
	data := msgData(complete)
	if gotID, _ := data["requestId"].(string); gotID != requestID {
		t.Fatalf("replay_complete requestId = %q, want %q", gotID, requestID)
	}
	if rt, _ := data["replayThroughSeq"].(float64); rt != 10 {
		t.Fatalf("replayThroughSeq = %v, want 10", rt)
	}
}

// TestP0_OldClientReplayStillWorks（P0-4 兼容 + 旧客户端行为）：
// (a) init_config 携带 replayCompleteEcho=true（布尔值）与 resultReplay=true；
// (b) 不带 requestId 的旧式 replay_results 请求（requestId 为空串，与字段
// 缺失等价——服务端解析零值同为 ""）仍能正常完成重放，replay_complete 到达，
// 回显 requestId 为 ""。旧客户端忽略未知字段、不读回显，故行为不变。
// 后端侧不存在“报告 false”的路径（P0-1 无条件实现，回显字段恒为 true）；
// false 分支的语义（缺失/false = 旧后端，fail-closed）在 P1 客户端实现与
// 验证，不属于 P0 后端测试范围。
func TestP0_OldClientReplayStillWorks(t *testing.T) {
	_, wsURL := r3Server(t)
	// (a) 能力字段。
	c0 := r3Dial(t, wsURL)
	initMsg := c0.waitFor("init_config", 10*time.Second)
	initData, _ := initMsg["data"].(map[string]interface{})
	if echo, ok := initData["replayCompleteEcho"].(bool); !ok || !echo {
		t.Fatalf("init_config replayCompleteEcho = %v (type %T), want true bool", initData["replayCompleteEcho"], initData["replayCompleteEcho"])
	}
	if rr, ok := initData["resultReplay"].(bool); !ok || !rr {
		t.Fatalf("init_config resultReplay = %v, want true", initData["resultReplay"])
	}
	c0.Close()
	// (b) 旧式重放。
	bg, _ := t6BG(t, "t6-p0-compat")
	taskID := bg.taskIDSnapshot()
	c1 := r3Dial(t, wsURL)
	r1 := t6NewReader(t, c1)
	epoch := t6Follow(t, r1)
	for i := 0; i < 5; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("8.8.8.%d", i+1), Port: 443})
	}
	if got := r1.collectRows("scan_result", 5, 10*time.Second); got[4] != 5 {
		t.Fatalf("前 5 行 seq = %v, want 1..5", got)
	}
	c1.Close()
	time.Sleep(200 * time.Millisecond)
	for i := 5; i < 10; i++ {
		bg.sendTaskResult(t6Ctx(bg), "scan_result", ScanResult{IP: fmt.Sprintf("8.8.8.%d", i+1), Port: 443})
	}
	c2 := r3Dial(t, wsURL)
	r2 := t6NewReader(t, c2)
	if epoch2 := t6Follow(t, r2); epoch2 != epoch {
		t.Fatalf("重连后 epoch 变化")
	}
	t6SendReplay(c2, taskID, epoch, 5, "")
	if got := r2.collectRows("scan_result", 5, 10*time.Second); got[0] != 6 || got[4] != 10 {
		t.Fatalf("补发行 seq = %v, want 6..10", got)
	}
	complete := r2.waitFor("replay_complete", 10*time.Second)
	data := msgData(complete)
	if gotID, _ := data["requestId"].(string); gotID != "" {
		t.Fatalf("replay_complete requestId = %q, want \"\"（旧式请求无 requestId）", gotID)
	}
	if rt, _ := data["replayThroughSeq"].(float64); rt != 10 {
		t.Fatalf("replayThroughSeq = %v, want 10", rt)
	}
}
