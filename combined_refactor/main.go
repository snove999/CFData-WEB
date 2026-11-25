package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var webUser, webPassword string
var webSessionMinutes int
var boolFlagNames = []string{"cli", "nsbtls", "progress", "nocolor", "compactipv4", "nsbcompact", "github", "skipgeo", "outv6bracket", "edgetunnel"}

type latestReleaseInfo struct {
	TagName string `json:"tag_name"`
	HTMLURL string `json:"html_url"`
}

func getLatestRelease(ctx context.Context) (latestReleaseInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com/repos/PoemMisty/CFData-WEB/releases/latest", nil)
	if err != nil {
		return latestReleaseInfo{}, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "CFData-WEB/"+appVersion)
	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	resp, err := managementHTTPClient(0).Do(req.WithContext(ctx))
	if err != nil {
		return latestReleaseInfo{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return latestReleaseInfo{}, fmt.Errorf("GitHub 返回状态 %d", resp.StatusCode)
	}
	var info latestReleaseInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return latestReleaseInfo{}, err
	}
	if info.HTMLURL == "" {
		info.HTMLURL = releaseLatestURL
	}
	return info, nil
}

func versionIsOlder(current, latest string) bool {
	current = strings.TrimPrefix(strings.TrimSpace(current), "v")
	latest = strings.TrimPrefix(strings.TrimSpace(latest), "v")
	if current == "" || latest == "" || current == "dev" {
		return false
	}
	cParts := strings.FieldsFunc(current, func(r rune) bool { return r == '.' || r == '-' || r == '_' })
	lParts := strings.FieldsFunc(latest, func(r rune) bool { return r == '.' || r == '-' || r == '_' })
	for i := 0; i < len(cParts) || i < len(lParts); i++ {
		c, l := 0, 0
		if i < len(cParts) {
			c, _ = strconv.Atoi(cParts[i])
		}
		if i < len(lParts) {
			l, _ = strconv.Atoi(lParts[i])
		}
		if c != l {
			return c < l
		}
	}
	return false
}

func checkAndPrintUpdate(prefix string) {
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	info, err := getLatestRelease(ctx)
	if err != nil {
		recordDebugError("update_check", err.Error())
		fmt.Printf("%s更新检测失败: %v\n", prefix, err)
		return
	}
	if versionIsOlder(appVersion, info.TagName) {
		fmt.Printf("%s发现新版本 %s，下载: %s\n", prefix, info.TagName, releaseLatestURL)
	}
}

func rewriteBoolFlagArgs() {
	if len(os.Args) <= 2 {
		return
	}
	boolSet := map[string]struct{}{}
	for _, n := range boolFlagNames {
		boolSet[n] = struct{}{}
	}
	rewritten := append([]string(nil), os.Args[:1]...)
	for i := 1; i < len(os.Args); i++ {
		arg := os.Args[i]
		if name, ok := matchBoolFlag(arg, boolSet); ok && i+1 < len(os.Args) {
			next := strings.ToLower(os.Args[i+1])
			if next == "true" || next == "false" || next == "1" || next == "0" || next == "t" || next == "f" {
				rewritten = append(rewritten, "-"+name+"="+next)
				i++
				continue
			}
		}
		rewritten = append(rewritten, arg)
	}
	os.Args = rewritten
}

func matchBoolFlag(arg string, boolSet map[string]struct{}) (string, bool) {
	if !strings.HasPrefix(arg, "-") {
		return "", false
	}
	name := strings.TrimLeft(arg, "-")
	if strings.Contains(name, "=") {
		return "", false
	}
	if _, ok := boolSet[name]; ok {
		return name, true
	}
	return "", false
}

func hasNoColorArg() bool {
	for _, arg := range os.Args[1:] {
		name := strings.TrimLeft(arg, "-")
		if name == "nocolor" || strings.HasPrefix(name, "nocolor=") {
			if name == "nocolor" || strings.EqualFold(strings.TrimPrefix(name, "nocolor="), "true") {
				return true
			}
		}
	}
	return false
}

// 后端就绪状态（第二阶段）：只反映本地 HTTP 服务状态，不依赖任何外网探测。
// starting：Main() 已开始但监听器尚未绑定；
// ready：监听器已绑定且 Serve 循环已开始，本地初始化（配置加载等）均已完成；
// failed：监听失败、Serve 异常退出或 Main() 已退出。
var (
	backendHealthMu     sync.Mutex
	backendHealthStatus = "starting"
	backendHealthError  = ""
)

func setBackendHealthStatus(status, errMsg string) {
	backendHealthMu.Lock()
	defer backendHealthMu.Unlock()
	backendHealthStatus = status
	backendHealthError = errMsg
}

func backendHealthStatusValue() string {
	backendHealthMu.Lock()
	defer backendHealthMu.Unlock()
	return backendHealthStatus
}

// noteBackendExited 在后端退出时把健康状态置为 failed：
// 状态为 starting 或 ready 时都转换；状态已为 failed 时保留已有的具体失败原因，
// 不被通用的退出原因覆盖。
func noteBackendExited(msg string) {
	backendHealthMu.Lock()
	defer backendHealthMu.Unlock()
	if backendHealthStatus == "starting" || backendHealthStatus == "ready" {
		backendHealthStatus = "failed"
		backendHealthError = msg
	}
}

// isLoopbackRemoteAddr 判断请求是否来自本机回环地址（127.0.0.0/8、::1）。
// RemoteAddr 形如 "IP:port"，解析失败时按非回环处理。
func isLoopbackRemoteAddr(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

// handleHealthz 只读本地状态，不做任何外网探测、不读配置、不访问业务 handler，
// 因此它自身不会成为启动死锁的原因；也不走 requireAuth。
// 访问控制：仅允许回环客户端，非回环请求一律 403，不泄露健康状态。
// 监听地址本身不改（桌面端仍监听全部接口，页面访问行为不变），只收紧本接口。
func handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")
	if !isLoopbackRemoteAddr(r.RemoteAddr) {
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "forbidden", "error": "仅允许本机访问"})
		return
	}
	backendHealthMu.Lock()
	status, errMsg := backendHealthStatus, backendHealthError
	backendHealthMu.Unlock()
	if status == "ready" {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	payload := map[string]interface{}{"status": status}
	if errMsg != "" {
		payload["error"] = errMsg
	}
	_ = json.NewEncoder(w).Encode(payload)
}

func main() {
	setBackendHealthStatus("starting", "")
	// R3 §2.3（第二轮补齐任务 4）：每次实际后端启动的唯一汇合点——
	// 桌面端与 iOS 端都经过 main() 顶部（iOS 的 Start() 拉起的 Main()
	// 即本函数经构建改名），因此只在此处重置一次；不在 Start() 里
	// 再调一次，否则一次启动会递增两次代次（见交付报告）。
	resetBackendGeneration()
	rewriteBoolFlagArgs()
	if !enableTerminalANSI() || os.Getenv("NO_COLOR") != "" || hasNoColorArg() {
		disableANSIColors()
	}
	cliCfg := registerCLIFlags()

	flag.IntVar(&listenPort, "port", 13335, "服务监听端口")
	flag.StringVar(&listenHost, "host", "", "服务监听地址；留空监听全部地址，Android APK 建议使用 127.0.0.1")
	flag.StringVar(&speedTestURL, "url", autoSpeedURLValue, "测速下载地址；auto 表示由后端自动选择内置测速源")
	flag.BoolVar(&skipGeoCheck, "skipgeo", false, "跳过地区/代理环境验证")
	flag.StringVar(&customDNSServer, "dns", defaultDNSServers, "自定义 DNS 服务器，例如 223.5.5.5、8.8.8.8:53 或逗号分隔多个；默认系统 DNS 优先、失败回退到该内置 DNS，显式提供时强制使用指定 DNS")
	flag.Var(debugFlagValue{}, "debug", "开启调试输出等级：error、all；也兼容 true/false，-debug 默认为 error")
	flag.StringVar(&webUser, "user", "", "Web 认证用户名（不设置则不启用认证）")
	flag.StringVar(&webPassword, "password", "", "Web 认证密码（需同时设置 -user）")
	flag.IntVar(&webSessionMinutes, "session", 720, "Web 登录会话有效期（分钟）")
	flag.Parse()
	if debugMode && flag.NArg() > 0 {
		for _, arg := range flag.Args() {
			if normalizeDebugLevel(arg) == "all" {
				debugLevel = "all"
			}
		}
	}
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "dns" {
			customDNSForced = true
		}
	})
	printBanner()
	enterMenu := false
	if cliCfg.enabled {
		args := flag.Args()
		quickStart := len(args) > 0 && strings.EqualFold(strings.TrimSpace(args[0]), "qs")
		if !quickStart && onlyCLIArgProvided() {
			enterMenu = true
		}
		if !enterMenu {
			if err := prepareCLIConfig(cliCfg); err != nil {
				if errors.Is(err, errCLIConfigCreated) {
					return
				}
				recordProgramDebugError("cli_prepare", err.Error())
				fmt.Printf("CLI 执行失败: %v\n", err)
				return
			}
		}
	}
	speedTestWorkers = cliCfg.speedTest
	configureHTTPClients()
	startupSpeedTestURL := speedTestURL
	if !cliCfg.enabled {
		if os.Getenv("CFDATA_IOS") == "1" {
			// iOS 首屏不等待外网测速源探测；测速源在首次实际测速时按需选择。
			setAutoSpeedURLDefault(cloudflareSpeedURL)
		} else {
			ensureSpeedURLAvailability()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resolvedSpeedURL, speedISP, err := resolveStartupSpeedTestURL(ctx, speedTestURL)
			cancel()
			if err != nil {
				recordDebugError("speed_isp_check", err.Error())
			} else {
				startupSpeedTestURL = resolvedSpeedURL
				recordDebugByLevel("all", "speed_isp_check", fmt.Sprintf("startup asn=%d org=%s mobile=%v selected=%s", speedISP.ASN, speedISP.ASOrganization, isChinaMobileISP(speedISP), currentAutoSpeedURLDefault()))
			}
		}
	}
	if webSessionMinutes <= 0 {
		webSessionMinutes = 720
	}
	webSessionTTL = time.Duration(webSessionMinutes) * time.Minute

	if !cliCfg.enabled {
		cfgPath := defaultCLIConfigPath()
		if _, created, err := loadOrCreateCLIConfig(cfgPath); err != nil && !errors.Is(err, errCLIConfigCreated) {
			fmt.Printf("[config] 加载配置文件失败: %v\n", err)
		} else if created {
			fmt.Printf("[config] 已生成配置文件模板: %s\n", cfgPath)
		}
		applyManagementProxyFromConfigFile(cfgPath)
	}
	if os.Getenv("CFDATA_IOS") == "1" {
		// locations 只影响地区名称展示，不应阻塞 iOS 本地 HTTP 服务启动。
		go initLocations()
	} else {
		initLocations()
	}
	if cliCfg.enabled {
		if enterMenu {
			runCLIMenu(cliCfg)
			return
		}
		if err := runCLI(cliCfg); err != nil {
			recordProgramDebugError("cli_run", err.Error())
			fmt.Printf("CLI 执行失败: %v\n", err)
		}
		return
	}

	http.HandleFunc("/auth/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			handleLoginPost(w, r)
			return
		}
		handleLoginPage(w, r)
	})
	http.HandleFunc("/auth/logout", handleLogout)
	http.HandleFunc("/favicon.png", func(w http.ResponseWriter, r *http.Request) {
		data, err := staticFiles.ReadFile("favicon.png")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(data)
	})

	http.HandleFunc("/", requireAuth(func(w http.ResponseWriter, r *http.Request) {
		data, err := staticFiles.ReadFile("index.html")
		if err != nil {
			http.Error(w, "无法加载页面", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(data)
	}))
	http.HandleFunc("/ws", requireAuth(handleWebSocket))
	http.HandleFunc("/healthz", handleHealthz)

	addr := fmt.Sprintf(":%d", listenPort)
	displayHost := "localhost"
	if strings.TrimSpace(listenHost) != "" {
		addr = fmt.Sprintf("%s:%d", strings.TrimSpace(listenHost), listenPort)
		displayHost = strings.TrimSpace(listenHost)
	}
	fmt.Printf("CFData-WEB 版本: %s\n", appVersion)
	go checkAndPrintUpdate("")
	displayURL := fmt.Sprintf("http://%s:%d", displayHost, listenPort)
	if webUser != "" && webPassword != "" {
		fmt.Printf("Web 认证已启用，用户名: %s\n", webUser)
		fmt.Printf("Web 会话有效期: %s 分钟\n", strconv.Itoa(webSessionMinutes))
	} else if webUser != "" || webPassword != "" {
		fmt.Println("警告： 需要同时设置 -user 和 -password 才会启用认证")
	}
	fmt.Printf("当前测速网址: %s\n", startupSpeedTestURL)
	if skipGeoCheck {
		fmt.Println("地区验证: 已跳过")
	} else {
		fmt.Println("地区验证: 启用")
	}
	if strings.TrimSpace(customDNSServer) == "" {
		fmt.Println("当前 DNS: 系统 DNS")
	} else {
		fmt.Printf("当前 DNS: %s\n", customDNSServer)
	}
	fmt.Printf("调试模式: %v\n", debugMode)
	if debugMode {
		fmt.Printf("调试等级: %s\n", normalizeDebugLevel(debugLevel))
		fmt.Printf("调试日志: %s\n", defaultDebugLogPath())
	}
	fmt.Printf("服务启动于 %s\n", displayURL)
	fmt.Printf("服务启动成功，复制 %s 到浏览器打开\n", displayURL)
	server := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: 10 * time.Second,
	}
	// 第二阶段：先 Listen 再 Serve。监听成功即表示 HTTP 服务已开始接收请求，
	// 此时本地初始化（配置加载、代理应用等）均已完成；外网探测
	// （locations、版本检查）均在后台协程，不阻塞就绪。Serve 仍使用
	// 同一个 server（ReadHeaderTimeout 不变），与 ListenAndServe 等价。
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		setBackendHealthStatus("failed", err.Error())
		fmt.Printf("启动失败: %v\n", err)
		return
	}
	// R3 §16.3 testMainListenerHook：Listen 成功后立即调用（生产恒 nil）。
	if h := testHookGet(&testMainListenerHook); h != nil {
		h(listener)
	}
	// R3 §2.10（第二轮补齐任务 5）：启动本代次的 stateSweeper（每代次一个，
	// 由当次 Main() 拥有）；Serve 返回后停止 defer 等待其退出确认。
	// Listen 失败时 sweeper 从不启动。重复启动由 startStateSweeper 的
	// CAS 拒绝（sweeperLive 恒 ∈ {0,1}）。
	sweeperStop, sweeperDone, sweeperStarted := startStateSweeper()
	if sweeperStarted {
		defer func() {
			close(sweeperStop)
			<-sweeperDone
		}()
	}
	// S7 Batch 2（方案 A，§11.5.4）：与 stateSweeper 并列启动看门狗。
	// started=false 只表示已有看门狗在运行（防御性分支；生产 main() 只跑一次，
	// 实际不可达；iOS 重启时前次 Main() 的 defer 已停旧看门狗，live 已回 0）——
	// 此时不注册停止 defer（不拥有该实例的 stop 通道），继续 Serve。
	// stopWatchdog 幂等：重复停止安全；defer 在 sweeper 的 defer 之后注册，
	// 返回时先执行（LIFO），两者独立、无共享锁，停止顺序无关。
	wdStop, wdDone, wdStarted := startWatchdog()
	if wdStarted {
		defer stopWatchdog(wdStop, wdDone)
	}
	setBackendHealthStatus("ready", "")
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		setBackendHealthStatus("failed", err.Error())
		fmt.Printf("启动失败: %v\n", err)
	}
}
