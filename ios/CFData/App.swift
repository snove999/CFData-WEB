import UIKit
import WebKit
import Cfdatacore

private let backendPort = 13335

/// 第一阶段性能日志：仅输出日志，不改变任何行为。
/// 格式：`[perf ts=<ISO8601> elapsed_ms=<自进程启动> load_gen=<启动代次> event=<事件> <详情>]`
/// load_gen 取 WebViewController.loadGeneration；App 启动时 load_gen 尚未产生，记为 0。
private enum PerfLog {
    private static let processStart = Date()
    private static let formatter: ISO8601DateFormatter = {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return f
    }()
    // 第二阶段：迁移日志在后台队列调用，ISO8601DateFormatter 不是线程安全的，用锁保护。
    private static let formatterLock = NSLock()

    static func log(_ event: String, loadGen: Int? = nil, _ details: String = "") {
        let elapsed = Int(Date().timeIntervalSince(processStart) * 1000)
        let genPart = loadGen.map { " load_gen=\($0)" } ?? ""
        let detailPart = details.isEmpty ? "" : " \(details)"
        formatterLock.lock()
        let timestamp = formatter.string(from: Date())
        formatterLock.unlock()
        print("[perf ts=\(timestamp) elapsed_ms=\(elapsed)\(genPart) event=\(event)\(detailPart)]")
    }
}

@main
class AppDelegate: UIResponder, UIApplicationDelegate {
    var window: UIWindow?

    func application(_ application: UIApplication,
                     didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]? = nil) -> Bool {
        PerfLog.log("app_launch", loadGen: 0)
        window = UIWindow(frame: UIScreen.main.bounds)
        window?.rootViewController = WebViewController()
        window?.makeKeyAndVisible()
        return true
    }
}

// MARK: - Go 后端

enum Backend {
    /// 私有数据目录（Application Support/CFData），不在“文件”App 中暴露，避免泄露配置中的 Token
    /// 第二阶段：这里只做纯路径计算，不做任何文件 IO；目录创建和迁移见 prepareDataDirectory。
    static let dataDirectory: URL = {
        let fm = FileManager.default
        let base = fm.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
        return base.appendingPathComponent("CFData", isDirectory: true)
    }()

    /// 数据目录准备状态（第二阶段迁移门禁）。
    /// 只有 completed 之后才允许调用 start()，即 Go 后端读取配置；
    /// failed 时不启动后端，调用方须向 UI 报告错误；失败可重试，不永久缓存失败。
    private enum MigrationState {
        case notStarted, inProgress, completed
        case failed(String)
    }

    private static let migrationQueue = DispatchQueue(label: "com.cfdata.backend.migration", qos: .userInitiated)
    private static let migrationLock = NSLock()
    private static var migrationState: MigrationState = .notStarted
    private static var migrationWaiters: [(Result<URL, Error>) -> Void] = []

    /// 在后台队列准备数据目录；完成回调一律在主线程调用，包括已完成分支。
    /// 已完成时在主线程回调成功；进行中时加入等待列表，不重复执行迁移；
    /// 上次失败时重新尝试迁移。调用方必须等 success 后才调用 start()。
    static func prepareDataDirectory(loadGen: Int, completion: @escaping (Result<URL, Error>) -> Void) {
        migrationLock.lock()
        if case .completed = migrationState {
            migrationLock.unlock()
            DispatchQueue.main.async { completion(.success(dataDirectory)) }
            return
        }
        if case .inProgress = migrationState {
            migrationWaiters.append(completion)
            migrationLock.unlock()
            return
        }
        // notStarted 或 failed：开始一次新的迁移尝试（失败可重试）。
        migrationState = .inProgress
        migrationWaiters.append(completion)
        migrationLock.unlock()
        PerfLog.log("data_directory_prepare_start", loadGen: loadGen)
        migrationQueue.async {
            let result = Self.runMigration()
            Self.migrationLock.lock()
            let waiters = Self.migrationWaiters
            Self.migrationWaiters = []
            switch result {
            case .success:
                Self.migrationState = .completed
                PerfLog.log("data_directory_prepare_finished", loadGen: loadGen)
            case .failure(let error):
                Self.migrationState = .failed(error.localizedDescription)
                PerfLog.log("data_directory_prepare_failed", loadGen: loadGen, error.localizedDescription)
            }
            Self.migrationLock.unlock()
            DispatchQueue.main.async {
                for waiter in waiters { waiter(result) }
            }
        }
    }

    /// 后台队列执行：先建目录，再迁移旧 Documents 数据。任一步失败即返回失败，
    /// 不启动后端、不让 Go 读取配置或创建新配置；已移动的文件不回滚，
    /// 未移动的文件保留在原处，重试时可继续迁移。
    private static func runMigration() -> Result<URL, Error> {
        do {
            try FileManager.default.createDirectory(at: dataDirectory, withIntermediateDirectories: true)
        } catch {
            return .failure(error)
        }
        if let error = migrateLegacyDocuments(to: dataDirectory) {
            return .failure(error)
        }
        return .success(dataDirectory)
    }

    /// 旧版本把数据写在 Documents，首次启动时迁移到私有目录。
    /// 规则与原来一致：目标已存在的文件跳过不移动、不覆盖；移动失败时记录首个错误
    /// 并继续迁移其余文件，源文件保留在原处。返回 nil 表示全部成功或无文件可迁移。
    private static func migrateLegacyDocuments(to dir: URL) -> Error? {
        let fm = FileManager.default
        let docs = fm.urls(for: .documentDirectory, in: .userDomainMask)[0]
        guard fm.fileExists(atPath: docs.path) else { return nil }
        guard let items = try? fm.contentsOfDirectory(at: docs, includingPropertiesForKeys: nil) else {
            return NSError(domain: "com.cfdata.backend", code: 1,
                           userInfo: [NSLocalizedDescriptionKey: "无法读取 Documents 目录"])
        }
        var firstError: Error?
        for item in items {
            let dest = dir.appendingPathComponent(item.lastPathComponent)
            if !fm.fileExists(atPath: dest.path) {
                do {
                    try fm.moveItem(at: item, to: dest)
                } catch {
                    if firstError == nil { firstError = error }
                }
            }
        }
        return firstError
    }

    /// 启动（或在已退出时重启）后端；返回错误描述。
    /// 调用前提：prepareDataDirectory 已成功。Go 侧 stateMu 保证同一进程
    /// 不会重复启动；已在运行时直接返回成功。
    static func start() -> String? {
        // gomobile 导出的是 C 函数，Swift 不会自动转换为 throws，需要显式传入 NSError 指针
        var error: NSError?
        if CfdatacoreStart(dataDirectory.path, backendPort, &error) {
            return nil
        }
        return error?.localizedDescription ?? "后端启动失败"
    }

    /// 本地 HTTP 健康检查状态（第二阶段）。
    enum HealthStatus {
        case ready
        case starting
        case failed(String)
        case unreachable
    }

    /// GET /healthz，1 秒超时。严格校验 HTTP 状态码与 JSON status 的对应关系：
    /// ready 必须对应 200，starting / failed 必须对应 503，forbidden 对应 403，
    /// 不一致即明确失败；禁用缓存，避免读到过期状态；不依赖任何外网服务。
    static func checkHealth(completion: @escaping (HealthStatus) -> Void) {
        let url = URL(string: "http://127.0.0.1:\(backendPort)/healthz")!
        var request = URLRequest(url: url)
        request.timeoutInterval = 1
        request.cachePolicy = .reloadIgnoringLocalAndRemoteCacheData
        request.setValue("no-cache", forHTTPHeaderField: "Cache-Control")
        URLSession.shared.dataTask(with: request) { data, response, error in
            if error != nil {
                completion(.unreachable)
                return
            }
            guard let http = response as? HTTPURLResponse,
                  let data = data,
                  let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                  let status = json["status"] as? String else {
                completion(.failed("健康检查响应无效"))
                return
            }
            switch status {
            case "ready":
                guard http.statusCode == 200 else {
                    completion(.failed("健康检查状态不一致：status=ready 但 HTTP \(http.statusCode)"))
                    return
                }
                completion(.ready)
            case "starting":
                guard http.statusCode == 503 else {
                    completion(.failed("健康检查状态不一致：status=starting 但 HTTP \(http.statusCode)"))
                    return
                }
                completion(.starting)
            case "failed":
                guard http.statusCode == 503 else {
                    completion(.failed("健康检查状态不一致：status=failed 但 HTTP \(http.statusCode)"))
                    return
                }
                completion(.failed(json["error"] as? String ?? "后端启动失败"))
            case "forbidden":
                guard http.statusCode == 403 else {
                    completion(.failed("健康检查状态不一致：status=forbidden 但 HTTP \(http.statusCode)"))
                    return
                }
                completion(.failed("健康检查被拒绝：仅允许本机访问"))
            default:
                completion(.failed("未知健康状态：\(status)"))
            }
        }.resume()
    }
}

// MARK: - 页面容器

final class WebViewController: UIViewController {
    private let pageURL = URL(string: "http://127.0.0.1:\(backendPort)")!
    private let startupTimeout: TimeInterval = 45

    private var webView: WKWebView!
    private lazy var statusView = StatusOverlay { [weak self] in self?.requestRecovery(reason: "retry", needsPageLoad: true) }

    private var loadGeneration = 0
    // 第四阶段 C（S8/S9）：恢复协调状态。所有读写都在主线程。
    // 同一时刻只存在一个有效恢复流程：recoveryInFlight 为 true 时新请求合并，
    // 不开启新流程；recoveryNeedsPageLoad 是当前流程是否需要在就绪后加载页面
    // （合并请求可将其从 false 升级为 true）；currentNavigation 是当前恢复流程
    // 发起的页面导航，用于归属导航回调，代替只用 pageLoaded 布尔值判断。
    private var recoveryInFlight = false
    private var recoveryNeedsPageLoad = false
    private var currentNavigation: WKNavigation?
    /// 第四阶段 C 第二轮：恢复阶段（问题一）。
    /// 阶段用于区分恢复流程所处的位置，合并请求与导航回调按阶段处理，
    /// 不能只靠 recoveryInFlight 一个布尔值：
    /// idle = 无流程；checking = 检查后端状态（含异常复核等待）；
    /// waitingReady = 等待后端 ready（/healthz 轮询中）；
    /// readyToLoad = 已就绪、尚未发起导航（主线程内瞬时过渡）；
    /// loading = 页面导航加载中；loaded = 页面加载完成（终态）；
    /// failed = 流程失败或已结束（终态，含被更新的导航接管而结束）。
    private enum RecoveryPhase {
        case idle, checking, waitingReady, readyToLoad, loading, loaded, failed
    }
    private var recoveryPhase: RecoveryPhase = .idle
    /// 加载阶段收到新的页面恢复请求（典型：WebContent 在加载期间再次终止）：
    /// 当前导航若失败/超时，需以新代次重新发起一次有效加载；若当前导航成功完成，
    /// 该请求即由本次成功加载满足，随之清除。
    private var pendingReload = false
    /// 当前恢复事件内自动重新加载已执行的次数；上限见 maxReloadRetries。
    /// 仅在真正的终态（当前导航成功完成 / 流程失败结束 / 取消被接管）复位。
    private var reloadRetryCount = 0
    /// 自动重新加载上限：最多重试 2 次（原加载 + 2 次重试 = 3 次加载尝试）。
    /// 未经测量的自选值，用于防止重复终止事件造成无限加载循环；用户可调整。
    private let maxReloadRetries = 2
    /// 最新开始的主框架导航（didStartProvisionalNavigation 记录），
    /// 供 didFinish 归属用户站内导航、供取消分支确认后继导航使用；
    /// 恢复导航本身由 currentNavigation 标识。
    /// 生命周期边界：新代次开始时（startRecoveryFlow）一并清除，不保留旧代次身份。
    private var latestNavigation: WKNavigation?
    /// 恢复加载顺序计数：每次恢复流程调用 webView.load() 时递增。
    /// latestNavigationLoadOrder 是 latestNavigation 的 didStart 到达时的计数值：
    /// 与 currentLoadOrder 一致，说明该导航在当前恢复导航的 load() 之后开始，
    /// 是取消判定的后继导航候选；在 load() 之前开始的导航（前驱）计数值更小，
    /// 不得被误认为后继导航。
    /// 注意：didStart 回调到达时间只能证明回调到达时的顺序，不能证明导航的真实
    /// 开始时间——在 load() 之前开始、回调在 load() 之后才到达的导航（竞态 C）
    /// 同样会记为当前顺序号；该残留限制见交付报告，取消判定用“未观察到失败”
    /// 与延迟判定进一步收窄。
    private var recoveryLoadOrder = 0
    private var currentLoadOrder = 0
    private var latestNavigationLoadOrder = 0
    /// 最新导航的状态（第四轮：候选 → 提交 → 成功 / 失败 / 取消 / 超时六态）。
    /// candidate = didStart 已到，尚未提交（候选，未证明接管）；
    /// committed = didCommit 已到，尚未完成（已进入提交阶段，未证明加载成功）；
    /// succeeded = didFinish 已到（成功完成，唯一可确认接管的状态）；
    /// failed = didFail 已到（非取消）；cancelled = didFail(NSURLErrorCancelled) 已到；
    /// timedOut = 看门狗超时。
    /// 仅在 latestNavigation 非空时有意义；新代次开始时随 latestNavigation 一并清除。
    /// didStart / didCommit / didFinish / didFail 到达时更新本字段；
    /// 只有 .succeeded 的导航可以确认接管并消费 pendingReload。
    private enum LatestNavigationState { case candidate, committed, succeeded, failed, cancelled, timedOut }
    private var latestNavigationState: LatestNavigationState = .candidate
    /// 取消延迟判定窗口：取消回调到达时若不能确认后继导航，等待本时长，
    /// 让可能迟到的新导航开始回调有机会到达，再做决定。未经测量的自选值。
    private let cancelDecisionDelay: TimeInterval = 0.1
    /// 后台 TCP 检查代次与在途标志（问题三）：每次发起检查递增；
    /// 回调核对代次，旧检查结果直接丢弃；同一时刻最多一次检查在途。
    private var recoveryCheckGeneration = 0
    private var recoveryCheckPending = false
    /// 检查在途时合并进来的恢复需求（needsPageLoad 取或值；reason 取最新值），
    /// 检查返回后按合并后的需求决策。
    private var pendingNeedsPageLoad = false
    private var pendingReason = ""
    /// 加载阶段看门狗时限：当前导航在此时限内无任何导航回调时，按加载失败处理。
    /// 兜底 nil 导航失败等无法归属的情形；未经测量的自选值，用户可调整。
    private let navigationTimeout: TimeInterval = 30
    private var pageLoaded = false
    private var pageIsDark: Bool?
    private var isBusy = false
    private var backgroundTask: UIBackgroundTaskIdentifier = .invalid
    private var downloads: [ObjectIdentifier: URL] = [:]

    override var preferredStatusBarStyle: UIStatusBarStyle {
        guard let dark = pageIsDark else { return .default }
        return dark ? .lightContent : .darkContent
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground

        let config = WKWebViewConfiguration()
        let contentController = config.userContentController
        contentController.add(WeakScriptMessageHandler(self), name: "cfdataIOS")
        contentController.addUserScript(WKUserScript(source: Self.bridgeScript,
                                                     injectionTime: .atDocumentEnd,
                                                     forMainFrameOnly: true))

        webView = WKWebView(frame: view.bounds, configuration: config)
        PerfLog.log("webview_created", loadGen: loadGeneration)
        webView.autoresizingMask = [.flexibleWidth, .flexibleHeight]
        webView.navigationDelegate = self
        webView.uiDelegate = self
        view.addSubview(webView)

        statusView.frame = view.bounds
        statusView.autoresizingMask = [.flexibleWidth, .flexibleHeight]
        view.addSubview(statusView)

        let center = NotificationCenter.default
        center.addObserver(self, selector: #selector(appDidBecomeActive),
                           name: UIApplication.didBecomeActiveNotification, object: nil)
        center.addObserver(self, selector: #selector(appDidEnterBackground),
                           name: UIApplication.didEnterBackgroundNotification, object: nil)

        // 第二阶段：WKWebView 已创建完成；数据目录准备在后台队列进行，
        // 与主线程的 UI 初始化不再互相阻塞。
        PerfLog.log("webview_initialization_finished", loadGen: loadGeneration)
        requestRecovery(reason: "initial", needsPageLoad: true)
    }

    // MARK: 启动与恢复

    /// 统一恢复协调入口（第四阶段 C，S8/S9；第二轮：问题一 / 问题三）。
    /// 所有恢复触发点都经过这里：初始启动、重试按钮、前台恢复、WebContent 终止。
    ///
    /// 职责分离：Swift 只负责原生生命周期、后端运行状态检查与 WebView 恢复协调；
    /// 页面存活时 WebSocket 的断线检测与重连交给 JS（visibilitychange 与现有重连机制），
    /// Swift 不抢先 reload 页面；Go 负责服务生命周期与状态接口。
    ///
    /// 状态决策（running / ready 两次读取不是原子快照）：
    /// - running=true, ready=true, 不需要加载页面 → 什么都不做，直接返回。
    /// - running=true, ready=false → 等待现有后端就绪，不调用 Backend.start()。
    /// - running=false, ready=false → 进入唯一启动流程（迁移门禁 → 启动 → 等待就绪）。
    /// - running=false, ready=true → 异常状态，0.5 秒后复核一次；仍异常则报失败，不直接启动。
    /// 需要加载页面时（初始、重试、WebContent 终止），即使后端存活且 ready 也会进入流程，
    /// 但跳过 Backend.start()，就绪后只加载一次页面。
    ///
    /// 并发：同一时刻只存在一个有效恢复流程。流程进行中又有触发到达时合并进当前流程，
    /// 不开启新流程；若新请求需要加载页面，把该需求并入当前流程（就绪时一并加载）。
    /// 后台检查在途时新请求同样合并（pendingNeedsPageLoad 取或值），不发起第二次检查。
    /// 加载阶段（recoveryPhase == .loading）收到新的页面恢复请求时，另记 pendingReload：
    /// 当前导航若失败/超时，以新代次重新发起一次有效加载（上限见 maxReloadRetries）；
    /// 当前导航若成功完成，该请求即由本次加载满足，随之清除。
    ///
    /// 线程：IsReady() 的 TCP 检查只在后台队列执行（checkBackendState），
    /// 回调回主线程后重新读取 IsRunning() 并校验检查代次与恢复状态；
    /// 快路径不阻塞主线程，不用同步等待或 semaphore。
    /// 所有恢复状态读写都在主线程。
    private func requestRecovery(reason: String, needsPageLoad: Bool) {
        if recoveryInFlight {
            // 合并：不开新流程；新请求若需要页面加载，把该需求并入当前流程，
            // 页面也随之标记为未加载（当前内容已失效，等待本次流程的加载完成）。
            if needsPageLoad {
                recoveryNeedsPageLoad = true
                pageLoaded = false
                if recoveryPhase == .loading {
                    // 加载阶段的终止：当前导航可能随之中断；若它失败/超时，
                    // 需以新代次重新发起一次有效加载（见 failCurrentNavigation）。
                    pendingReload = true
                    PerfLog.log("recovery_reload_pending", loadGen: loadGeneration, "reason=\(reason)")
                }
            }
            PerfLog.log("recovery_merged", loadGen: loadGeneration, "reason=\(reason)")
            return
        }
        if recoveryCheckPending {
            // 已有一次后台检查在途：合并，不发起第二次检查；需求取或值，原因取最新值。
            if needsPageLoad {
                pendingNeedsPageLoad = true
                pageLoaded = false
            }
            pendingReason = reason
            PerfLog.log("recovery_check_merged", loadGen: loadGeneration, "reason=\(reason)")
            return
        }
        // 发起一次后台状态检查：ready 在后台队列读 TCP，running 在回调回主线程后新鲜读取。
        pendingNeedsPageLoad = needsPageLoad
        pendingReason = reason
        recoveryCheckGeneration += 1
        let checkGeneration = recoveryCheckGeneration
        recoveryCheckPending = true
        // 代次边界（第四轮）：新流程在此进入，上一代次的导航身份在此清除。
        // 此后检查期间开始的导航属于本代次，必须保留到 startRecoveryFlow；
        // 若在 startRecoveryFlow 才清除，会误删检查期间开始的用户导航，
        // 使 waitForBackend 的场景 J 检查失效（T4-17 发现的真缺陷）。
        latestNavigation = nil
        latestNavigationLoadOrder = 0
        latestNavigationState = .candidate
        PerfLog.log("recovery_check_started", loadGen: loadGeneration, "reason=\(reason)")
        checkBackendState(checkGeneration: checkGeneration) { [weak self] running, ready in
            guard let self else { return }
            guard self.recoveryCheckPending else {
                PerfLog.log("recovery_check_stale", loadGen: self.loadGeneration)
                return
            }
            self.recoveryCheckPending = false
            let needsPageLoad = self.pendingNeedsPageLoad
            self.pendingNeedsPageLoad = false
            let reason = self.pendingReason
            self.startRecoveryFlow(running: running, ready: ready, needsPageLoad: needsPageLoad, reason: reason)
        }
    }

    /// 后台读取后端状态：IsReady() 的 TCP 检查在后台队列执行，回调回主线程。
    /// running 在回调回到主线程后新鲜读取（快，无 I/O），不复用检查发起时的旧值——
    /// 检查期间后端可能退出或启动，旧快照不得作为决策依据。
    /// 回调时校验检查代次：旧检查的结果直接丢弃（当前调用图中同一时刻最多一次检查在途，
    /// 该校验为防御性保护）。
    private func checkBackendState(checkGeneration: Int, completion: @escaping (_ running: Bool, _ ready: Bool) -> Void) {
        DispatchQueue.global(qos: .userInitiated).async {
            let ready = CfdatacoreIsReady(backendPort)
            DispatchQueue.main.async { [weak self] in
                guard let self else { return }
                guard checkGeneration == self.recoveryCheckGeneration else {
                    PerfLog.log("recovery_check_stale", loadGen: self.loadGeneration)
                    return
                }
                completion(CfdatacoreIsRunning(), ready)
            }
        }
    }

    /// 新代次恢复流程的启动（仅后台检查回调调用）。
    /// 后端存活且已就绪、又不需要加载页面时，什么都不做（快路径，日志 recovery_noop_backend_alive）。
    private func startRecoveryFlow(running: Bool, ready: Bool, needsPageLoad: Bool, reason: String) {
        guard !recoveryInFlight else {
            // 防御：检查返回时已有流程在进行（当前调用图下不应发生），不开启第二条流程。
            PerfLog.log("recovery_flow_already_in_flight", loadGen: loadGeneration, "reason=\(reason)")
            return
        }
        if !needsPageLoad, running, ready {
            PerfLog.log("recovery_noop_backend_alive", loadGen: loadGeneration, "reason=\(reason)")
            return
        }
        loadGeneration += 1
        let generation = loadGeneration
        recoveryInFlight = true
        recoveryNeedsPageLoad = needsPageLoad
        recoveryPhase = .checking
        currentNavigation = nil
        // 注意：latestNavigation 不在此清除——代次边界在 requestRecovery 发起检查时；
        // 检查期间开始的导航属于本代次，清除会误删它（见 requestRecovery 注释）。
        pendingReload = false
        if needsPageLoad { pageLoaded = false }
        PerfLog.log("recovery_started", loadGen: generation, "reason=\(reason) needsPageLoad=\(needsPageLoad)")
        if needsPageLoad { statusView.showLoading() }
        beginRecoveryPath(generation: generation,
                          deadline: Date().addingTimeInterval(startupTimeout),
                          running: running, ready: ready, rechecked: false)
    }

    /// 按（新鲜读取的）后端状态决定恢复路径。
    /// running/ready 由调用方在决策点新鲜读取，不复用任何旧快照；
    /// 异常状态（未运行但端口就绪）只复核一次：rechecked=true 时仍异常则报失败，不直接启动。
    /// 复核本身也是后台检查（recheckBackendState），不阻塞主线程。
    private func beginRecoveryPath(generation: Int, deadline: Date, running: Bool, ready: Bool, rechecked: Bool) {
        if running {
            // 后端存活：不调用 Backend.start()，直接等待就绪。
            PerfLog.log("recovery_reuse_backend", loadGen: generation)
            waitForBackend(generation: generation, deadline: deadline, allowRestart: true)
            return
        }
        if !ready {
            // 后端已退出：走统一启动流程。
            // 严格顺序：数据目录准备及迁移完成 → Backend.start()（Go 读配置）→ /healthz ready → 按需加载。
            // prepareDataDirectory 的回调在主线程调用，因此 Backend.start() 仍在主线程执行，与原来一致。
            Backend.prepareDataDirectory(loadGen: generation) { [weak self] result in
                guard let self, generation == self.loadGeneration else { return }
                switch result {
                case .failure(let error):
                    // 迁移失败：不启动后端、不让 Go 创建新配置，向 UI 返回明确错误；重试会重新迁移。
                    self.endRecoveryFlowAsFailed()
                    self.statusView.showFailure("数据目录准备失败：\(error.localizedDescription)")
                case .success:
                    if let error = Backend.start() {
                        PerfLog.log("backend_start_failed", loadGen: generation, error)
                        self.endRecoveryFlowAsFailed()
                        self.statusView.showFailure(error)
                        return
                    }
                    PerfLog.log("backend_start_returned", loadGen: generation)
                    self.waitForBackend(generation: generation, deadline: deadline, allowRestart: true)
                }
            }
            return
        }
        // running=false 且 ready=true：异常状态（端口已被占用但后端未运行），审慎处理。
        if !rechecked {
            PerfLog.log("recovery_anomalous_state", loadGen: generation, "running=false ready=true; rechecking")
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) { [weak self] in
                guard let self, generation == self.loadGeneration, self.recoveryInFlight else { return }
                self.recheckBackendState(generation: generation, deadline: deadline)
            }
            return
        }
        PerfLog.log("recovery_anomalous_state_persistent", loadGen: generation)
        endRecoveryFlowAsFailed()
        statusView.showFailure("后端状态异常：端口已被占用但后端未运行，请重试")
    }

    /// 异常状态的复核：重新做一次后台状态检查（不直接读 ready，避免主线程阻塞）。
    /// 复核在已有流程内：复用当前代次与截止时间；needsPageLoad 取流程当前值（可能已被合并升级）。
    private func recheckBackendState(generation: Int, deadline: Date) {
        recoveryCheckGeneration += 1
        let checkGeneration = recoveryCheckGeneration
        recoveryCheckPending = true
        checkBackendState(checkGeneration: checkGeneration) { [weak self] running, ready in
            guard let self else { return }
            guard self.recoveryCheckPending else {
                PerfLog.log("recovery_check_stale", loadGen: self.loadGeneration)
                return
            }
            self.recoveryCheckPending = false
            guard generation == self.loadGeneration, self.recoveryInFlight else { return }
            self.beginRecoveryPath(generation: generation, deadline: deadline,
                                   running: running, ready: ready, rechecked: true)
        }
    }

    /// 结束当前恢复流程（失败终态）：复位流程标志、终态、重试计数与当前导航身份。
    /// 页面状态与失败界面由调用方决定（本函数不碰 pageLoaded，不显示失败界面）。
    /// currentNavigation 必须清掉：已失败的导航不会再有回调，残留的身份会让
    /// 迟到的旧回调被误认为当前导航。
    private func endRecoveryFlowAsFailed() {
        recoveryInFlight = false
        recoveryPhase = .failed
        pendingReload = false
        reloadRetryCount = 0
        currentNavigation = nil
    }

    /// 当前恢复导航终结（失败 / 超时）的公共处理。
    /// 调用前已确认无候选导航可接管转移（取消 / 失败 / 看门狗三处均先检查后继，
    /// 在途候选会接管转移而不是走到这里）；若加载阶段曾收到新的页面恢复请求
    /// （pendingReload）且自动重试未超限，以新代次重新发起一次有效加载，
    /// 不显示失败界面；否则结束流程并显示失败界面。
    /// 取消无后继时也经由这里（延迟判定后）；取消被接管转移时不经由这里。
    private func failCurrentNavigation(message: String) {
        if pendingReload, reloadRetryCount < maxReloadRetries {
            reloadRetryCount += 1
            pendingReload = false
            currentNavigation = nil
            recoveryInFlight = false
            recoveryPhase = .checking
            PerfLog.log("recovery_reload_retry", loadGen: loadGeneration, "attempt=\(reloadRetryCount)")
            requestRecovery(reason: "reload_after_nav_failed", needsPageLoad: true)
            return
        }
        endRecoveryFlowAsFailed()
        pageLoaded = false
        statusView.showFailure(message)
    }

    /// 加载阶段看门狗：当前导航在 navigationTimeout 内无任何导航回调时，按加载失败处理。
    /// 兜底 nil 导航失败等无法归属的情形；导航已被接管或流程已结束时不触发。
    /// 触发时先看是否有候选导航可接管转移（第四轮）；无候选时才按失败处理。
    private func scheduleNavigationWatchdog(generation: Int, navigation: WKNavigation) {
        DispatchQueue.main.asyncAfter(deadline: .now() + navigationTimeout) { [weak self] in
            guard let self,
                  generation == self.loadGeneration,
                  self.recoveryInFlight,
                  self.recoveryPhase == .loading,
                  self.currentNavigation === navigation else { return }
            // 超时：先把超时导航记为 .timedOut，再看是否有候选导航可接管转移；
            // 无候选时才按加载失败处理。
            if let latest = self.latestNavigation, latest === navigation {
                self.latestNavigationState = .timedOut
            }
            if let (candidate, status) = self.confirmedSuccessor(excluding: navigation) {
                switch status {
                case .finished:
                    PerfLog.log("page_load_watchdog_timeout_successor_finished", loadGen: generation)
                    self.endCancelAsTakeover()
                case .inProgress:
                    PerfLog.log("page_load_watchdog_timeout_transfer", loadGen: generation)
                    self.transferToCandidate(candidate)
                }
                return
            }
            PerfLog.log("page_load_watchdog_timeout", loadGen: generation)
            self.failCurrentNavigation(message: "页面加载超时，请重试")
        }
    }

    /// 用 /healthz 轮询代替 TCP 端口判断：ready 后按当前流程需求决定是否加载页面。
    /// 每轮只发一次请求，上一轮完成后再约 0.1 秒发下一轮，不重叠、不固定等待；
    /// 旧代次的回调一律丢弃；同一代次只会在 ready 时加载一次。
    /// 阶段：进入本函数即为 waitingReady；ready 后需加载时经 readyToLoad 进入 loading，
    /// 无需加载时直接结束（loaded）；超时或失败时结束（failed）。
    /// 结束恢复流程时必须经 endRecoveryFlowAsFailed()（或 loaded 终态）复位，
    /// 否则后续恢复请求会一直合并进死流程。
    private func waitForBackend(generation: Int, deadline: Date, allowRestart: Bool, healthAttempts: Int = 0) {
        if Date() > deadline {
            PerfLog.log("backend_timeout", loadGen: generation)
            endRecoveryFlowAsFailed()
            statusView.showFailure("后端服务启动超时，请检查网络后重试")
            return
        }
        recoveryPhase = .waitingReady
        let attempt = healthAttempts + 1
        if attempt == 1 {
            PerfLog.log("health_check_attempt", loadGen: generation, "attempt=1")
        }
        Backend.checkHealth { [weak self] status in
            DispatchQueue.main.async {
                guard let self, generation == self.loadGeneration else { return }
                switch status {
                case .ready:
                    PerfLog.log("backend_http_ready", loadGen: generation, "attempts=\(attempt)")
                    if self.recoveryNeedsPageLoad {
                        self.statusView.hide()
                        PerfLog.log("initial_page_load_requested", loadGen: generation)
                        self.recoveryPhase = .readyToLoad
                        // J：就绪时若已有用户导航在途，不用恢复加载覆盖它；转由该导航接管流程。
                        // 该导航在等待阶段开始（未调用 load()），转移后它成为 currentNavigation，
                        // 其结果决定流程走向（成功则满足请求，失败则重试）。
                        if let userNav = self.latestNavigation,
                           self.latestNavigationState == .candidate || self.latestNavigationState == .committed {
                            self.currentNavigation = userNav
                            self.recoveryPhase = .loading
                            self.scheduleNavigationWatchdog(generation: generation, navigation: userNav)
                            PerfLog.log("recovery_transferred_to_user_nav", loadGen: generation)
                            return
                        }
                        if let navigation = self.webView.load(URLRequest(url: self.pageURL)) {
                            // 恢复加载顺序号：本次 load() 的顺序，供取消判定确认后继导航。
                            self.recoveryLoadOrder += 1
                            self.currentLoadOrder = self.recoveryLoadOrder
                            self.currentNavigation = navigation
                            self.recoveryPhase = .loading
                            self.scheduleNavigationWatchdog(generation: generation, navigation: navigation)
                        } else {
                            // load 未能创建导航对象：按加载失败处理，结束本代次恢复流程。
                            self.currentNavigation = nil
                            self.endRecoveryFlowAsFailed()
                            self.statusView.showFailure("页面加载失败：无法创建导航")
                        }
                    } else {
                        // 仅等待后端就绪（前台恢复且页面存活）：不加载页面，恢复流程结束。
                        PerfLog.log("backend_ready_no_page_load", loadGen: generation)
                        self.pendingReload = false
                        self.reloadRetryCount = 0
                        self.currentNavigation = nil
                        self.recoveryInFlight = false
                        self.recoveryPhase = .loaded
                    }
                case .starting:
                    if Date() > deadline {
                        PerfLog.log("backend_timeout", loadGen: generation)
                        self.endRecoveryFlowAsFailed()
                        self.statusView.showFailure("后端服务启动超时，请检查网络后重试")
                        return
                    }
                    DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) {
                        self.waitForBackend(generation: generation, deadline: deadline,
                                            allowRestart: allowRestart, healthAttempts: attempt)
                    }
                case .unreachable:
                    // 服务尚未监听。若后端 goroutine 已退出，重启一次；
                    // 否则继续等（监听中或正在启动）。
                    if !CfdatacoreIsRunning() {
                        PerfLog.log("backend_not_running", loadGen: generation)
                        // 迁移已完成（进程级状态，不随代次重置），此处直接 start() 不再重复迁移。
                        if allowRestart, Backend.start() == nil {
                            self.waitForBackend(generation: generation, deadline: deadline,
                                                allowRestart: false, healthAttempts: attempt)
                            return
                        }
                        let reason = CfdatacoreLastError()
                        self.endRecoveryFlowAsFailed()
                        self.statusView.showFailure("\(reason.isEmpty ? "后端服务未运行" : reason)\n（端口 \(backendPort) 可能被占用）")
                        return
                    }
                    if Date() > deadline {
                        PerfLog.log("backend_timeout", loadGen: generation)
                        self.endRecoveryFlowAsFailed()
                        self.statusView.showFailure("后端服务启动超时，请检查网络后重试")
                        return
                    }
                    DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) {
                        self.waitForBackend(generation: generation, deadline: deadline,
                                            allowRestart: allowRestart, healthAttempts: attempt)
                    }
                case .failed(let message):
                    PerfLog.log("backend_http_failed", loadGen: generation, message)
                    // 后端明确报告失败且已退出时，允许重启一次；否则直接报失败，不无限循环。
                    if !CfdatacoreIsRunning(), allowRestart, Backend.start() == nil {
                        self.waitForBackend(generation: generation, deadline: deadline,
                                            allowRestart: false, healthAttempts: attempt)
                        return
                    }
                    self.endRecoveryFlowAsFailed()
                    self.statusView.showFailure("后端服务启动失败：\(message)")
                }
            }
        }
    }

    @objc private func appDidBecomeActive() {
        PerfLog.log("app_become_active", loadGen: loadGeneration)
        endBackgroundTask()
        // 仅在页面已加载过且未显示启动/失败界面时检查；启动中或失败界面由自身流程处理。
        // 页面存活时 WebSocket 恢复交给 JS（visibilitychange 与现有重连机制）；
        // Swift 只在后端确实不可用时介入，且不只凭一次 TCP 未就绪就 reload 页面。
        guard pageLoaded, statusView.isHidden else { return }
        requestRecovery(reason: "app_active", needsPageLoad: false)
    }

    @objc private func appDidEnterBackground() {
        PerfLog.log("app_enter_background", loadGen: loadGeneration)
        // 扫描进行中时申请有限的后台执行时间，降低被立即挂起导致中断的概率。
        // 正常情况下会在收到 busy=true 时提前申请，这里作为兜底。
        guard isBusy else { return }
        beginBackgroundTaskIfNeeded()
    }

    private func beginBackgroundTaskIfNeeded() {
        guard backgroundTask == .invalid else { return }
        backgroundTask = UIApplication.shared.beginBackgroundTask(withName: "CFDataScan") { [weak self] in
            self?.endBackgroundTask()
        }
    }

    private func endBackgroundTask() {
        guard backgroundTask != .invalid else { return }
        UIApplication.shared.endBackgroundTask(backgroundTask)
        backgroundTask = .invalid
    }

    // MARK: 工具

    private func isExternal(_ url: URL) -> Bool {
        guard let scheme = url.scheme?.lowercased() else { return false }
        if ["about", "blob", "data", "javascript"].contains(scheme) { return false }
        if scheme == "http" || scheme == "https" {
            let host = url.host?.lowercased() ?? ""
            return host != "127.0.0.1" && host != "localhost"
        }
        return true
    }

    private func openExternally(_ url: URL) {
        UIApplication.shared.open(url, options: [:], completionHandler: nil)
    }

    private var topViewController: UIViewController {
        var top: UIViewController = self
        while let presented = top.presentedViewController, !presented.isBeingDismissed {
            top = presented
        }
        return top
    }

    private func presentOnTop(_ controller: UIViewController) {
        if let popover = controller.popoverPresentationController {
            popover.sourceView = view
            popover.sourceRect = CGRect(x: view.bounds.midX, y: view.bounds.midY, width: 0, height: 0)
            popover.permittedArrowDirections = []
        }
        topViewController.present(controller, animated: true)
    }

    private func showMessage(_ title: String, _ message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: "确定", style: .default))
        presentOnTop(alert)
    }

    private func share(_ file: URL) {
        let activity = UIActivityViewController(activityItems: [file], applicationActivities: nil)
        activity.completionWithItemsHandler = { _, _, _, _ in
            try? FileManager.default.removeItem(at: file.deletingLastPathComponent())
        }
        presentOnTop(activity)
    }

    /// 注入页面：同步主题（状态栏）与扫描状态（防锁屏/后台时间），无需修改共用前端代码
    private static let bridgeScript = """
    (function () {
      function post(msg) {
        try { window.webkit.messageHandlers.cfdataIOS.postMessage(msg); } catch (e) {}
      }
      var root = document.documentElement;
      var lastDark = null;
      function syncTheme() {
        var dark = root.classList.contains('dark-theme');
        if (dark !== lastDark) { lastDark = dark; post({ type: 'theme', dark: dark }); }
      }
      new MutationObserver(syncTheme).observe(root, { attributes: true, attributeFilter: ['class'] });
      syncTheme();
      var lastBusy = null;
      setInterval(function () {
        var busy = false;
        try { busy = busy || (typeof isTaskRunning !== 'undefined' && !!isTaskRunning); } catch (e) {}
        try { busy = busy || (typeof officialBatchSpeedRunning !== 'undefined' && !!officialBatchSpeedRunning); } catch (e) {}
        try { busy = busy || (typeof nsbBatchSpeedRunning !== 'undefined' && !!nsbBatchSpeedRunning); } catch (e) {}
        try { busy = busy || (typeof nsbReallinkRunning !== 'undefined' && !!nsbReallinkRunning); } catch (e) {}
        if (busy !== lastBusy) { lastBusy = busy; post({ type: 'busy', busy: busy }); }
      }, 1000);
    })();
    """
}

// MARK: - JS 桥

extension WebViewController: WKScriptMessageHandler {
    func userContentController(_ userContentController: WKUserContentController,
                               didReceive message: WKScriptMessage) {
        guard let body = message.body as? [String: Any], let type = body["type"] as? String else { return }
        switch type {
        case "theme":
            let dark = body["dark"] as? Bool ?? false
            pageIsDark = dark
            view.backgroundColor = dark ? UIColor(red: 0.06, green: 0.09, blue: 0.16, alpha: 1) : .white
            setNeedsStatusBarAppearanceUpdate()
        case "busy":
            isBusy = body["busy"] as? Bool ?? false
            UIApplication.shared.isIdleTimerDisabled = isBusy   // 仅扫描期间防止锁屏
            if isBusy {
                // 在前台任务刚开始时就申请后台时间，避免等到进入后台才申请过晚。
                beginBackgroundTaskIfNeeded()
            } else {
                endBackgroundTask()
            }
        default:
            break
        }
    }
}

/// 避免 WKUserContentController 强引用控制器造成循环引用
private final class WeakScriptMessageHandler: NSObject, WKScriptMessageHandler {
    weak var target: WKScriptMessageHandler?
    init(_ target: WKScriptMessageHandler) { self.target = target }
    func userContentController(_ controller: WKUserContentController, didReceive message: WKScriptMessage) {
        target?.userContentController(controller, didReceive: message)
    }
}

// MARK: - 导航

extension WebViewController: WKNavigationDelegate {
    func webView(_ webView: WKWebView, decidePolicyFor action: WKNavigationAction,
                 preferences: WKWebpagePreferences,
                 decisionHandler: @escaping (WKNavigationActionPolicy, WKWebpagePreferences) -> Void) {
        if action.shouldPerformDownload {
            decisionHandler(.download, preferences)
            return
        }
        let isMainFrame = action.targetFrame?.isMainFrame ?? true
        if isMainFrame, let url = action.request.url, isExternal(url) {
            openExternally(url)
            decisionHandler(.cancel, preferences)
            return
        }
        decisionHandler(.allow, preferences)
    }

    func webView(_ webView: WKWebView, decidePolicyFor response: WKNavigationResponse,
                 decisionHandler: @escaping (WKNavigationResponsePolicy) -> Void) {
        decisionHandler(response.canShowMIMEType ? .allow : .download)
    }

    func webView(_ webView: WKWebView, navigationAction: WKNavigationAction, didBecome download: WKDownload) {
        download.delegate = self
    }

    func webView(_ webView: WKWebView, navigationResponse: WKNavigationResponse, didBecome download: WKDownload) {
        download.delegate = self
    }

    func webView(_ webView: WKWebView, didStartProvisionalNavigation navigation: WKNavigation!) {
        guard let navigation else { return }
        // 记录最新开始的主框架导航及其到达时的加载顺序号，状态为候选；
        // 开始只建立候选记录，不能证明接管成功，不修改恢复流程状态。
        // 顺序号只能证明回调到达时在当前恢复加载之后，不能证明导航的真实开始时间
        // （竞态 C 见交付报告）；取消判定另有“未观察到失败”与接管转移收窄。
        latestNavigation = navigation
        latestNavigationLoadOrder = recoveryLoadOrder
        latestNavigationState = .candidate
        PerfLog.log("nav_provisional_started", loadGen: loadGeneration)
    }

    func webView(_ webView: WKWebView, didCommit navigation: WKNavigation!) {
        guard let navigation else { return }
        // 只做归属校验与状态记录：内容开始到达不等于加载完成，
        // 旧导航的提交更不能影响新代次；不修改任何恢复流程状态。
        // 最新导航进入提交阶段时记为 .committed（已提交，未证明加载成功）。
        if navigation === latestNavigation, latestNavigationState == .candidate {
            latestNavigationState = .committed
        }
        if navigation === currentNavigation {
            PerfLog.log("nav_committed_current", loadGen: loadGeneration)
        }
    }

    func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
        guard let navigation else { return }
        if navigation === currentNavigation {
            // 当前恢复导航完成：结束本代次恢复流程；加载阶段收到的新恢复请求
            // 已由本次成功加载满足，随之清除。
            PerfLog.log("page_load_finished", loadGen: loadGeneration)
            if latestNavigation === navigation { latestNavigationState = .succeeded }
            pendingReload = false
            reloadRetryCount = 0
            currentNavigation = nil
            recoveryInFlight = false
            recoveryPhase = .loaded
            pageLoaded = true
            return
        }
        // 用户站内导航：只有最新记录的导航（新代次开始时已清除旧记录），
        // 其完成才更新页面存活状态；旧导航 / 无关导航一律忽略。
        // 页面存活状态（pageLoaded）与导航完成状态的区别：前者是 WebView 当前是否
        // 显示已加载页面这一应用级状态，只由恢复流程事件与最新导航的完成维护；
        // 后者是单个导航是否完成。旧导航的完成不能证明当前页面存活。
        // 只有成功完成（didFinish）才能确认接管并满足恢复请求；didStart / didCommit 不能。
        if navigation === latestNavigation {
            latestNavigationState = .succeeded
            pageLoaded = true
        }
    }

    func webView(_ webView: WKWebView, didFailProvisionalNavigation navigation: WKNavigation!, withError error: Error) {
        handleNavigationFailed(navigation, error: error, provisional: true)
    }

    func webView(_ webView: WKWebView, didFail navigation: WKNavigation!, withError error: Error) {
        handleNavigationFailed(navigation, error: error, provisional: false)
    }

    /// 后继导航状态：inProgress = 候选或已提交（在途，未证明接管成功）；
    /// finished = 已成功完成（页面已加载，唯一可确认接管的状态）。
    private enum SuccessorStatus { case inProgress, finished }

    /// 确认的后继导航（若存在）：返回导航对象与其状态。
    /// 确认条件（缺一不可）：latestNavigation 存在且不是被排除的导航；
    /// 其 didStart 在当前恢复导航的 load() 之后到达（顺序号一致）；
    /// 未被观察到失败 / 取消 / 超时。
    /// 注意：顺序号不是 WebKit 原生导航身份（见交付报告 §3.4）；
    /// .inProgress 只表示候选在途，不表示接管成功——接管成功只由 didFinish 确认。
    private func confirmedSuccessor(excluding nav: WKNavigation) -> (navigation: WKNavigation, status: SuccessorStatus)? {
        guard let latest = latestNavigation,
              latest !== nav,
              latestNavigationLoadOrder == currentLoadOrder else { return nil }
        switch latestNavigationState {
        case .candidate, .committed: return (latest, .inProgress)
        case .succeeded: return (latest, .finished)
        case .failed, .cancelled, .timedOut: return nil
        }
    }

    /// 将恢复流程的当前导航转移给候选导航（接管转移，非接管成功）。
    /// 流程保持 inFlight、阶段保持 .loading；pendingReload 不清除（请求未被满足，
    /// 由候选导航的成功完成消费，或由其失败触发重试）；
    /// 候选导航成为 currentNavigation，其成功 / 失败 / 取消 / 超时按当前导航处理；
    /// 看门狗为候选导航重新排期（原导航的看门狗因身份守卫失效，不再触发）。
    /// 仅在候选导航已确认且状态为 .inProgress 时调用。
    private func transferToCandidate(_ candidate: WKNavigation) {
        currentNavigation = candidate
        recoveryPhase = .loading
        scheduleNavigationWatchdog(generation: loadGeneration, navigation: candidate)
    }

    /// 后继导航已成功完成时的接管终态：清除挂起的重新加载（接管导航即页面加载），
    /// 结束流程，不显示失败界面。
    /// 仅在 confirmedSuccessor 确认后继为 .finished 时调用——后继的 didFinish 已执行，
    /// pageLoaded 已为 true，因此阶段置为 .loaded（成功终态），不是 .failed。
    /// （第四轮误置为 .failed，造成“页面已加载但恢复阶段为失败”的不一致状态；
    /// 第五轮修正。）
    /// 四终态的身份清理与此一致：清 currentNavigation、pendingReload、重试计数，
    /// 置 inFlight=false 与终态；区别只在 pageLoaded 与失败界面（见各终态）。
    private func endCancelAsTakeover() {
        pendingReload = false
        reloadRetryCount = 0
        currentNavigation = nil
        recoveryInFlight = false
        recoveryPhase = .loaded
    }

    /// 导航失败公共处理（didFailProvisionalNavigation 与 didFail 共用）。
    /// 判断顺序：先归属，后处理。
    /// 1. navigation == nil：无法可靠归属，一律忽略（诊断保留错误域）。
    /// 2. navigation !== currentNavigation：旧导航 / 无关导航。只更新最新导航的
    ///    状态记录（若是它，按取消 / 失败分别记为 .cancelled / .failed，供接管判定使用），
    ///    不碰恢复流程状态。
    /// 3. 当前导航失败：按后继状态处理——后继已完成则静默结束；后继在途则接管转移
    ///    （流程保持在途，等待候选导航的结果）；无后继则按失败处理。
    ///    取消与非取消失败均如此：候选导航的在途状态绝不解释为接管成功。
    /// 4. NSURLErrorDomain 下的 NSURLErrorCancelled 单独处理（接管判定 + 延迟判定）。
    /// 5. 其他错误域的当前导航失败同样先查后继，再决定转移或结束流程；
    ///    诊断保留错误域、错误码与错误描述，不笼统归为网络错误。
    /// 残留限制：didStart 回调到达时间不能证明导航真实开始时间（竞态 C）；
    /// 接管转移使该残留不再导致请求丢失（候选失败会触发重试），见交付报告。
    private func handleNavigationFailed(_ navigation: WKNavigation?, error: Error, provisional: Bool) {
        let nsError = error as NSError
        guard let navigation else {
            PerfLog.log("nav_failure_unattributed_nil", loadGen: loadGeneration, "provisional=\(provisional) domain=\(nsError.domain)")
            return
        }
        guard navigation === currentNavigation else {
            if navigation === latestNavigation {
                latestNavigationState = (nsError.domain == NSURLErrorDomain && nsError.code == NSURLErrorCancelled) ? .cancelled : .failed
            }
            return
        }
        if latestNavigation === navigation {
            latestNavigationState = (nsError.domain == NSURLErrorDomain && nsError.code == NSURLErrorCancelled) ? .cancelled : .failed
        }
        if nsError.domain == NSURLErrorDomain, nsError.code == NSURLErrorCancelled {
            // 取消：按后继状态处理；无后继时延迟判定，给迟到的新导航开始回调留窗口。
            if let (candidate, status) = confirmedSuccessor(excluding: navigation) {
                switch status {
                case .finished:
                    PerfLog.log("page_load_cancelled", loadGen: loadGeneration)
                    endCancelAsTakeover()
                case .inProgress:
                    PerfLog.log("page_load_cancelled_transfer", loadGen: loadGeneration)
                    transferToCandidate(candidate)
                }
                return
            }
            let generation = loadGeneration
            let description = nsError.localizedDescription
            DispatchQueue.main.asyncAfter(deadline: .now() + cancelDecisionDelay) { [weak self] in
                guard let self, generation == self.loadGeneration, self.recoveryInFlight,
                      self.recoveryPhase == .loading, self.currentNavigation === navigation else { return }
                if let (candidate, status) = self.confirmedSuccessor(excluding: navigation) {
                    switch status {
                    case .finished:
                        PerfLog.log("page_load_cancelled", loadGen: generation)
                        self.endCancelAsTakeover()
                    case .inProgress:
                        PerfLog.log("page_load_cancelled_transfer", loadGen: generation)
                        self.transferToCandidate(candidate)
                    }
                } else {
                    PerfLog.log("page_load_cancelled_no_successor", loadGen: generation)
                    self.failCurrentNavigation(message: "页面加载失败：\(description)")
                }
            }
            return
        }
        // 其他错误：当前导航失败。先查后继（转移或静默结束），无后继才按失败处理。
        if let (candidate, status) = confirmedSuccessor(excluding: navigation) {
            switch status {
            case .finished:
                PerfLog.log("page_load_failed_successor_finished", loadGen: loadGeneration)
                endCancelAsTakeover()
            case .inProgress:
                PerfLog.log("page_load_failed_transfer", loadGen: loadGeneration)
                transferToCandidate(candidate)
            }
            return
        }
        PerfLog.log("page_load_failed", loadGen: loadGeneration, "\(nsError.domain):\(nsError.code) \(nsError.localizedDescription)")
        failCurrentNavigation(message: "页面加载失败：\(nsError.localizedDescription)")
    }

    func webViewWebContentProcessDidTerminate(_ webView: WKWebView) {
        PerfLog.log("webcontent_terminated", loadGen: loadGeneration)
        // WebContent 终止不等于 Go 后端退出：先检查后端状态，存活则复用，只恢复页面；
        // 后端已退出时进入统一启动协调流程；已有恢复流程在进行时合并，不重复启动。
        requestRecovery(reason: "webcontent_terminated", needsPageLoad: true)
    }
}

// MARK: - JS 弹窗与新窗口

extension WebViewController: WKUIDelegate {
    func webView(_ webView: WKWebView, createWebViewWith configuration: WKWebViewConfiguration,
                 for action: WKNavigationAction, windowFeatures: WKWindowFeatures) -> WKWebView? {
        // target="_blank" / window.open：外链交给 Safari，本地链接在当前页面打开
        if let url = action.request.url {
            if isExternal(url) {
                openExternally(url)
            } else {
                webView.load(action.request)
            }
        }
        return nil
    }

    func webView(_ webView: WKWebView, runJavaScriptAlertPanelWithMessage message: String,
                 initiatedByFrame frame: WKFrameInfo, completionHandler: @escaping () -> Void) {
        let alert = UIAlertController(title: nil, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: "确定", style: .default) { _ in completionHandler() })
        presentOnTop(alert)
    }

    func webView(_ webView: WKWebView, runJavaScriptConfirmPanelWithMessage message: String,
                 initiatedByFrame frame: WKFrameInfo, completionHandler: @escaping (Bool) -> Void) {
        let alert = UIAlertController(title: nil, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: "取消", style: .cancel) { _ in completionHandler(false) })
        alert.addAction(UIAlertAction(title: "确定", style: .default) { _ in completionHandler(true) })
        presentOnTop(alert)
    }

    func webView(_ webView: WKWebView, runJavaScriptTextInputPanelWithPrompt prompt: String,
                 defaultText: String?, initiatedByFrame frame: WKFrameInfo,
                 completionHandler: @escaping (String?) -> Void) {
        let alert = UIAlertController(title: nil, message: prompt, preferredStyle: .alert)
        alert.addTextField { $0.text = defaultText }
        alert.addAction(UIAlertAction(title: "取消", style: .cancel) { _ in completionHandler(nil) })
        alert.addAction(UIAlertAction(title: "确定", style: .default) { [weak alert] _ in
            completionHandler(alert?.textFields?.first?.text ?? "")
        })
        presentOnTop(alert)
    }
}

// MARK: - 导出下载（blob）→ 系统分享面板

extension WebViewController: WKDownloadDelegate {
    func download(_ download: WKDownload, decideDestinationUsing response: URLResponse,
                  suggestedFilename: String, completionHandler: @escaping (URL?) -> Void) {
        // 每个下载使用独立临时目录，支持并发导出且不会互相覆盖
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString, isDirectory: true)
        do {
            try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        } catch {
            completionHandler(nil)
            return
        }
        let name = suggestedFilename.isEmpty ? "cfdata-export" : suggestedFilename
        let dest = dir.appendingPathComponent(name)
        downloads[ObjectIdentifier(download)] = dest
        completionHandler(dest)
    }

    func downloadDidFinish(_ download: WKDownload) {
        guard let file = downloads.removeValue(forKey: ObjectIdentifier(download)) else { return }
        share(file)
    }

    func download(_ download: WKDownload, didFailWithError error: Error, resumeData: Data?) {
        if let file = downloads.removeValue(forKey: ObjectIdentifier(download)) {
            try? FileManager.default.removeItem(at: file.deletingLastPathComponent())
        }
        showMessage("导出失败", error.localizedDescription)
    }
}

// MARK: - 启动 / 错误覆盖层

private final class StatusOverlay: UIView {
    private let spinner = UIActivityIndicatorView(style: .large)
    private let titleLabel = UILabel()
    private let messageLabel = UILabel()
    private let retryButton = UIButton(type: .system)

    init(onRetry: @escaping () -> Void) {
        super.init(frame: .zero)
        backgroundColor = .systemBackground

        titleLabel.font = .preferredFont(forTextStyle: .headline)
        titleLabel.textAlignment = .center
        messageLabel.font = .preferredFont(forTextStyle: .subheadline)
        messageLabel.textColor = .secondaryLabel
        messageLabel.textAlignment = .center
        messageLabel.numberOfLines = 0
        retryButton.setTitle("重试", for: .normal)
        retryButton.titleLabel?.font = .preferredFont(forTextStyle: .headline)
        retryButton.addAction(UIAction { _ in onRetry() }, for: .touchUpInside)

        let stack = UIStackView(arrangedSubviews: [spinner, titleLabel, messageLabel, retryButton])
        stack.axis = .vertical
        stack.alignment = .center
        stack.spacing = 12
        stack.translatesAutoresizingMaskIntoConstraints = false
        addSubview(stack)
        NSLayoutConstraint.activate([
            stack.centerXAnchor.constraint(equalTo: centerXAnchor),
            stack.centerYAnchor.constraint(equalTo: centerYAnchor),
            stack.leadingAnchor.constraint(greaterThanOrEqualTo: layoutMarginsGuide.leadingAnchor),
            stack.trailingAnchor.constraint(lessThanOrEqualTo: layoutMarginsGuide.trailingAnchor),
        ])
    }

    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }

    func showLoading() {
        isHidden = false
        spinner.startAnimating()
        spinner.isHidden = false
        titleLabel.text = "正在启动服务…"
        messageLabel.text = nil
        messageLabel.isHidden = true
        retryButton.isHidden = true
    }

    func showFailure(_ message: String) {
        isHidden = false
        spinner.stopAnimating()
        spinner.isHidden = true
        titleLabel.text = "服务不可用"
        messageLabel.text = message
        messageLabel.isHidden = false
        retryButton.isHidden = false
    }

    func hide() {
        spinner.stopAnimating()
        isHidden = true
    }
}
