import UIKit
import WebKit
import Cfdatacore

private let backendPort = 13335

@main
class AppDelegate: UIResponder, UIApplicationDelegate {
    var window: UIWindow?

    func application(_ application: UIApplication,
                     didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]? = nil) -> Bool {
        window = UIWindow(frame: UIScreen.main.bounds)
        window?.rootViewController = WebViewController()
        window?.makeKeyAndVisible()
        return true
    }
}

// MARK: - Go 后端

enum Backend {
    /// 私有数据目录（Application Support/CFData），不在“文件”App 中暴露，避免泄露配置中的 Token
    static let dataDirectory: URL = {
        let fm = FileManager.default
        let base = fm.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
        let dir = base.appendingPathComponent("CFData", isDirectory: true)
        try? fm.createDirectory(at: dir, withIntermediateDirectories: true)
        migrateLegacyDocuments(to: dir)
        return dir
    }()

    /// 旧版本把数据写在 Documents，首次启动时迁移到私有目录
    private static func migrateLegacyDocuments(to dir: URL) {
        let fm = FileManager.default
        let docs = fm.urls(for: .documentDirectory, in: .userDomainMask)[0]
        guard let items = try? fm.contentsOfDirectory(at: docs, includingPropertiesForKeys: nil) else { return }
        for item in items {
            let dest = dir.appendingPathComponent(item.lastPathComponent)
            if !fm.fileExists(atPath: dest.path) {
                try? fm.moveItem(at: item, to: dest)
            }
        }
    }

    /// 启动（或在已退出时重启）后端；返回错误描述
    static func start() -> String? {
        // gomobile 导出的是 C 函数，Swift 不会自动转换为 throws，需要显式传入 NSError 指针
        var error: NSError?
        if CfdatacoreStart(dataDirectory.path, backendPort, &error) {
            return nil
        }
        return error?.localizedDescription ?? "后端启动失败"
    }
}

// MARK: - 页面容器

final class WebViewController: UIViewController {
    private let pageURL = URL(string: "http://127.0.0.1:\(backendPort)")!
    private let startupTimeout: TimeInterval = 45

    private var webView: WKWebView!
    private lazy var statusView = StatusOverlay { [weak self] in self?.startBackendAndLoad() }

    private var loadGeneration = 0
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

        startBackendAndLoad()
    }

    // MARK: 启动与恢复

    /// 启动后端并等待端口就绪后加载页面；失败或超时显示错误与重试按钮
    private func startBackendAndLoad() {
        loadGeneration += 1
        let generation = loadGeneration
        statusView.showLoading()
        if let error = Backend.start() {
            statusView.showFailure(error)
            return
        }
        waitForBackend(generation: generation,
                       deadline: Date().addingTimeInterval(startupTimeout),
                       allowRestart: true)
    }

    private func waitForBackend(generation: Int, deadline: Date, allowRestart: Bool) {
        DispatchQueue.global(qos: .userInitiated).async {
            let ready = CfdatacoreIsReady(backendPort)
            let running = CfdatacoreIsRunning()
            DispatchQueue.main.async { [weak self] in
                guard let self, generation == self.loadGeneration else { return }
                if ready {
                    self.statusView.hide()
                    self.webView.load(URLRequest(url: self.pageURL))
                    return
                }
                if !running {
                    // 例如挂起期间监听 socket 被系统回收：自动重启一次
                    if allowRestart, Backend.start() == nil {
                        self.waitForBackend(generation: generation, deadline: deadline, allowRestart: false)
                        return
                    }
                    let reason = CfdatacoreLastError()
                    self.statusView.showFailure("\(reason.isEmpty ? "后端服务未运行" : reason)\n（端口 \(backendPort) 可能被占用）")
                    return
                }
                if Date() > deadline {
                    self.statusView.showFailure("后端服务启动超时，请检查网络后重试")
                    return
                }
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.05) {
                    self.waitForBackend(generation: generation, deadline: deadline, allowRestart: allowRestart)
                }
            }
        }
    }

    @objc private func appDidBecomeActive() {
        endBackgroundTask()
        // 仅在页面已加载过时检查；启动中或失败界面由自身流程处理
        guard pageLoaded, statusView.isHidden else { return }
        DispatchQueue.global(qos: .userInitiated).async {
            let ready = CfdatacoreIsReady(backendPort)
            DispatchQueue.main.async { [weak self] in
                guard let self, !ready else { return }
                self.pageLoaded = false
                self.startBackendAndLoad()
            }
        }
    }

    @objc private func appDidEnterBackground() {
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

    func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
        pageLoaded = true
    }

    func webView(_ webView: WKWebView, didFailProvisionalNavigation navigation: WKNavigation!, withError error: Error) {
        let nsError = error as NSError
        guard nsError.domain == NSURLErrorDomain, nsError.code != NSURLErrorCancelled else { return }
        pageLoaded = false
        statusView.showFailure("页面加载失败：\(nsError.localizedDescription)")
    }

    func webViewWebContentProcessDidTerminate(_ webView: WKWebView) {
        // WebContent 进程被系统回收（内存压力等）时白屏，重新检查后端并加载
        pageLoaded = false
        startBackendAndLoad()
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
