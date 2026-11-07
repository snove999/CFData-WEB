import UIKit
import WebKit
import Cfdatacore

@main
class AppDelegate: UIResponder, UIApplicationDelegate {
    var window: UIWindow?

    func application(_ application: UIApplication,
                     didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?) -> Bool {
        let docs = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0].path
        CfdatacoreStart(docs, 13335)
        application.isIdleTimerDisabled = true   // 扫描时防止锁屏挂起

        window = UIWindow(frame: UIScreen.main.bounds)
        window?.rootViewController = WebViewController()
        window?.makeKeyAndVisible()
        return true
    }
}

class WebViewController: UIViewController, WKNavigationDelegate, WKDownloadDelegate {
    private let url = URL(string: "http://127.0.0.1:13335")!
    private var webView: WKWebView!
    private var lastDownload: URL?

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        webView = WKWebView(frame: view.bounds, configuration: WKWebViewConfiguration())
        webView.autoresizingMask = [.flexibleWidth, .flexibleHeight]
        webView.navigationDelegate = self
        view.addSubview(webView)
        waitAndLoad()
    }

    // 等待后端端口就绪后再加载页面
    private func waitAndLoad(attempt: Int = 0) {
        URLSession.shared.dataTask(with: url) { _, resp, _ in
            DispatchQueue.main.async {
                if resp != nil {
                    self.webView.load(URLRequest(url: self.url))
                } else if attempt < 150 {
                    DispatchQueue.main.asyncAfter(deadline: .now() + 0.2) { self.waitAndLoad(attempt: attempt + 1) }
                }
            }
        }.resume()
    }

    // ---- 导出 CSV/TXT（blob 下载）→ 系统分享面板 ----
    func webView(_ webView: WKWebView, decidePolicyFor action: WKNavigationAction,
                 preferences: WKWebpagePreferences,
                 decisionHandler: @escaping (WKNavigationActionPolicy, WKWebpagePreferences) -> Void) {
        decisionHandler(action.shouldPerformDownload ? .download : .allow, preferences)
    }
    func webView(_ webView: WKWebView, decidePolicyFor response: WKNavigationResponse,
                 decisionHandler: @escaping (WKNavigationResponsePolicy) -> Void) {
        decisionHandler(response.canShowMIMEType ? .allow : .download)
    }
    func webView(_ webView: WKWebView, navigationAction: WKNavigationAction, didBecome download: WKDownload) { download.delegate = self }
    func webView(_ webView: WKWebView, navigationResponse: WKNavigationResponse, didBecome download: WKDownload) { download.delegate = self }

    func download(_ download: WKDownload, decideDestinationUsing response: URLResponse,
                  suggestedFilename: String, completionHandler: @escaping (URL?) -> Void) {
        let dest = FileManager.default.temporaryDirectory.appendingPathComponent(suggestedFilename)
        try? FileManager.default.removeItem(at: dest)
        lastDownload = dest
        completionHandler(dest)
    }
    func downloadDidFinish(_ download: WKDownload) {
        guard let file = lastDownload else { return }
        let vc = UIActivityViewController(activityItems: [file], applicationActivities: nil)
        vc.popoverPresentationController?.sourceView = view
        present(vc, animated: true)
    }
}
