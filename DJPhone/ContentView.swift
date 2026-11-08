//
//  ContentView.swift
//  DJPhone v15.1
//

import SwiftUI
import WebKit
import UIKit

struct ContentView: View {
    @EnvironmentObject var modemDriver: QuectelModemDriver

    var body: some View {
        ZStack {
            BridgeWebView(modemDriver: modemDriver)
                .edgesIgnoringSafeArea(.all)
        }
    }
}

struct BridgeWebView: UIViewRepresentable {
    @ObservedObject var modemDriver: QuectelModemDriver

    class Coordinator: NSObject, WKScriptMessageHandler, WKNavigationDelegate, WKUIDelegate {
        var parent: BridgeWebView
        weak var webView: WKWebView?

        init(_ parent: BridgeWebView) {
            self.parent = parent
        }

        func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
            guard message.name == "djphone",
                  let body = message.body as? [String: Any],
                  let action = body["action"] as? String else { return }

            let driver = parent.modemDriver
            switch action {
            case "dial":
                if let number = body["number"] as? String {
                    driver.dial(number: number)
                }
            case "answer":
                driver.answerIncomingCall()
            case "hangup":
                driver.hangup(fromCallKit: false)
            case "dtmf":
                if let digit = body["digit"] as? String {
                    driver.sendDTMF(digit)
                }
            case "mute":
                if let val = body["value"] as? Bool {
                    driver.setMute(val)
                }
            case "speaker":
                if let val = body["value"] as? Bool {
                    driver.setSpeaker(val)
                }
            case "sendSMS":
                let clientId = body["id"] as? String ?? UUID().uuidString
                let to = body["to"] as? String ?? ""
                let text = body["text"] as? String ?? ""
                driver.sendSMS(clientId: clientId, recipient: to, body: text)
            case "ackSmsSaved":
                if let key = body["dedupKey"] as? String {
                    driver.acknowledgeSmsSavedByWeb(dedupKey: key)
                }
            case "resolveUnknownSms":
                if let msgId = body["id"] as? String, let status = body["status"] as? String {
                    driver.resolveUnknownSmsStatus(msgId: msgId, newStatus: status)
                }
            case "cancelQueuedSms":
                if let msgId = body["id"] as? String {
                    driver.cancelQueuedSmsRetry(msgId: msgId)
                }
            case "deleteSms":
                if let msgId = body["id"] as? String {
                    driver.deleteStoredSms(msgId: msgId)
                }
            case "exportSms":
                driver.promptAndExportAllSmsViaShareSheet()
            case "exportSerialLog":
                driver.exportSerialDiagnosticTraceViaShareSheet()
            case "runAudioDiag":
                driver.runAudioDiagnostics()
            case "pollSMS":
                driver.pollSMSMessages()
            case "setVoLTE":
                if let enabled = body["enabled"] as? Bool {
                    driver.setVoLTE(enabled)
                }
            case "setKeepAwake":
                if let enabled = body["enabled"] as? Bool {
                    driver.setKeepScreenAwake(enabled)
                }
            case "setNotifPreview":
                if let enabled = body["enabled"] as? Bool {
                    driver.setNotificationPreview(enabled)
                }
            case "setAPN":
                if let apn = body["apn"] as? String {
                    driver.configureCarrierAPNExplicit(apn: apn)
                }
            case "triggerMBN":
                driver.triggerManualMBNAutoSel()
            case "updateEndpoint":
                let host = body["host"] as? String ?? "192.168.225.1"
                let port = body["port"] as? Int ?? 5555
                driver.updateATEndpoint(host: host, port: port)
            case "copyText":
                if let text = body["text"] as? String {
                    UIPasteboard.general.string = text
                }
            case "sendAT":
                if let cmd = body["cmd"] as? String {
                    driver.enqueueAT(cmd)
                }
            case "probeUSB":
                driver.probeUSBRegistry()
            case "probePorts":
                driver.probeModulePorts()
            case "refreshStatus":
                driver.refreshNetworkInterfaceStatus()
                driver.pollModemRegistrationAndSignal()
            default:
                break
            }
        }

        func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
            parent.modemDriver.refreshNetworkInterfaceStatus()
            parent.modemDriver.syncAllStoredSmsToWeb()
            parent.modemDriver.pollModemRegistrationAndSignal()
        }

        // Fixes v9 Bug 7: Uses UIApplication.topMostViewController() and guarantees completionHandler is ALWAYS called
        func webView(_ webView: WKWebView, runJavaScriptConfirmPanelWithMessage message: String, initiatedByFrame frame: WKFrameInfo, completionHandler: @escaping (Bool) -> Void) {
            guard let topVC = UIApplication.topMostViewController(), topVC.view.window != nil else {
                completionHandler(false)
                return
            }
            var completed = false
            let finish: (Bool) -> Void = { result in
                guard !completed else { return }
                completed = true
                completionHandler(result)
            }
            let alert = UIAlertController(title: nil, message: message, preferredStyle: .alert)
            alert.addAction(UIAlertAction(title: "取消", style: .cancel) { _ in finish(false) })
            alert.addAction(UIAlertAction(title: "确定", style: .default) { _ in finish(true) })
            topVC.present(alert, animated: true) {
                if alert.presentingViewController == nil {
                    finish(false)
                }
            }
        }

        func webView(_ webView: WKWebView, runJavaScriptAlertPanelWithMessage message: String, initiatedByFrame frame: WKFrameInfo, completionHandler: @escaping () -> Void) {
            guard let topVC = UIApplication.topMostViewController(), topVC.view.window != nil else {
                completionHandler()
                return
            }
            var completed = false
            let finish: () -> Void = {
                guard !completed else { return }
                completed = true
                completionHandler()
            }
            let alert = UIAlertController(title: nil, message: message, preferredStyle: .alert)
            alert.addAction(UIAlertAction(title: "好", style: .default) { _ in finish() })
            topVC.present(alert, animated: true) {
                if alert.presentingViewController == nil {
                    finish()
                }
            }
        }
    }

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    func makeUIView(context: Context) -> WKWebView {
        let config = WKWebViewConfiguration()
        config.allowsInlineMediaPlayback = true
        config.userContentController.add(context.coordinator, name: "djphone")

        let webView = WKWebView(frame: .zero, configuration: config)
        webView.navigationDelegate = context.coordinator
        webView.uiDelegate = context.coordinator
        webView.isOpaque = false
        webView.backgroundColor = .systemBackground
        webView.scrollView.bounces = false
        context.coordinator.webView = webView

        modemDriver.onJavaScriptEvent = { [weak webView] jsCode in
            DispatchQueue.main.async {
                webView?.evaluateJavaScript(jsCode, completionHandler: nil)
            }
        }

        if let url = Bundle.main.url(forResource: "index", withExtension: "html", subdirectory: "www") ??
                     Bundle.main.url(forResource: "index", withExtension: "html") {
            webView.loadFileURL(url, allowingReadAccessTo: url.deletingLastPathComponent())
        }

        return webView
    }

    func updateUIView(_ uiView: WKWebView, context: Context) {}
}
