//
//  QuectelModemDriver.swift
//  DJPhone v15.1
//
//  Fixes & Architecture Upgrades in v15.1:
//  1. Uses ATStreamDecoder for stateful multi-line & empty-body +CMGL decoding (Fixes Bug 10).
//  2. Uses CallSessionCoordinator for single CallKit report on DND rejection, in-place notification update
//     when CLIP arrives after 0.6s timer, and .failed CallKit end reason on ATD error (Fixes Bugs 3 & 6).
//  3. Auto-queues SMS retries during the 60s late-receipt window with live countdown & duplicate cancellation (Fixes Bug 5).
//  4. Top-Most ViewController helper (`UIApplication.topMostViewController()`) so alerts/share sheets never fail silently (Fixes Bug 7).
//  5. Dedicated `DJPhoneExports` temp folder swept on app startup + 15s deferred cleanup after share completion
//     so async "Save to Files" / AirDrop never lose the file and force-kill leaves no residue (Fixes Bug 8).
//  6. Millisecond-timestamped Raw Serial Log Recorder (`exportSerialLog`) & UAC/QPCMV Audio Path Diagnostic (`runAudioDiagnostics`)
//     for real-device verification of Bug 4 & Bug 11.
//

import Foundation
import Combine
import AVFoundation
import CallKit
import Network
import UIKit
import UserNotifications

// MARK: - Top-Most View Controller Helper (Fixes Bug 7)
public extension UIApplication {
    static func topMostViewController() -> UIViewController? {
        let scenes = UIApplication.shared.connectedScenes
            .compactMap { $0 as? UIWindowScene }
        let activeScene = scenes.first(where: { $0.activationState == .foregroundActive }) ?? scenes.first
        guard let windowScene = activeScene else { return nil }

        let keyWindow = windowScene.windows.first(where: { $0.isKeyWindow }) ?? windowScene.windows.first(where: { !$0.isHidden })
        guard var top = keyWindow?.rootViewController else { return nil }

        while true {
            if let presented = top.presentedViewController, !presented.isBeingDismissed {
                top = presented
            } else if let nav = top as? UINavigationController, let visible = nav.visibleViewController {
                top = visible
            } else if let tab = top as? UITabBarController, let selected = tab.selectedViewController {
                top = selected
            } else {
                break
            }
        }
        return top
    }
}

// StoredSMSItem & SMSStoreMigrator defined in ATProtocolEngine.swift


// MARK: - Port probe helpers (file-scope, thread-safe)
private final class PortProbeState {
    private let lock = NSLock()
    private var openPorts: [Int] = []
    private var refusedCount = 0
    private var otherCount = 0
    func record(port: Int, result: String) {
        lock.lock(); defer { lock.unlock() }
        switch result {
        case "open": openPorts.append(port)
        case "refused": refusedCount += 1
        default: otherCount += 1
        }
    }
    func snapshot() -> (open: [Int], refused: Int, other: Int) {
        lock.lock(); defer { lock.unlock() }
        return (openPorts.sorted(), refusedCount, otherCount)
    }
}

private final class OneShotFlag {
    private let lock = NSLock()
    private var done = false
    /// Returns true only for the first caller.
    func fire() -> Bool {
        lock.lock(); defer { lock.unlock() }
        if done { return false }
        done = true
        return true
    }
}

public class QuectelModemDriver: NSObject, ObservableObject, CXProviderDelegate, UNUserNotificationCenterDelegate, DriverSystemBridgeProtocol {
    public static let shared = QuectelModemDriver()

    @Published public var isDongleConnected: Bool = false
    @Published public var localAssignedIP: String = "未分配"
    @Published public var atBridgeStatus: String = "未连接"
    @Published public var atBridgeConnected: Bool = false
    @Published public var atHost: String = UserDefaults.standard.string(forKey: "djphone_at_host") ?? "192.168.225.1"
    @Published public var atPort: UInt16 = UInt16(UserDefaults.standard.integer(forKey: "djphone_at_port") != 0 ? UserDefaults.standard.integer(forKey: "djphone_at_port") : 5555)

    @Published public var signalCSQ: Int = 0
    @Published public var signalBars: Int = 0
    @Published public var operatorName: String = "未检测"
    @Published public var simStatus: String = "检测中"
    @Published public var regStatus: String = "未注册"
    @Published public var volteEnabled: Bool = false
    @Published public var imsRegistered: Bool = false
    @Published public var smscNumber: String = ""
    @Published public var callState: CallLifecycleState = .idle
    @Published public var activePhoneNumber: String = ""
    @Published public var isMuted: Bool = false
    @Published public var isSpeakerOn: Bool = true
    @Published public var callKitAudioActive: Bool = false
    @Published public var qpcmvEnabled: Bool = false
    @Published public var keepScreenAwake: Bool = true
    @Published public var showNotificationPreview: Bool = UserDefaults.standard.bool(forKey: "djphone_notif_preview")

    public var onJavaScriptEvent: ((String) -> Void)?

    private let scheduler = RunLoopScheduler()
    private lazy var callSM = ATCallStateMachine(scheduler: scheduler)
    private lazy var queueCore = ATCommandQueueCore(scheduler: scheduler)
    private lazy var streamDecoder = ATStreamDecoder()
    private lazy var callCoordinator = CallSessionCoordinator(callSM: callSM, queueCore: queueCore, scheduler: scheduler, bridge: self)

    private let audioSession = AVAudioSession.sharedInstance()
    private let callController = CXCallController()
    private var callProvider: CXProvider?

    private var atConnection: NWConnection?
    private var connectionGeneration: UInt64 = 0
    private var isConnectingAT: Bool = false
    private var receiveBuffer: String = ""

    private let diskStoreQueue = DispatchQueue(label: "com.zengenerations.djphone.diskstore", qos: .utility)
    private let hardMaxStoredSmsCount = 500
    private var saveDebounceTimer: ATCancellableTimer? = nil

    private var nativeSmsList: [StoredSMSItem] = []
    private var seenSmsKeys: Set<String> = []
    private var pendingDeleteModemIndices: [String: Int] = [:]

    // Millisecond-timestamped serial diagnostic trace buffer for real-device verification (Optimization 8)
    private var serialTraceLines: [String] = []
    private let maxSerialTraceLines = 2000

    private var clccSawCallLineInCurrentPoll: Bool = false
    private var consecutiveEmptyCLCCCount: Int = 0
    private var callDialStartTime: Date? = nil

    private var statusPollTimer: Timer?
    private var clccPollTimer: Timer?
    private var lateWindowCountdownTimer: Timer?

    override init() {
        super.init()
        Self.sweepStaleExportDirectory()
        loadNativeSmsStore()
        NotificationCenter.default.addObserver(
            self,
            selector: #selector(handleProtectedDataDidBecomeAvailable),
            name: UIApplication.protectedDataDidBecomeAvailableNotification,
            object: nil
        )
        callCoordinator.showNotificationPreview = showNotificationPreview
        UNUserNotificationCenter.current().delegate = self

        let config = CXProviderConfiguration(localizedName: "DG电话")
        config.supportsVideo = false
        config.maximumCallsPerCallGroup = 1
        config.supportedHandleTypes = [.phoneNumber]
        let provider = CXProvider(configuration: config)
        provider.setDelegate(self, queue: .main)
        self.callProvider = provider

        setupEngineCallbacks()
    }

    private func recordSerialTrace(_ prefix: String, _ text: String) {
        let fmt = DateFormatter()
        fmt.dateFormat = "HH:mm:ss.SSS"
        let line = "[\(fmt.string(from: Date()))] [\(prefix)] \(text)"
        serialTraceLines.append(line)
        if serialTraceLines.count > maxSerialTraceLines {
            serialTraceLines.removeFirst(serialTraceLines.count - maxSerialTraceLines)
        }
    }

    private func setupEngineCallbacks() {
        queueCore.isCallIdleProvider = { [weak self] in
            return self?.callSM.state == .idle
        }

        callCoordinator.onWatchdogHangupTriggered = { [weak self] timedOutState, number in
            guard let self = self else { return }
            self.recordSerialTrace("CALL_WATCHDOG", "Transient state \(timedOutState.rawValue) timed out for \(number), executing full hangup")
            self.stopCLCCPolling()
            self.consecutiveEmptyCLCCCount = 0
            self.callDialStartTime = nil
            let cleanNum = number.isEmpty ? "未知号码" : number
            self.notifyWebUI(event: "callEndEvent", data: [
                "number": cleanNum,
                "logType": "超时未接通",
                "toast": "呼叫状态超时 (\(timedOutState.rawValue))，已自动挂断"
            ])
        }

        callSM.onStateChanged = { [weak self] newState, number in
            guard let self = self else { return }
            self.callState = newState
            self.activePhoneNumber = number
            self.recordSerialTrace("CALL_SM", "state=\(newState.rawValue) number=\(number)")
            if newState == .idle {
                self.callCoordinator.resetSessionFlags()
            }
            self.notifyWebUI(event: "callState", data: [
                "state": newState.rawValue,
                "number": number,
                "uac": self.hasUSBAudioDevice(),
                "audioActive": self.callKitAudioActive,
                "qpcmv": self.qpcmvEnabled,
                "smsSubmitting": self.queueCore.isSendingSMS
            ])
            if newState == .idle {
                self.queueCore.checkDeferredSMSPollIfIdle()
            }
        }

        queueCore.onWriteRaw = { [weak self] raw in
            guard let self = self, let conn = self.atConnection else { return }
            let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
            if self.queueCore.currentCommand?.tag == "CLCC" {
                self.clccSawCallLineInCurrentPoll = false
            }
            if self.queueCore.currentCommand?.tag == "ATH_DONE" {
                self.callSM.armHangupWatchdogOnATHDispatched()
            }
            if trimmed == "\u{1B}" {
                self.recordSerialTrace("TX", "<ESC 0x1B>")
                self.notifyWebUI(event: "atLog", data: ["text": ">>> [ESC 中止短信输入]"])
            } else if trimmed.hasSuffix("\u{1A}") {
                self.recordSerialTrace("TX", "<UCS2_BODY + Ctrl-Z 0x1A> (len=\(trimmed.count))")
                self.notifyWebUI(event: "atLog", data: ["text": ">>> [已提交 UCS2 正文 + Ctrl-Z，最多等待 12s]"])
            } else if !trimmed.isEmpty {
                self.recordSerialTrace("TX", trimmed)
                self.notifyWebUI(event: "atLog", data: ["text": ">>> " + trimmed])
            }
            conn.send(content: raw.data(using: .utf8), completion: .contentProcessed({ [weak self] error in
                if let error = error {
                    DispatchQueue.main.async {
                        self?.recordSerialTrace("TX_ERR", error.localizedDescription)
                        self?.notifyWebUI(event: "atLog", data: ["text": "[发送异常: \(error.localizedDescription)]"])
                    }
                }
            }))
        }

        queueCore.onCommandTimedOut = { [weak self] timedOut in
            self?.recordSerialTrace("TIMEOUT", "cmd=\(timedOut.command) tag=\(timedOut.tag) timeout=\(timedOut.timeout)s")
        }

        queueCore.onCommandCompleted = { [weak self] finished, isError, errorLine in
            guard let self = self else { return }
            if finished.tag == "MBN_AUTOSEL" && !isError {
                UserDefaults.standard.set(true, forKey: "djphone_mbn_autosel_done")
            }
            if finished.tag == "QPCMV_ON" && !isError {
                self.qpcmvEnabled = true
                self.pushFullStatusToWeb()
            } else if finished.tag == "QPCMV_OFF" {
                self.qpcmvEnabled = false
                self.pushFullStatusToWeb()
            }
            // Fixes Bug 6 (Part B): Pass isDialFailure: true so CallKit receives .failed instead of .unanswered!
            if finished.tag == "ATD_CMD" && isError && self.callSM.state != .idle {
                self.handleRemoteCallEnded(endReasonLabel: "拨号失败 (\(errorLine))", isDialFailure: true)
                return
            }
            if finished.tag == "ATH_DONE" && self.callSM.state == .hangingUp {
                self.callSM.completeHangupToIdle()
            }
            if finished.tag == "CLCC_WATCHDOG_VERIFY" {
                self.callCoordinator.handleWatchdogCLCCVerifyCommandCompleted(isError: isError)
            }
            if finished.tag == "CLCC" && !isError && self.callSM.state.shouldProcessActiveCLCC {
                if self.clccSawCallLineInCurrentPoll {
                    self.consecutiveEmptyCLCCCount = 0
                } else {
                    let inGracePeriod = (self.callDialStartTime != nil && Date().timeIntervalSince(self.callDialStartTime!) < 5.0)
                    if !inGracePeriod {
                        self.consecutiveEmptyCLCCCount += 1
                        if self.consecutiveEmptyCLCCCount >= 3 {
                            self.handleRemoteCallEnded(endReasonLabel: nil, isDialFailure: false)
                        }
                    }
                }
            }
        }

        queueCore.onSMSResult = { [weak self] cid, status, detail in
            guard let self = self else { return }
            self.recordSerialTrace("SMS_RESULT", "id=\(cid) status=\(status) detail=\(detail)")
            self.updateLateWindowCountdownTimer()
            self.pushFullStatusToWeb()
            let timeStr = (status == "sent") ? ("已发送 " + Self.currentTimeString()) : detail
            if let idx = self.nativeSmsList.firstIndex(where: { $0.id == cid }) {
                self.nativeSmsList[idx].status = status
                self.nativeSmsList[idx].time = timeStr
                self.scheduleDebouncedDiskSave()
            }
            self.notifyWebUI(event: "smsSendResult", data: [
                "id": cid,
                "status": status,
                "error": detail,
                "time": timeStr,
                "lateWindowRemaining": self.queueCore.remainingLateWindowSeconds
            ])
        }

        queueCore.onTriggerDeferredSMSPoll = { [weak self] in
            self?.pollSMSMessages()
        }

        // Fixes Bug 5: Automatically dispatch queued retry/SMS when the 60s late-receipt window ends
        queueCore.onDispatchQueuedSMS = { [weak self] item in
            guard let self = self else { return }
            self.recordSerialTrace("SMS_AUTO_RETRY", "Auto-dispatching queued SMS id=\(item.clientId)")
            self.sendSMS(clientId: item.clientId, recipient: item.recipient, body: item.body)
        }
    }

    private func updateLateWindowCountdownTimer() {
        lateWindowCountdownTimer?.invalidate()
        lateWindowCountdownTimer = nil
        guard queueCore.hasPendingLateSMSReceipt else { return }
        lateWindowCountdownTimer = Timer.scheduledTimer(withTimeInterval: 1.0, repeats: true) { [weak self] timer in
            guard let self = self else { timer.invalidate(); return }
            let rem = self.queueCore.remainingLateWindowSeconds
            let queuedIds = self.queueCore.queuedSMSAfterWindow.map { $0.clientId }
            if !queuedIds.isEmpty {
                for qid in queuedIds {
                    if let idx = self.nativeSmsList.firstIndex(where: { $0.id == qid }) {
                        self.nativeSmsList[idx].time = "⏳ 排队等待中 (\(rem)s 后自动发送)"
                    }
                }
                self.notifyWebUI(event: "smsQueueCountdown", data: [
                    "remaining": rem,
                    "ids": queuedIds
                ])
            }
            if rem <= 0 || !self.queueCore.hasPendingLateSMSReceipt {
                timer.invalidate()
                self.lateWindowCountdownTimer = nil
            }
        }
    }

    // MARK: - DriverSystemBridgeProtocol Implementation (For CallSessionCoordinator)
    public func reportNewIncomingCall(uuid: UUID, caller: String, completion: @escaping (Error?) -> Void) {
        let update = CXCallUpdate()
        update.remoteHandle = CXHandle(type: .phoneNumber, value: caller)
        callProvider?.reportNewIncomingCall(with: uuid, update: update) { [weak self] error in
            DispatchQueue.main.async {
                if let error = error {
                    self?.recordSerialTrace("CALLKIT_DND", "Rejected by system: \(error.localizedDescription)")
                    self?.notifyWebUI(event: "atLog", data: ["text": "[CallKit 来电被专注/勿扰模式拦截 (\(error.localizedDescription))，本次来电不再重复调用 CallKit]"])
                }
                completion(error)
            }
        }
    }

    public func updateCallCaller(uuid: UUID, caller: String) {
        let update = CXCallUpdate()
        update.remoteHandle = CXHandle(type: .phoneNumber, value: caller)
        callProvider?.reportCall(with: uuid, updated: update)
    }

    public func reportCallEnded(uuid: UUID, reason: CallKitEndReasonType) {
        let cxReason: CXCallEndedReason
        switch reason {
        case .failed: cxReason = .failed
        case .unanswered: cxReason = .unanswered
        case .remoteEnded: cxReason = .remoteEnded
        }
        callProvider?.reportCall(with: uuid, endedAt: Date(), reason: cxReason)
    }

    public func postOrUpdateNotification(id: String, title: String, body: String) {
        let content = UNMutableNotificationContent()
        content.title = title
        content.body = body
        content.sound = .default
        let req = UNNotificationRequest(identifier: id, content: content, trigger: nil)
        UNUserNotificationCenter.current().add(req, withCompletionHandler: nil)
    }

    private func postSMSNotification(peer: String, body: String) {
        let title = showNotificationPreview ? "新短信 (\(peer))" : "DG电话 - 收到新短信"
        let text = showNotificationPreview ? body : "请打开 DG电话 查看详情"
        postOrUpdateNotification(id: "djphone_sms_\(UUID().uuidString)", title: title, body: text)
    }

    public func userNotificationCenter(_ center: UNUserNotificationCenter, willPresent notification: UNNotification, withCompletionHandler completionHandler: @escaping (UNNotificationPresentationOptions) -> Void) {
        completionHandler([.banner, .sound])
    }

    public func initializeBridge() {
        prepareAudioSessionCategory()
        refreshNetworkInterfaceStatus()
        connectATBridge()

        statusPollTimer?.invalidate()
        statusPollTimer = Timer.scheduledTimer(withTimeInterval: 6.0, repeats: true) { [weak self] _ in
            guard let self = self else { return }
            self.refreshNetworkInterfaceStatus()
            if !self.atBridgeConnected && !self.isConnectingAT {
                self.connectATBridge()
            } else if self.atBridgeConnected && !self.queueCore.isSendingSMS && !self.queueCore.isSyncingBarrier {
                let hasLowPollQueued = self.queueCore.queue.contains(where: { $0.priority == .low })
                if !hasLowPollQueued {
                    self.pollModemRegistrationAndSignal()
                }
            }
        }
    }

    public func setKeepScreenAwake(_ enabled: Bool) {
        self.keepScreenAwake = enabled
        DispatchQueue.main.async {
            UIApplication.shared.isIdleTimerDisabled = enabled
            self.pushFullStatusToWeb()
        }
    }

    public func setNotificationPreview(_ enabled: Bool) {
        self.showNotificationPreview = enabled
        self.callCoordinator.showNotificationPreview = enabled
        UserDefaults.standard.set(enabled, forKey: "djphone_notif_preview")
        pushFullStatusToWeb()
    }

    // MARK: - Native Persistent SMS Storage with Schema Version & Automatic Legacy Migration (Fixes v12 Bug 1)
    private static func smsStoreDirectoryURL() -> URL {
        let fm = FileManager.default
        let appSupport = fm.urls(for: .applicationSupportDirectory, in: .userDomainMask).first!
        try? fm.createDirectory(at: appSupport, withIntermediateDirectories: true)
        return appSupport
    }

    private static func smsStoreFileURL() -> URL {
        return smsStoreDirectoryURL().appendingPathComponent(SMSStoreMigrator.canonicalFileName)
    }

    private static func exportTempDirectoryURL() -> URL {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent("DJPhoneExports", isDirectory: true)
        try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    public static func sweepStaleExportDirectory() {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent("DJPhoneExports", isDirectory: true)
        try? FileManager.default.removeItem(at: dir)
    }

    private var storeLoadFailedReadOnlyGuard: Bool = false

    private func loadNativeSmsStore() {
        let dir = Self.smsStoreDirectoryURL()
        let unlocked = UIApplication.shared.isProtectedDataAvailable
        let result = SMSStoreMigrator.loadAndMigrateIfNeeded(in: dir, maxCount: hardMaxStoredSmsCount, isProtectedDataAvailable: unlocked)
        self.storeLoadFailedReadOnlyGuard = result.loadFailedReadOnlyGuard
        self.nativeSmsList = result.messages
        self.seenSmsKeys = result.seenKeys
        if !result.quarantinedFiles.isEmpty {
            recordSerialTrace("STORE_QUARANTINE", "Quarantined corrupt store files: \(result.quarantinedFiles.joined(separator: ", "))")
        }
        if result.loadFailedReadOnlyGuard {
            recordSerialTrace("STORE_GUARD", "Main store & .bak unreadable (locked or corrupt); using session fallback store until unlock")
            DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) { [weak self] in
                self?.notifyWebUI(event: "toast", data: ["msg": "⚠️ 短信主库暂不可读(可能锁屏保护中)，新短信已暂存至安全备用库，解锁后自动合并"])
            }
        } else if result.didMigrateOrModify {
            scheduleDebouncedDiskSave()
        }
    }

    @objc private func handleProtectedDataDidBecomeAvailable() {
        guard storeLoadFailedReadOnlyGuard else { return }
        recordSerialTrace("STORE_UNLOCK", "Protected data available; retrying SMS store load & merging session fallback")
        let currentSessionItems = self.nativeSmsList
        let dir = Self.smsStoreDirectoryURL()
        if !currentSessionItems.isEmpty {
            _ = SMSStoreMigrator.writeSessionFallbackStore(messages: currentSessionItems, in: dir)
        }
        loadNativeSmsStore()
        if !storeLoadFailedReadOnlyGuard {
            syncAllStoredSmsToWeb()
            notifyWebUI(event: "toast", data: ["msg": "✓ 设备已解锁，短信主库与备用库已成功合并保存"])
        }
    }

    private func appendToNativeSmsStore(_ item: StoredSMSItem) {
        nativeSmsList.append(item)
        while nativeSmsList.count > hardMaxStoredSmsCount {
            if let nonOtpAckedIdx = nativeSmsList.firstIndex(where: {
                self.pendingDeleteModemIndices[$0.dedupKey] == nil && $0.otp.isEmpty
            }) {
                nativeSmsList.remove(at: nonOtpAckedIdx)
            } else if let ackedIdx = nativeSmsList.firstIndex(where: {
                self.pendingDeleteModemIndices[$0.dedupKey] == nil
            }) {
                nativeSmsList.remove(at: ackedIdx)
            } else {
                break
            }
        }
        scheduleDebouncedDiskSave()
        if nativeSmsList.count >= 450 {
            notifyWebUI(event: "toast", data: ["msg": "提示: 本地短信已达 \(nativeSmsList.count)/500 条，建议导出备份"])
        }
    }

    private func scheduleDebouncedDiskSave() {
        saveDebounceTimer?.cancel()
        saveDebounceTimer = scheduler.scheduleOnce(delay: 0.3) { [weak self] in
            guard let self = self else { return }
            let snapshot = self.nativeSmsList
            let dir = Self.smsStoreDirectoryURL()
            let isReadOnlyMain = self.storeLoadFailedReadOnlyGuard
            self.diskStoreQueue.async {
                if isReadOnlyMain {
                    // Save to emergency session fallback file without touching main store
                    _ = SMSStoreMigrator.writeSessionFallbackStore(messages: snapshot, in: dir)
                } else {
                    _ = SMSStoreMigrator.writeStoreAtomicallyWithBackup(messages: snapshot, in: dir)
                }
            }
        }
    }

    public func resolveUnknownSmsStatus(msgId: String, newStatus: String) {
        queueCore.clearUnknownSMSTracking(for: msgId)
        if let idx = nativeSmsList.firstIndex(where: { $0.id == msgId }) {
            nativeSmsList[idx].status = newStatus
            if newStatus == "sent" {
                nativeSmsList[idx].time = "已确认送达 " + Self.currentTimeString()
            }
            scheduleDebouncedDiskSave()
            syncAllStoredSmsToWeb()
        }
        let rem = queueCore.remainingLateWindowSeconds
        if queueCore.isLateCMGSChannelWindowOpen && rem > 0 {
            notifyWebUI(event: "toast", data: ["msg": "已更新状态 (短信通道保护还剩 \(rem) 秒)"])
        }
    }

    public func cancelQueuedSmsRetry(msgId: String) {
        _ = queueCore.cancelQueuedSMS(clientId: msgId)
    }

    public func deleteStoredSms(msgId: String) {
        queueCore.clearUnknownSMSTracking(for: msgId)
        nativeSmsList.removeAll { $0.id == msgId }
        scheduleDebouncedDiskSave()
        syncAllStoredSmsToWeb()
        let rem = queueCore.remainingLateWindowSeconds
        if queueCore.isLateCMGSChannelWindowOpen && rem > 0 {
            notifyWebUI(event: "toast", data: ["msg": "已删除短信 (短信通道保护还剩 \(rem) 秒)"])
        }
    }

    /// Fixes Bug 7 & Bug 8: Uses `UIApplication.topMostViewController()`, writes to `DJPhoneExports` with `.completeFileProtection`,
    /// and deletes the temp file 15s after `completionWithItemsHandler` so async "Save to Files" / AirDrop finish reading cleanly.
    public func promptAndExportAllSmsViaShareSheet() {
        DispatchQueue.main.async {
            guard let topVC = UIApplication.topMostViewController() else {
                self.notifyWebUI(event: "toast", data: ["msg": "无法获取顶层视图控制器"])
                return
            }

            let alert = UIAlertController(
                title: "导出短信备份",
                message: "确认导出全部 \(self.nativeSmsList.count) 条短信记录？将通过系统分享面板保存为 JSON 文件。",
                preferredStyle: .alert
            )
            alert.addAction(UIAlertAction(title: "取消", style: .cancel))
            alert.addAction(UIAlertAction(title: "确认导出", style: .default) { [weak self] _ in
                self?.performShareSheetExport()
            })
            topVC.present(alert, animated: true)
        }
    }

    private func performShareSheetExport() {
        guard let topVC = UIApplication.topMostViewController() else { return }
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        guard let data = try? encoder.encode(nativeSmsList) else {
            notifyWebUI(event: "toast", data: ["msg": "生成导出数据失败"])
            return
        }
        let tempURL = Self.exportTempDirectoryURL().appendingPathComponent("DJPhone_SMS_Backup_\(Int(Date().timeIntervalSince1970)).json")
        presentTempFileShareSheet(tempURL: tempURL, data: data, from: topVC)
    }

    /// Optimization 8: Export millisecond-timestamped raw serial & state-machine trace log for real-device verification of Bug 4 & Bug 11.
    public func exportSerialDiagnosticTraceViaShareSheet() {
        DispatchQueue.main.async {
            guard let topVC = UIApplication.topMostViewController() else {
                self.notifyWebUI(event: "toast", data: ["msg": "无法获取顶层视图控制器"])
                return
            }
            let header = [
                "# DJPhone v15.1 Real-Device Serial & Audio Diagnostic Trace",
                "# Generated: \(Date())",
                "# Endpoint: \(self.atHost):\(self.atPort) | Connected: \(self.atBridgeConnected)",
                "# CallState: \(self.callSM.state.rawValue) | QPCMV: \(self.qpcmvEnabled) | UAC: \(self.hasUSBAudioDevice())",
                "# LateWindowActive: \(self.queueCore.hasPendingLateSMSReceipt) (remaining: \(self.queueCore.remainingLateWindowSeconds)s)",
                "------------------------------------------------------------------------"
            ].joined(separator: "\n")
            let fullText = header + "\n" + self.serialTraceLines.joined(separator: "\n") + "\n"
            guard let data = fullText.data(using: .utf8) else { return }
            let tempURL = Self.exportTempDirectoryURL().appendingPathComponent("DJPhone_Serial_Trace_\(Int(Date().timeIntervalSince1970)).log")
            self.presentTempFileShareSheet(tempURL: tempURL, data: data, from: topVC)
        }
    }

    private func presentTempFileShareSheet(tempURL: URL, data: Data, from topVC: UIViewController) {
        do {
            try data.write(to: tempURL, options: [.atomic, .completeFileProtection])
            let activityVC = UIActivityViewController(activityItems: [tempURL], applicationActivities: nil)
            activityVC.completionWithItemsHandler = { [weak self] _, _, _, _ in
                // Fixes Bug 8: Wait 15s after completion callback before unlinking so async "Save to Files" / AirDrop finish reading
                _ = self?.scheduler.scheduleOnce(delay: 15.0) {
                    try? FileManager.default.removeItem(at: tempURL)
                }
            }
            if let pop = activityVC.popoverPresentationController {
                pop.sourceView = topVC.view
                pop.sourceRect = CGRect(x: topVC.view.bounds.midX, y: 80, width: 0, height: 0)
            }
            topVC.present(activityVC, animated: true)
        } catch {
            notifyWebUI(event: "toast", data: ["msg": "导出写入失败: \(error.localizedDescription)"])
        }
    }

    public func syncAllStoredSmsToWeb() {
        guard let data = try? JSONEncoder().encode(nativeSmsList),
              let jsonArray = try? JSONSerialization.jsonObject(with: data) as? [[String: Any]] else { return }
        notifyWebUI(event: "smsSyncAll", data: ["messages": jsonArray, "totalCount": nativeSmsList.count])
    }

    public func acknowledgeSmsSavedByWeb(dedupKey: String) {
        if let modemIdx = pendingDeleteModemIndices.removeValue(forKey: dedupKey) {
            enqueueAT("AT+CMGD=\(modemIdx)", priority: .normal, timeout: 4.0, tag: "CMGD")
        }
    }

    // MARK: - 1. Network Interface & UAC Audio Diagnostics (Bug 11 Verification Toolkit)
    public func refreshNetworkInterfaceStatus() {
        var foundDongleIP: String? = nil
        var ifaddr: UnsafeMutablePointer<ifaddrs>? = nil

        if getifaddrs(&ifaddr) == 0, let firstAddr = ifaddr {
            var ptr: UnsafeMutablePointer<ifaddrs>? = firstAddr
            while let current = ptr {
                if let ifaAddr = current.pointee.ifa_addr {
                    if ifaAddr.pointee.sa_family == UInt8(AF_INET) {
                        var hostname = [CChar](repeating: 0, count: Int(NI_MAXHOST))
                        if getnameinfo(ifaAddr, socklen_t(ifaAddr.pointee.sa_len),
                                       &hostname, socklen_t(hostname.count),
                                       nil, 0, NI_NUMERICHOST) == 0 {
                            let ip = String(cString: hostname)
                            if ip.hasPrefix("192.168.225.") {
                                foundDongleIP = ip
                                break
                            }
                        }
                    }
                }
                ptr = current.pointee.ifa_next
            }
            freeifaddrs(ifaddr)
        }

        let uacAvailable = hasUSBAudioDevice()
        DispatchQueue.main.async {
            if let ip = foundDongleIP {
                self.isDongleConnected = true
                self.localAssignedIP = ip
            } else if uacAvailable {
                self.isDongleConnected = true
                self.localAssignedIP = "仅USB声卡"
            } else {
                self.isDongleConnected = false
                self.localAssignedIP = "未连接"
            }
            self.pushFullStatusToWeb()
        }
    }

    public func runAudioDiagnostics() {
        let inputs = (audioSession.availableInputs ?? []).map { "\($0.portName)(\($0.portType.rawValue))" }.joined(separator: ", ")
        let routeIn = audioSession.currentRoute.inputs.map { "\($0.portName)(\($0.portType.rawValue))" }.joined(separator: ", ")
        let routeOut = audioSession.currentRoute.outputs.map { "\($0.portName)(\($0.portType.rawValue))" }.joined(separator: ", ")
        let summary = "[UAC自检] 可用输入: [\(inputs.isEmpty ? "无" : inputs)] | 当前输入: [\(routeIn.isEmpty ? "无" : routeIn)] | 当前输出: [\(routeOut.isEmpty ? "无" : routeOut)] | SampleRate: \(Int(audioSession.sampleRate))Hz"
        recordSerialTrace("AUDIO_DIAG", summary)
        notifyWebUI(event: "atLog", data: ["text": summary])
        notifyWebUI(event: "toast", data: ["msg": hasUSBAudioDevice() ? "检测到 USB 声卡，已发送 QPCMV/QDAI 查询" : "未检测到 USB 声卡，请检查供电转接头"])
        enqueueAT("AT+QPCMV?", priority: .normal)
        enqueueAT("AT+QDAI?", priority: .normal)
    }

    public func pushFullStatusToWeb() {
        notifyWebUI(event: "modemStatus", data: [
            "connected": isDongleConnected,
            "ip": localAssignedIP,
            "atConnected": atBridgeConnected,
            "atStatus": atBridgeStatus,
            "atHost": atHost,
            "atPort": Int(atPort),
            "uac": hasUSBAudioDevice(),
            "audioActive": callKitAudioActive,
            "qpcmv": qpcmvEnabled,
            "keepAwake": keepScreenAwake,
            "notifPreview": showNotificationPreview,
            "isSendingSMS": queueCore.isSendingSMS,
            "lateWindowRemaining": queueCore.remainingLateWindowSeconds,
            "queuedRetryCount": queueCore.queuedSMSAfterWindow.count,
            "csq": signalCSQ,
            "bars": signalBars,
            "operator": operatorName,
            "sim": simStatus,
            "reg": regStatus,
            "volte": volteEnabled,
            "imsReg": imsRegistered,
            "smsc": smscNumber
        ])
    }

    // MARK: - 2. Audio Session
    public func hasUSBAudioDevice() -> Bool {
        let inputs = audioSession.availableInputs ?? []
        let outputs = audioSession.currentRoute.outputs
        return inputs.contains(where: { $0.portType == .usbAudio }) ||
               outputs.contains(where: { $0.portType == .usbAudio })
    }

    public func prepareAudioSessionCategory() {
        do {
            try audioSession.setCategory(.playAndRecord, mode: .voiceChat, options: [.allowBluetooth, .defaultToSpeaker])
        } catch {
            print("[AudioSession] Category error: \(error)")
        }
    }

    public func setMute(_ muted: Bool) {
        self.isMuted = muted
        enqueueAT("AT+CMUT=\(muted ? 1 : 0)", priority: .high)
        notifyWebUI(event: "audioState", data: ["muted": isMuted, "speaker": isSpeakerOn])
    }

    public func setSpeaker(_ speaker: Bool) {
        self.isSpeakerOn = speaker
        do {
            try audioSession.overrideOutputAudioPort(speaker ? .speaker : .none)
        } catch {
            print("[AudioSession] Speaker error: \(error)")
        }
        notifyWebUI(event: "audioState", data: ["muted": isMuted, "speaker": isSpeakerOn])
    }

    // MARK: - 3. Connection Management
    public func updateATEndpoint(host: String, port: Int) {
        let cleanHost = host.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !cleanHost.isEmpty, port > 0 && port <= 65535 else { return }
        self.atHost = cleanHost
        self.atPort = UInt16(port)
        UserDefaults.standard.set(cleanHost, forKey: "djphone_at_host")
        UserDefaults.standard.set(port, forKey: "djphone_at_port")
        connectATBridge()
    }

    public func connectATBridge() {
        connectionGeneration &+= 1
        let myGen = connectionGeneration

        resetQueueAndCallStateOnDisconnect()

        atConnection?.cancel()
        atConnection = nil
        isConnectingAT = true

        DispatchQueue.main.async {
            self.atBridgeConnected = false
            self.atBridgeStatus = "正在连接 \(self.atHost):\(self.atPort)..."
            self.pushFullStatusToWeb()
        }

        let endpointHost = NWEndpoint.Host(atHost)
        guard let endpointPort = NWEndpoint.Port(rawValue: atPort) else {
            isConnectingAT = false
            return
        }

        let conn = NWConnection(host: endpointHost, port: endpointPort, using: .tcp)
        conn.stateUpdateHandler = { [weak self] state in
            guard let self = self else { return }
            DispatchQueue.main.async {
                guard self.connectionGeneration == myGen else { return }
                switch state {
                case .ready:
                    self.isConnectingAT = false
                    self.atBridgeConnected = true
                    self.atBridgeStatus = "已连接 (\(self.atHost):\(self.atPort))"
                    self.recordSerialTrace("CONN", "Connected to \(self.atHost):\(self.atPort)")
                    self.pushFullStatusToWeb()
                    self.receiveATResponse(generation: myGen)
                    self.initializeModemSettings()
                case .waiting(let err):
                    self.isConnectingAT = false
                    self.atBridgeConnected = false
                    self.atBridgeStatus = "等待网络 (\(self.describeNWError(err)))"
                    self.pushFullStatusToWeb()
                case .failed(let err):
                    self.isConnectingAT = false
                    self.atBridgeConnected = false
                    self.atBridgeStatus = "连接失败 (\(self.describeNWError(err)))"
                    self.recordSerialTrace("CONN_FAIL", err.localizedDescription)
                    self.resetQueueAndCallStateOnDisconnect()
                    self.pushFullStatusToWeb()
                case .cancelled:
                    self.isConnectingAT = false
                    self.atBridgeConnected = false
                    self.atBridgeStatus = "已断开"
                    self.resetQueueAndCallStateOnDisconnect()
                    self.pushFullStatusToWeb()
                default:
                    break
                }
            }
        }
        conn.start(queue: .global())
        self.atConnection = conn
    }

    private func resetQueueAndCallStateOnDisconnect() {
        stopCLCCPolling()
        lateWindowCountdownTimer?.invalidate()
        lateWindowCountdownTimer = nil
        streamDecoder.reset()

        if queueCore.isSendingSMS {
            let cid = queueCore.pendingSMSClientId
            let bodyWasSent = queueCore.smsBodyDispatched
            queueCore.resetAll()
            if !cid.isEmpty {
                let st = bodyWasSent ? "unknown" : "failed"
                let msg = bodyWasSent ? "状态未知 (连接中断)" : "发送失败: AT通道断开"
                if let idx = nativeSmsList.firstIndex(where: { $0.id == cid }) {
                    nativeSmsList[idx].status = st
                    nativeSmsList[idx].time = msg
                    scheduleDebouncedDiskSave()
                }
                notifyWebUI(event: "smsSendResult", data: ["id": cid, "status": st, "error": msg, "time": msg])
            }
        } else {
            queueCore.resetAll()
        }

        receiveBuffer = ""

        if callSM.state != .idle {
            _ = callCoordinator.handleCallEnded(isDialFailure: true)
            consecutiveEmptyCLCCCount = 0
            callDialStartTime = nil
            qpcmvEnabled = false
            callSM.resetOnDisconnect()
        }
    }

    public func enqueueAT(
        _ command: String,
        priority: ATCommandPriority = .normal,
        timeout: TimeInterval = 4.0,
        tag: String = "",
        batchId: String = ""
    ) {
        let executeBlock = { [weak self] in
            guard let self = self else { return }
            guard self.atBridgeConnected else {
                if priority != .low {
                    self.notifyWebUI(event: "atLog", data: ["text": "[未连接 AT 通道，已拒绝命令: \(command)]"])
                }
                return
            }
            self.queueCore.enqueue(command, priority: priority, timeout: timeout, tag: tag, batchId: batchId)
        }
        if Thread.isMainThread {
            executeBlock()
        } else {
            DispatchQueue.main.async(execute: executeBlock)
        }
    }

    // MARK: - 4. Modem Initialization & Polling
    public func initializeModemSettings() {
        enqueueAT("ATE0", priority: .normal)
        enqueueAT("AT+CMEE=2", priority: .normal)
        enqueueAT("AT+CRC=0", priority: .normal)
        enqueueAT("AT+CLIP=1", priority: .normal)
        enqueueAT("AT+CNMI=2,1,0,0,0", priority: .normal)
        enqueueAT("AT+COPS=3,2", priority: .normal)

        if !UserDefaults.standard.bool(forKey: "djphone_mbn_autosel_done") {
            enqueueAT("AT+QMBNCFG=\"AutoSel\",1", priority: .normal, tag: "MBN_AUTOSEL")
        }

        enqueueAT("AT+CPIN?", priority: .normal)
        enqueueAT("AT+CSCA?", priority: .normal)
        enqueueAT("AT+QCFG=\"ims\"", priority: .normal)
        enqueueAT("AT+COPS?", priority: .normal)
        enqueueAT("AT+CLCC", priority: .high, tag: "CLCC_SYNC")
        pollSMSMessages()
    }

    public func triggerManualMBNAutoSel() {
        enqueueAT("AT+QMBNCFG=\"AutoSel\",1", priority: .normal, tag: "MBN_AUTOSEL")
        enqueueAT("AT+QMBNCFG=\"List\"", priority: .normal)
    }

    public func pollModemRegistrationAndSignal() {
        guard atBridgeConnected, !queueCore.isSendingSMS, !queueCore.isSyncingBarrier else { return }
        enqueueAT("AT+CSQ", priority: .low)
        enqueueAT("AT+CEREG?", priority: .low)
        enqueueAT("AT+COPS?", priority: .low)
    }

    public func setVoLTE(_ enabled: Bool) {
        enqueueAT("AT+QCFG=\"ims\",\(enabled ? 1 : 0)", priority: .normal)
        enqueueAT("AT+QCFG=\"ims\"", priority: .normal)
    }

    public func configureCarrierAPNExplicit(apn: String) {
        let cleanAPN = ATCodecUtils.sanitizeAPN(apn)
        guard !cleanAPN.isEmpty else { return }
        enqueueAT("AT+CGDCONT=1,\"IPV4V6\",\"\(cleanAPN)\"", priority: .normal)
    }

    // MARK: - 5. Synchronous Call Control & Coordinator Integration
    public func dial(number: String) {
        guard atBridgeConnected else {
            notifyWebUI(event: "toast", data: ["msg": "AT 通道未连接，无法发起拨号"])
            return
        }
        guard !queueCore.isSendingSMS else {
            notifyWebUI(event: "toast", data: ["msg": "短信正在提交中，请等待完成后再拨号"])
            return
        }
        guard callSM.state == .idle else {
            notifyWebUI(event: "toast", data: ["msg": "当前已有通话或正在挂断中"])
            return
        }

        let cleanNumber = ATCodecUtils.sanitizePhoneNumber(number)
        guard !cleanNumber.isEmpty else { return }

        self.consecutiveEmptyCLCCCount = 0
        self.callDialStartTime = Date()
        let uuid = callCoordinator.startOutgoingDial(number: cleanNumber)

        let handle = CXHandle(type: .phoneNumber, value: cleanNumber)
        let startAction = CXStartCallAction(call: uuid, handle: handle)
        callController.request(CXTransaction(action: startAction)) { [weak self] error in
            if error != nil {
                DispatchQueue.main.async {
                    self?.performModemDial(cleanNumber: cleanNumber)
                }
            }
        }
    }

    private func performModemDial(cleanNumber: String) {
        enqueueAT("AT+QPCMV=1,2", priority: .high, tag: "QPCMV_ON")
        enqueueAT("ATD\(cleanNumber);", priority: .high, timeout: 8.0, tag: "ATD_CMD")
        startCLCCPolling()
    }

    public func answerIncomingCall() {
        guard atBridgeConnected else {
            notifyWebUI(event: "toast", data: ["msg": "AT 通道未连接，无法接听"])
            return
        }
        guard callSM.state == .incoming else { return }
        self.consecutiveEmptyCLCCCount = 0

        if queueCore.isSendingSMS && queueCore.smsBodyDispatched {
            notifyWebUI(event: "toast", data: ["msg": "⚠️ 短信正在提交运营商(最多等待12s)，接听指令已优先排队"])
        }
        _ = callSM.transition(to: .connecting, number: callSM.activeNumber)

        if let uuid = callCoordinator.activeCallUUID {
            callController.request(CXTransaction(action: CXAnswerCallAction(call: uuid))) { [weak self] error in
                if error != nil {
                    DispatchQueue.main.async {
                        self?.performModemAnswer()
                    }
                }
            }
        } else {
            performModemAnswer()
        }
    }

    private func performModemAnswer() {
        _ = queueCore.abortUnsubmittedSMSPromptWithESC(reason: "被接听来电中断")
        enqueueAT("AT+QPCMV=1,2", priority: .high, tag: "QPCMV_ON")
        enqueueAT("ATA", priority: .high, timeout: 6.0)
        startCLCCPolling()
    }

    public func hangup(fromCallKit: Bool = false) {
        let outcome = callCoordinator.handleLocalHangup()
        guard outcome.didBeginHangup else { return }
        let existingUUID = outcome.endedCallKitUUID
        stopCLCCPolling()
        queueCore.removeTag("CLCC")
        self.consecutiveEmptyCLCCCount = 0
        self.callDialStartTime = nil

        _ = queueCore.abortUnsubmittedSMSPromptWithESC(reason: "被挂断操作中断")
        enqueueAT("ATH", priority: .high, tag: "ATH_DONE")
        enqueueAT("AT+CHUP", priority: .high)
        enqueueAT("AT+QPCMV=0", priority: .high, tag: "QPCMV_OFF")

        if !fromCallKit, let uuid = existingUUID {
            callController.request(CXTransaction(action: CXEndCallAction(call: uuid))) { _ in }
        }
    }

    private func handleRemoteCallEnded(endReasonLabel: String?, isDialFailure: Bool) {
        let wasIncomingUnanswered = (!callSM.isOutgoing && callSM.state == .incoming)
        let wasOutgoingUnconnected = (callSM.isOutgoing && callSM.state != .connected)
        let lastNum = callSM.activeNumber.isEmpty ? "未知号码" : callSM.activeNumber

        stopCLCCPolling()
        queueCore.removeTag("CLCC")
        enqueueAT("AT+QPCMV=0", priority: .high, tag: "QPCMV_OFF")

        self.consecutiveEmptyCLCCCount = 0
        self.callDialStartTime = nil

        if wasIncomingUnanswered {
            notifyWebUI(event: "callEndEvent", data: ["number": lastNum, "logType": "未接来电", "toast": "未接来电: \(lastNum)"])
        } else if wasOutgoingUnconnected {
            let reasonText = endReasonLabel ?? "未接通"
            notifyWebUI(event: "callEndEvent", data: ["number": lastNum, "logType": "呼出\(reasonText)", "toast": "通话未接通: \(reasonText)"])
        } else if let reason = endReasonLabel {
            notifyWebUI(event: "callEndEvent", data: ["number": lastNum, "logType": "", "toast": "通话结束 (\(reason))"])
        }

        _ = callCoordinator.handleCallEnded(isDialFailure: isDialFailure)
    }

    public func sendDTMF(_ digit: String) {
        let clean = ATCodecUtils.sanitizeDTMF(digit)
        guard !clean.isEmpty else { return }
        enqueueAT("AT+VTS=\"\(clean)\"", priority: .high)
    }

    private func startCLCCPolling() {
        let block = { [weak self] in
            guard let self = self else { return }
            if let existing = self.clccPollTimer, existing.isValid { return }
            self.clccPollTimer = Timer.scheduledTimer(withTimeInterval: 1.5, repeats: true) { [weak self] _ in
                guard let self = self else { return }
                guard self.callSM.state.shouldProcessActiveCLCC else { return }
                guard !self.queueCore.awaitingSMSPrompt, !self.queueCore.isSyncingBarrier else { return }
                let alreadyQueued = self.queueCore.queue.contains(where: { $0.tag == "CLCC" }) || self.queueCore.currentCommand?.tag == "CLCC"
                if !alreadyQueued {
                    self.enqueueAT("AT+CLCC", priority: .high, timeout: 3.0, tag: "CLCC")
                }
            }
        }
        if Thread.isMainThread { block() } else { DispatchQueue.main.async(execute: block) }
    }

    private func stopCLCCPolling() {
        clccPollTimer?.invalidate()
        clccPollTimer = nil
    }

    // MARK: - 6. SMS Engine with Auto-Queued Retry During Late-Receipt Window (Fixes Bug 5)
    public func pollSMSMessages() {
        guard atBridgeConnected else { return }
        if queueCore.isSendingSMS || callSM.state != .idle {
            queueCore.needsPendingSMSPoll = true
            return
        }
        enqueueAT("AT+CMGF=1", priority: .normal)
        enqueueAT("AT+CSCS=\"UCS2\"", priority: .normal)
        enqueueAT("AT+CMGL=\"ALL\"", priority: .normal, timeout: 8.0, tag: "CMGL_CMD")
    }

    public func sendSMS(clientId: String, recipient: String, body: String) {
        guard callSM.state == .idle else {
            notifyWebUI(event: "smsSendResult", data: ["id": clientId, "status": "failed", "error": "通话进行中，请挂断后再发短信"])
            return
        }
        let cleanRecipient = ATCodecUtils.sanitizePhoneNumber(recipient)
        guard !cleanRecipient.isEmpty else {
            notifyWebUI(event: "smsSendResult", data: ["id": clientId, "status": "failed", "error": "收件人号码无效"])
            return
        }
        guard ATCodecUtils.isValidUCS2Length(body) else {
            notifyWebUI(event: "smsSendResult", data: ["id": clientId, "status": "failed", "error": "UCS2 长度超限 (\(body.utf16.count)/70)"])
            return
        }
        guard atBridgeConnected else {
            notifyWebUI(event: "smsSendResult", data: ["id": clientId, "status": "failed", "error": "AT 通道未连接"])
            return
        }

        // Fixes Bug 5: If within the 60s late-receipt window, auto-queue the SMS/retry instead of returning a "failed" bubble!
        if queueCore.hasPendingLateSMSReceipt {
            let rem = max(1, queueCore.remainingLateWindowSeconds)
            if let idx = nativeSmsList.firstIndex(where: { $0.id == clientId }) {
                nativeSmsList[idx].status = "queued_wait"
                nativeSmsList[idx].time = "已排队 (\(rem)s 后自动发送)"
                scheduleDebouncedDiskSave()
            } else {
                appendToNativeSmsStore(StoredSMSItem(
                    id: clientId,
                    dedupKey: clientId,
                    peer: cleanRecipient,
                    dir: "out",
                    body: body,
                    time: "已排队 (\(rem)s 后自动发送)",
                    status: "queued_wait",
                    otp: "",
                    timestamp: Date().timeIntervalSince1970
                ))
            }
            queueCore.enqueueSMSForAfterLateWindow(clientId: clientId, recipient: cleanRecipient, body: body)
            updateLateWindowCountdownTimer()
            return
        }

        guard !queueCore.isSendingSMS else {
            notifyWebUI(event: "smsSendResult", data: ["id": clientId, "status": "failed", "error": "上一条短信正在提交中"])
            return
        }

        if let idx = nativeSmsList.firstIndex(where: { $0.id == clientId }) {
            nativeSmsList[idx].status = "pending"
            nativeSmsList[idx].time = "发送中..."
            scheduleDebouncedDiskSave()
        } else {
            appendToNativeSmsStore(StoredSMSItem(
                id: clientId,
                dedupKey: clientId,
                peer: cleanRecipient,
                dir: "out",
                body: body,
                time: "发送中...",
                status: "pending",
                otp: "",
                timestamp: Date().timeIntervalSince1970
            ))
        }

        queueCore.isSendingSMS = true
        queueCore.awaitingSMSPrompt = false
        queueCore.smsBodyDispatched = false
        queueCore.pendingSMSClientId = clientId
        queueCore.pendingSMSBodyHex = ATCodecUtils.encodeToUCS2Hex(body)
        let ucs2Recipient = ATCodecUtils.encodeToUCS2Hex(cleanRecipient)
        pushFullStatusToWeb()

        enqueueAT("AT+CMGF=1", priority: .normal, batchId: clientId)
        enqueueAT("AT+CSCS=\"UCS2\"", priority: .normal, batchId: clientId)
        enqueueAT("AT+CSMP=17,167,0,8", priority: .normal, batchId: clientId)
        enqueueAT("AT+CMGS=\"\(ucs2Recipient)\"", priority: .normal, timeout: 15.0, tag: "CMGS_CMD", batchId: clientId)
    }

    private static func currentTimeString() -> String {
        let fmt = DateFormatter()
        fmt.dateFormat = "HH:mm"
        return fmt.string(from: Date())
    }

    // MARK: - 7. Stream Reader & Stateful ATStreamDecoder Dispatch (Fixes Bug 10)
    private func receiveATResponse(generation: UInt64) {
        atConnection?.receive(minimumIncompleteLength: 1, maximumLength: 8192) { [weak self] data, _, isComplete, error in
            guard let self = self else { return }
            DispatchQueue.main.async {
                guard self.connectionGeneration == generation else { return }
                if let data = data, let chunk = String(data: data, encoding: .utf8) {
                    self.processIncomingATStream(chunk)
                }
                if error == nil && !isComplete {
                    self.receiveATResponse(generation: generation)
                }
            }
        }
    }

    private func processIncomingATStream(_ chunk: String) {
        queueCore.notifyIncomingSerialActivity()
        receiveBuffer += chunk

        if queueCore.currentCommand?.tag == "CMGS_CMD", !queueCore.smsBodyDispatched, receiveBuffer.contains(">") {
            receiveBuffer = receiveBuffer.replacingOccurrences(of: ">", with: "")
            recordSerialTrace("RX", "> [CMGS Prompt]")
            queueCore.handlePromptReceivedForCMGS()
        }

        let lines = receiveBuffer.components(separatedBy: "\r\n")
        guard lines.count > 1 else { return }

        for i in 0..<(lines.count - 1) {
            let rawLine = lines[i]
            let trimmed = rawLine.trimmingCharacters(in: .whitespacesAndNewlines)
            if !trimmed.isEmpty {
                recordSerialTrace("RX", trimmed)
            }
            let events = streamDecoder.feedLine(rawLine, fallbackTime: Self.currentTimeString())
            for ev in events {
                handleDecodedATEvent(ev, rawLine: trimmed)
            }
        }
        receiveBuffer = lines.last ?? ""
    }

    private func handleDecodedATEvent(_ parsed: ATParsedEvent, rawLine: String) {
        if case .cmglMessage = parsed {
            notifyWebUI(event: "atLog", data: ["text": "<<< [+CMGL 短信条目已解析]"])
        } else if !rawLine.isEmpty {
            notifyWebUI(event: "atLog", data: ["text": "<<< " + rawLine])
        }

        switch parsed {
        case .cmgsReceipt:
            queueCore.handleCMGSReceiptLine()
            updateLateWindowCountdownTimer()
        case .ok:
            queueCore.handleTerminalResponse(isError: false, line: "OK")
            updateLateWindowCountdownTimer()
        case .error(let errLine):
            queueCore.handleTerminalResponse(isError: true, line: errLine)
            updateLateWindowCountdownTimer()
        case .cgmr:
            queueCore.handleBarrierProbeDataLineReceived()
        case .csq(let rssi, let bars):
            queueCore.handleCSQLineReceived()
            self.signalCSQ = rssi
            self.signalBars = bars
            pushFullStatusToWeb()
        case .cpin(let ready, let stateText):
            self.simStatus = stateText
            if !ready { self.operatorName = "SIM卡未就绪" }
            pushFullStatusToWeb()
        case .cops(let opName):
            self.operatorName = opName
            pushFullStatusToWeb()
        case .cereg(let stat, let desc, let isReg):
            self.regStatus = desc
            if !isReg {
                self.operatorName = (stat == 2) ? "正在搜网..." : "未注册运营商"
            }
            pushFullStatusToWeb()
        case .imsConfig(let volte, let imsReg):
            self.volteEnabled = volte
            self.imsRegistered = imsReg
            pushFullStatusToWeb()
        case .csca(let num):
            self.smscNumber = num
            pushFullStatusToWeb()
        case .cmti:
            pollSMSMessages()
        case .cmglHeader:
            break
        case .cmglMessage(let msgIndex, let isIncoming, let sender, let msgTime, let rawBody):
            guard isIncoming else { return }
            let decodedSender = ATCodecUtils.decodeUCS2OrPlain(sender)
            let decodedBody = rawBody.isEmpty ? "" : ATCodecUtils.decodeUCS2OrPlain(rawBody)
            let dedupKey = "\(decodedSender)|\(msgTime)|\(decodedBody)"
            let otp = ATCodecUtils.extractOTPCode(from: decodedBody) ?? ""

            if !seenSmsKeys.contains(dedupKey) {
                seenSmsKeys.insert(dedupKey)
                let newItem = StoredSMSItem(
                    id: "in_" + UUID().uuidString,
                    dedupKey: dedupKey,
                    peer: decodedSender,
                    dir: "in",
                    body: decodedBody,
                    time: msgTime,
                    status: "received",
                    otp: otp,
                    timestamp: Date().timeIntervalSince1970
                )
                appendToNativeSmsStore(newItem)
                postSMSNotification(peer: decodedSender, body: decodedBody.isEmpty ? "[空短信]" : decodedBody)
            }

            if let idx = Int(msgIndex) {
                pendingDeleteModemIndices[dedupKey] = idx
            }

            notifyWebUI(event: "smsReceived", data: [
                "dedupKey": dedupKey,
                "sender": decodedSender,
                "time": msgTime,
                "body": decodedBody,
                "otp": otp
            ])
        case .ring:
            guard callSM.state.canAcceptIncomingRing else { return }
            guard !callSM.isIncomingRingSuppressed(for: "未知来电") else { return }

            _ = queueCore.abortUnsubmittedSMSPromptWithESC(reason: "来电中断")
            if queueCore.isSendingSMS && queueCore.smsBodyDispatched {
                notifyWebUI(event: "toast", data: ["msg": "⚠️ 短信回执等待中，接听最多等待 12 秒"])
            }

            self.consecutiveEmptyCLCCCount = 0
            callCoordinator.handleRingArrived()
            startCLCCPolling()
        case .clip(let caller):
            guard callSM.state != .hangingUp else { return }
            if callSM.state == .idle && callSM.isIncomingRingSuppressed(for: caller) {
                return
            }
            self.consecutiveEmptyCLCCCount = 0
            _ = queueCore.abortUnsubmittedSMSPromptWithESC(reason: "来电中断")
            callCoordinator.handleClipArrived(caller: caller)
            startCLCCPolling()
        case .clcc(let dir, let stat, let rawNum):
            guard callSM.state != .hangingUp else { return }
            clccSawCallLineInCurrentPoll = true
            consecutiveEmptyCLCCCount = 0

            let num = !rawNum.isEmpty ? rawNum : (callSM.activeNumber.isEmpty ? "未知号码" : callSM.activeNumber)
            let outgoingFlag = (dir == 0)

            if !outgoingFlag && callSM.state == .idle && callSM.isIncomingRingSuppressed(for: num) {
                return
            }

            let clccResult = callCoordinator.handleCLCCLine(dir: dir, stat: stat, number: num)
            if clccResult.didTransitionToConnected && clccResult.isOutgoing, let uuid = callCoordinator.activeCallUUID {
                callProvider?.reportOutgoingCall(with: uuid, connectedAt: Date())
            }
            if [0, 2, 3, 4, 5].contains(stat) {
                startCLCCPolling()
            }
        case .callEnded(let reasonLabel):
            if callSM.state.shouldProcessActiveCLCC {
                handleRemoteCallEnded(endReasonLabel: reasonLabel, isDialFailure: false)
            }
        case .other:
            break
        }
    }

    // MARK: - Error text / port probe (v15.2)
    private func describeNWError(_ err: NWError) -> String {
        if case .posix(let code) = err {
            switch code {
            case .ECONNREFUSED:
                return "连接被拒绝：\(atHost):\(atPort) 上没有服务在监听"
            case .ETIMEDOUT:
                return "连接超时"
            case .EHOSTUNREACH, .ENETUNREACH, .ENETDOWN:
                return "主机不可达"
            default:
                break
            }
        }
        return err.localizedDescription
    }

    /// Probes common TCP ports on the module's address and reports which accept a connection.
    public func probeModulePorts() {
        let host = atHost
        let ports: [UInt16] = [21, 22, 23, 53, 80, 81, 443, 554, 1883, 2323, 5000, 5555, 6000, 7547, 7575,
                               8000, 8080, 8081, 8443, 8888, 9000, 9999, 20000, 30000]
        notifyWebUI(event: "portProbe", data: ["phase": "start", "host": host, "total": ports.count])
        let state = PortProbeState()
        let group = DispatchGroup()
        for p in ports {
            guard let port = NWEndpoint.Port(rawValue: p) else { continue }
            group.enter()
            let conn = NWConnection(host: NWEndpoint.Host(host), port: port, using: .tcp)
            let once = OneShotFlag()
            let finish: (String) -> Void = { result in
                guard once.fire() else { return }
                state.record(port: Int(p), result: result)
                conn.cancel()
                group.leave()
            }
            conn.stateUpdateHandler = { st in
                switch st {
                case .ready:
                    finish("open")
                case .failed(let e), .waiting(let e):
                    if case .posix(let code) = e, code == .ECONNREFUSED {
                        finish("refused")
                    } else {
                        finish("other")
                    }
                default:
                    break
                }
            }
            conn.start(queue: .global())
            DispatchQueue.global().asyncAfter(deadline: .now() + 2.0) { finish("timeout") }
        }
        group.notify(queue: .main) { [weak self] in
            let snap = state.snapshot()
            self?.notifyWebUI(event: "portProbe", data: [
                "phase": "done", "host": host, "open": snap.open,
                "refused": snap.refused, "other": snap.other
            ])
        }
    }

    private func notifyWebUI(event: String, data: [String: Any]) {
        guard let jsonData = try? JSONSerialization.data(withJSONObject: ["event": event, "data": data]),
              let jsonStr = String(data: jsonData, encoding: .utf8) else { return }
        let js = "window.onNativeEvent && window.onNativeEvent(\(jsonStr));"
        onJavaScriptEvent?(js)
    }

    // MARK: - CXProviderDelegate
    public func providerDidReset(_ provider: CXProvider) {
        stopCLCCPolling()
        callCoordinator.resetSessionFlags()
        callSM.resetOnDisconnect()
        callKitAudioActive = false
    }

    public func provider(_ provider: CXProvider, perform action: CXStartCallAction) {
        prepareAudioSessionCategory()
        provider.reportOutgoingCall(with: action.callUUID, startedConnectingAt: Date())
        performModemDial(cleanNumber: callSM.activeNumber)
        action.fulfill()
    }

    public func provider(_ provider: CXProvider, perform action: CXAnswerCallAction) {
        prepareAudioSessionCategory()
        performModemAnswer()
        action.fulfill()
    }

    public func provider(_ provider: CXProvider, perform action: CXEndCallAction) {
        hangup(fromCallKit: true)
        action.fulfill()
    }

    public func provider(_ provider: CXProvider, didActivate audioSession: AVAudioSession) {
        self.callKitAudioActive = true
        recordSerialTrace("AUDIO", "CallKit didActivate audioSession")
        notifyWebUI(event: "atLog", data: ["text": "[CallKit 音频会话已激活]"])
        pushFullStatusToWeb()
        notifyWebUI(event: "callState", data: [
            "state": callSM.state.rawValue,
            "number": callSM.activeNumber,
            "uac": hasUSBAudioDevice(),
            "audioActive": true,
            "qpcmv": qpcmvEnabled
        ])
    }

    public func provider(_ provider: CXProvider, didDeactivate audioSession: AVAudioSession) {
        self.callKitAudioActive = false
        recordSerialTrace("AUDIO", "CallKit didDeactivate audioSession")
        notifyWebUI(event: "atLog", data: ["text": "[CallKit 音频会话已释放]"])
        pushFullStatusToWeb()
    }
}


// MARK: - Experimental (v15.2): does iOS expose the module's USB device to this app?
// Reads the IOKit registry through dlopen and tries IOServiceOpen on each USB device found.
// Expected on a normal sideloaded iPhone: no device visible, or IOServiceOpen refused.
extension QuectelModemDriver {
    public func probeUSBRegistry() {
        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            guard let self = self else { return }
            var lines: [String] = []
            defer {
                let out = lines
                DispatchQueue.main.async {
                    self.notifyWebUI(event: "usbProbe", data: ["lines": out])
                }
            }
            func hex(_ kr: Int32) -> String { return String(format: "0x%08X", UInt32(bitPattern: kr)) }

            guard let handle = dlopen("/System/Library/Frameworks/IOKit.framework/IOKit", RTLD_NOW) else {
                lines.append("无法加载 IOKit 框架（被系统或沙盒拒绝）")
                return
            }
            typealias MatchingFn = @convention(c) (UnsafePointer<CChar>) -> Unmanaged<CFDictionary>?
            typealias GetServicesFn = @convention(c) (UInt32, CFDictionary?, UnsafeMutablePointer<UInt32>) -> Int32
            typealias NextFn = @convention(c) (UInt32) -> UInt32
            typealias PropFn = @convention(c) (UInt32, CFString, CFAllocator?, UInt32) -> Unmanaged<CFTypeRef>?
            typealias ReleaseFn = @convention(c) (UInt32) -> Int32
            typealias OpenFn = @convention(c) (UInt32, UInt32, UInt32, UnsafeMutablePointer<UInt32>) -> Int32
            typealias CloseFn = @convention(c) (UInt32) -> Int32

            guard let m = dlsym(handle, "IOServiceMatching"),
                  let g = dlsym(handle, "IOServiceGetMatchingServices"),
                  let n = dlsym(handle, "IOIteratorNext"),
                  let p = dlsym(handle, "IORegistryEntryCreateCFProperty"),
                  let r = dlsym(handle, "IOObjectRelease"),
                  let o = dlsym(handle, "IOServiceOpen"),
                  let c = dlsym(handle, "IOServiceClose") else {
                lines.append("IOKit 缺少所需符号")
                return
            }
            let matching = unsafeBitCast(m, to: MatchingFn.self)
            let getServices = unsafeBitCast(g, to: GetServicesFn.self)
            let next = unsafeBitCast(n, to: NextFn.self)
            let prop = unsafeBitCast(p, to: PropFn.self)
            let release = unsafeBitCast(r, to: ReleaseFn.self)
            let open = unsafeBitCast(o, to: OpenFn.self)
            let close = unsafeBitCast(c, to: CloseFn.self)

            var totalFound = 0
            for className in ["IOUSBHostDevice", "IOUSBDevice"] {
                guard let dict = matching(className) else {
                    lines.append("\(className): 无法创建匹配字典")
                    continue
                }
                var iterator: UInt32 = 0
                let kr = getServices(0, dict.takeUnretainedValue(), &iterator)
                if kr != 0 {
                    lines.append("\(className): 枚举失败 kr=\(hex(kr))")
                    continue
                }
                var found = 0
                while true {
                    let service = next(iterator)
                    if service == 0 { break }
                    found += 1
                    totalFound += 1
                    let vid = (prop(service, "idVendor" as CFString, nil, 0)?.takeRetainedValue() as? NSNumber)?.intValue ?? -1
                    let pid = (prop(service, "idProduct" as CFString, nil, 0)?.takeRetainedValue() as? NSNumber)?.intValue ?? -1
                    let name = (prop(service, "USB Product Name" as CFString, nil, 0)?.takeRetainedValue() as? String) ?? "?"
                    var line = "\(className): VID=\(String(format: "0x%04X", vid)) PID=\(String(format: "0x%04X", pid)) \(name)"
                    var conn: UInt32 = 0
                    let okr = open(service, mach_task_self_, 0, &conn)
                    if okr == 0 {
                        line += " → IOServiceOpen 成功 (conn=\(conn))"
                        _ = close(conn)
                    } else {
                        line += " → IOServiceOpen 被拒绝 kr=\(hex(okr))"
                    }
                    lines.append(line)
                    _ = release(service)
                }
                _ = release(iterator)
                if found == 0 { lines.append("\(className): 注册表中没有匹配设备") }
            }
            if totalFound == 0 {
                lines.append("结论：当前 App 的沙盒内看不到任何 USB 设备（包括这个模块）。")
            }
        }
    }
}
