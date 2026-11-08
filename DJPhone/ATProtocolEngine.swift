//
//  ATProtocolEngine.swift
//  DJPhone v15.1
//
//  Fixes & Architecture Upgrades in v15.1:
//  1. Dedicated `AT+CGMR` Barrier Probe, Single-Decrement Counting & 30s Orphan Expiry (Fixes Bug 1 & Bug 2):
//     - Replaces `AT+CSQ` with `AT+CGMR` (which is NEVER used by periodic status polling and always returns a
//       firmware revision line `+CGMR: ...` or `EG25...` before its `OK`).
//     - Consequently, a late `+CSQ:` from a timed-out periodic status poll can NEVER be mistaken for a barrier probe
//       and will NEVER swallow `ATH`'s `OK`!
//     - Inside the barrier, `unansweredBarrierProbeCount` is decremented ONLY ONCE per probe (on the data line in
//       `handleBarrierProbeDataLineReceived()`, or on `OK` ONLY if no data line preceded it), fixing the double-decrement
//       bug when 2 probes are in flight and only 1 replies before the barrier exits!
//     - `unansweredOrphanProbesSent` is reset on every `enterSyncBarrier()` and expires automatically after 30.0s.
//  2. Lock-Screen-Writable Session Fallback Store (`.completeFileProtectionUntilFirstUserAuthentication`) (Fixes Bug 3):
//     - `writeSessionFallbackStore` uses `.completeFileProtectionUntilFirstUserAuthentication` so new SMS messages
//       received while the screen is locked are persisted to disk immediately and survive app termination before unlock.
//  3. Unlocked Main Store Corruption Self-Healing & Deduplicated/Capped Read-Only Fallback (Fixes Bug 4 & Bug 6):
//     - `loadAndMigrateIfNeeded` accepts `isProtectedDataAvailable: Bool` (from `UIApplication.shared.isProtectedDataAvailable`).
//     - If the device is ALREADY unlocked (`isProtectedDataAvailable == true`) and both `djphone_sms_store.json` and
//       its backups fail to decode, the corrupted main store files are quarantined to `.corrupt` and the main store
//       is immediately rebuilt from fallback/legacy sources (`loadFailedReadOnlyGuard = false`), preventing permanent read-only lock!
//     - Deduplication (`mergedById` / `dedupKeyToId`) and the `maxCount` (500) cap now run BEFORE returning in the
//       locked read-only fallback branch as well.
//  4. Two-Generation N-1 (`.bak`) & N-2 (`.bak2`) Backup Rotation with Change Check (Fixes Bug 5):
//     - Maintains both `djphone_sms_store.json.bak` (N-1) and `djphone_sms_store.json.bak2` (N-2), rotating only
//       when the message list actually changes (`prevItems != messages`), and recovering from `.bak` or `.bak2`.
//

import Foundation

// MARK: - 1. Injectable Clock & Timer Scheduler
public protocol ATCancellableTimer: AnyObject {
    func cancel()
}

public protocol ATScheduler: AnyObject {
    var now: Date { get }
    @discardableResult
    func scheduleOnce(delay: TimeInterval, action: @escaping () -> Void) -> ATCancellableTimer
}

public final class RunLoopTimerToken: ATCancellableTimer {
    private var timer: Timer?
    public init(_ timer: Timer) { self.timer = timer }
    public func cancel() {
        timer?.invalidate()
        timer = nil
    }
}

public final class RunLoopScheduler: ATScheduler {
    public init() {}
    public var now: Date { Date() }
    @discardableResult
    public func scheduleOnce(delay: TimeInterval, action: @escaping () -> Void) -> ATCancellableTimer {
        let t = Timer.scheduledTimer(withTimeInterval: delay, repeats: false) { _ in
            action()
        }
        return RunLoopTimerToken(t)
    }
}

public final class MockScheduler: ATScheduler {
    private final class MockTask: ATCancellableTimer {
        let id: UInt64
        let fireTime: TimeInterval
        let action: () -> Void
        var isCancelled: Bool = false
        init(id: UInt64, fireTime: TimeInterval, action: @escaping () -> Void) {
            self.id = id
            self.fireTime = fireTime
            self.action = action
        }
        func cancel() { isCancelled = true }
    }

    private var currentSeconds: TimeInterval = 1_000_000.0
    private var nextTaskId: UInt64 = 1
    private var tasks: [MockTask] = []

    public init() {}
    public var now: Date { Date(timeIntervalSince1970: currentSeconds) }

    @discardableResult
    public func scheduleOnce(delay: TimeInterval, action: @escaping () -> Void) -> ATCancellableTimer {
        let task = MockTask(id: nextIdValue(), fireTime: currentSeconds + max(0, delay), action: action)
        tasks.append(task)
        tasks.sort { $0.fireTime < $1.fireTime }
        return task
    }

    private func nextIdValue() -> UInt64 {
        defer { nextTaskId &+= 1 }
        return nextTaskId
    }

    public func advance(by delta: TimeInterval) {
        let target = currentSeconds + max(0, delta)
        while let next = tasks.first(where: { !$0.isCancelled && $0.fireTime <= target }) {
            currentSeconds = next.fireTime
            next.isCancelled = true
            next.action()
        }
        currentSeconds = target
        tasks.removeAll { $0.isCancelled }
    }
}

// MARK: - 2. Call Lifecycle State & Result-Driven Pre-Hangup CLCC Watchdog
public enum CallLifecycleState: String, Equatable {
    case idle = "idle"
    case dialing = "dialing"
    case alerting = "alerting"
    case incoming = "incoming"
    case connecting = "connecting"
    case connected = "connected"
    case hangingUp = "hangingUp"

    public var canAcceptIncomingRing: Bool {
        return self == .idle
    }

    public var shouldProcessActiveCLCC: Bool {
        switch self {
        case .dialing, .alerting, .incoming, .connecting, .connected:
            return true
        case .idle, .hangingUp:
            return false
        }
    }

    public var transientResidenceTimeout: TimeInterval? {
        switch self {
        case .dialing, .alerting, .incoming:
            return 75.0
        case .connecting:
            return 35.0
        case .idle, .connected, .hangingUp:
            return nil
        }
    }
}

public final class ATCallStateMachine {
    public private(set) var state: CallLifecycleState = .idle
    public private(set) var activeNumber: String = ""
    public private(set) var isOutgoing: Bool = false
    public private(set) var isAwaitingWatchdogCLCCVerification: Bool = false
    public private(set) var inconclusiveWatchdogRetryCount: Int = 0
    public static let maxInconclusiveWatchdogRetries: Int = 3
    public private(set) var transitionLog: [String] = []

    private let scheduler: ATScheduler
    private var hangupWatchdog: ATCancellableTimer? = nil
    private var transientStateWatchdog: ATCancellableTimer? = nil
    private var suppressedCaller: String? = nil
    private var suppressIncomingUntil: Date = .distantPast

    public var onStateChanged: ((CallLifecycleState, String) -> Void)?
    public var onRequestWatchdogCLCCVerification: ((CallLifecycleState, String) -> Void)?
    public var onTransientStateTimedOut: ((CallLifecycleState, String) -> Void)?

    public init(scheduler: ATScheduler) {
        self.scheduler = scheduler
    }

    public func isIncomingRingSuppressed(for caller: String) -> Bool {
        guard state == .idle else { return true }
        guard scheduler.now < suppressIncomingUntil else { return false }
        if let sup = suppressedCaller, !sup.isEmpty, !caller.isEmpty, caller != "未知来电" {
            return sup == caller
        }
        return true
    }

    @discardableResult
    public func transition(to newState: CallLifecycleState, number: String? = nil, outgoing: Bool? = nil) -> Bool {
        let resolvedNumber = number ?? activeNumber
        if let out = outgoing {
            self.isOutgoing = out
        }

        if self.state == newState && self.activeNumber == resolvedNumber {
            if newState == .connected || newState == .idle {
                isAwaitingWatchdogCLCCVerification = false
                inconclusiveWatchdogRetryCount = 0
                transientStateWatchdog?.cancel()
                transientStateWatchdog = nil
            }
            return false
        }

        if self.state == .hangingUp && newState != .idle {
            transitionLog.append("BLOCKED:\(state.rawValue)->\(newState.rawValue)")
            return false
        }

        let prevState = self.state
        self.state = newState
        self.activeNumber = (newState == .idle) ? "" : resolvedNumber
        self.isAwaitingWatchdogCLCCVerification = false
        self.inconclusiveWatchdogRetryCount = 0
        transitionLog.append("\(prevState.rawValue)->\(newState.rawValue)")

        if newState != .hangingUp {
            hangupWatchdog?.cancel()
            hangupWatchdog = nil
        }

        transientStateWatchdog?.cancel()
        transientStateWatchdog = nil
        if let maxResidence = newState.transientResidenceTimeout {
            let watchedState = newState
            transientStateWatchdog = scheduler.scheduleOnce(delay: maxResidence) { [weak self] in
                self?.startOrExtendWatchdogCLCCVerificationRound(watchedState: watchedState)
            }
        }

        onStateChanged?(self.state, self.activeNumber)
        return true
    }

    private func startOrExtendWatchdogCLCCVerificationRound(watchedState: CallLifecycleState) {
        guard state == watchedState else { return }
        let num = activeNumber
        if let verifyHandler = onRequestWatchdogCLCCVerification {
            isAwaitingWatchdogCLCCVerification = true
            verifyHandler(watchedState, num)
            transientStateWatchdog?.cancel()
            transientStateWatchdog = scheduler.scheduleOnce(delay: 15.0) { [weak self] in
                guard let self = self, self.state == watchedState, self.isAwaitingWatchdogCLCCVerification else { return }
                self.handleWatchdogVerificationInconclusiveOrRetry()
            }
        } else if let timeoutHandler = onTransientStateTimedOut {
            timeoutHandler(watchedState, num)
        } else {
            _ = beginHangup()
        }
    }

    public func handleWatchdogVerificationInconclusiveOrRetry() {
        guard isAwaitingWatchdogCLCCVerification else { return }
        if inconclusiveWatchdogRetryCount < Self.maxInconclusiveWatchdogRetries {
            inconclusiveWatchdogRetryCount += 1
            startOrExtendWatchdogCLCCVerificationRound(watchedState: state)
        } else {
            confirmWatchdogCLCCVerificationFailed()
        }
    }

    public func confirmWatchdogCLCCVerificationFailed() {
        guard isAwaitingWatchdogCLCCVerification else { return }
        isAwaitingWatchdogCLCCVerification = false
        inconclusiveWatchdogRetryCount = 0
        transientStateWatchdog?.cancel()
        transientStateWatchdog = nil
        let currentSt = state
        let num = activeNumber
        onTransientStateTimedOut?(currentSt, num)
    }

    public func beginHangup() -> String? {
        guard state != .idle && state != .hangingUp else { return nil }
        let lastNum = activeNumber
        isAwaitingWatchdogCLCCVerification = false
        inconclusiveWatchdogRetryCount = 0
        suppressedCaller = lastNum
        suppressIncomingUntil = scheduler.now.addingTimeInterval(3.5)
        _ = transition(to: .hangingUp, number: lastNum)
        armHangupWatchdog(delay: 9.0)
        return lastNum
    }

    public func armHangupWatchdogOnATHDispatched() {
        guard state == .hangingUp else { return }
        armHangupWatchdog(delay: 4.0)
    }

    private func armHangupWatchdog(delay: TimeInterval) {
        hangupWatchdog?.cancel()
        hangupWatchdog = scheduler.scheduleOnce(delay: delay) { [weak self] in
            guard let self = self, self.state == .hangingUp else { return }
            self.completeHangupToIdle()
        }
    }

    public func completeHangupToIdle() {
        hangupWatchdog?.cancel()
        hangupWatchdog = nil
        transientStateWatchdog?.cancel()
        transientStateWatchdog = nil
        isAwaitingWatchdogCLCCVerification = false
        inconclusiveWatchdogRetryCount = 0
        suppressedCaller = activeNumber.isEmpty ? suppressedCaller : activeNumber
        suppressIncomingUntil = scheduler.now.addingTimeInterval(3.5)
        _ = transition(to: .idle, number: "")
    }

    public func resetOnDisconnect() {
        hangupWatchdog?.cancel()
        hangupWatchdog = nil
        transientStateWatchdog?.cancel()
        transientStateWatchdog = nil
        isAwaitingWatchdogCLCCVerification = false
        inconclusiveWatchdogRetryCount = 0
        suppressedCaller = nil
        suppressIncomingUntil = .distantPast
        _ = transition(to: .idle, number: "")
    }
}

// MARK: - 3. Priority Command Queue & Dedicated `AT+CGMR` Barrier Engine (Fixes Bug 1 & Bug 2)
public enum ATCommandPriority: Int, Comparable {
    case low = 1    // Periodic status polling
    case normal = 2 // SMS & user settings
    case high = 3   // Voice call control

    public static func < (lhs: ATCommandPriority, rhs: ATCommandPriority) -> Bool {
        return lhs.rawValue < rhs.rawValue
    }
}

public struct QueuedATCommand: Equatable {
    public let id: UInt64
    public let command: String
    public let priority: ATCommandPriority
    public let timeout: TimeInterval
    public let tag: String
    public let batchId: String

    public init(id: UInt64, command: String, priority: ATCommandPriority, timeout: TimeInterval = 4.0, tag: String = "", batchId: String = "") {
        self.id = id
        self.command = command
        self.priority = priority
        self.timeout = timeout
        self.tag = tag
        self.batchId = batchId
    }

    public var canProduceCMSError: Bool {
        let up = command.uppercased()
        return up.hasPrefix("AT+CMG") || up.hasPrefix("AT+CSC") || up.hasPrefix("AT+CSM") || up.hasPrefix("AT+CNM") || up.hasPrefix("AT+CPM")
    }
}

public struct QueuedSMSRetryItem: Equatable {
    public let clientId: String
    public let recipient: String
    public let body: String
    public init(clientId: String, recipient: String, body: String) {
        self.clientId = clientId
        self.recipient = recipient
        self.body = body
    }
}

public final class ATCommandQueueCore {
    /// Dedicated barrier probe command (`AT+CGMR`) that is NEVER used by periodic status polling (`CSQ`/`CEREG`/`COPS`)
    /// and always returns a firmware revision line (`+CGMR: ...` or `EG25...`) before its `OK`!
    public static let barrierProbeCommand: String = "AT+CGMR"
    public static let maxUnansweredBarrierProbes: Int = 2

    public private(set) var queue: [QueuedATCommand] = []
    public private(set) var currentCommand: QueuedATCommand? = nil
    public private(set) var isSyncingBarrier: Bool = false
    public private(set) var barrierSawProbeResponse: Bool = false
    public private(set) var initialBarrierProbeWindowElapsed: Bool = false
    public private(set) var unansweredBarrierProbeCount: Int = 0
    /// Tracks whether the current probe inside `isSyncingBarrier` already decremented `unansweredBarrierProbeCount`
    /// on its data line (`+CGMR:` / `EG25...`), preventing a second decrement on the trailing `OK` (Fixes Bug 1)!
    private var barrierCurrentProbeDecrementedOnDataLine: Bool = false

    public private(set) var unansweredOrphanProbesSent: Int = 0
    public private(set) var awaitingOrphanProbeTrailingOK: Bool = false
    public private(set) var lastTimedOutCommandId: UInt64? = nil

    // Active SMS State
    public var isSendingSMS: Bool = false
    public var awaitingSMSPrompt: Bool = false
    public var smsBodyDispatched: Bool = false
    public var pendingSMSClientId: String = ""
    public var pendingSMSBodyHex: String = ""

    // Decoupled Channel Protection Window vs. SMS Attribution
    public private(set) var isLateCMGSChannelWindowOpen: Bool = false
    public private(set) var lateAwaitingCMGSClientId: String? = nil
    public private(set) var lastUnknownSMSClientId: String? = nil
    public private(set) var awaitingLateCMGSTrailingOK: Bool = false
    public private(set) var lateCMGSWindowExpiresAt: Date? = nil
    public private(set) var queuedSMSAfterWindow: [QueuedSMSRetryItem] = []

    public var needsPendingSMSPoll: Bool = false
    public var isCallIdleProvider: (() -> Bool) = { true }

    private let scheduler: ATScheduler
    private var commandTimer: ATCancellableTimer? = nil
    private var silenceTimer: ATCancellableTimer? = nil
    private var barrierProbeTimeoutTimer: ATCancellableTimer? = nil
    private var orphanProbeTrailingOKTimer: ATCancellableTimer? = nil
    private var orphanProbesCountExpiryTimer: ATCancellableTimer? = nil
    private var lateCMGSWindowTimer: ATCancellableTimer? = nil
    private var lateCMGSTrailingOKTimer: ATCancellableTimer? = nil
    private var nextId: UInt64 = 1

    public var onWriteRaw: ((String) -> Void)?
    public var onCommandCompleted: ((QueuedATCommand, Bool, String) -> Void)?
    public var onCommandTimedOut: ((QueuedATCommand) -> Void)?
    public var onSMSResult: ((String, String, String) -> Void)? // (clientId, status, detail)
    public var onTriggerDeferredSMSPoll: (() -> Void)?
    public var onDispatchQueuedSMS: ((QueuedSMSRetryItem) -> Void)?

    public init(scheduler: ATScheduler = RunLoopScheduler()) {
        self.scheduler = scheduler
    }

    public var hasPendingLateSMSReceipt: Bool {
        return isLateCMGSChannelWindowOpen || lateAwaitingCMGSClientId != nil || awaitingLateCMGSTrailingOK
    }

    public var remainingLateWindowSeconds: Int {
        guard isLateCMGSChannelWindowOpen, let exp = lateCMGSWindowExpiresAt else { return 0 }
        let rem = Int(ceil(exp.timeIntervalSince(scheduler.now)))
        return max(0, rem)
    }

    public func clearUnknownSMSTracking(for clientId: String) {
        if lateAwaitingCMGSClientId == clientId {
            lateAwaitingCMGSClientId = nil
        }
        if lastUnknownSMSClientId == clientId {
            lastUnknownSMSClientId = nil
        }
        queuedSMSAfterWindow.removeAll { $0.clientId == clientId }
    }

    @discardableResult
    public func cancelQueuedSMS(clientId: String) -> Bool {
        guard let idx = queuedSMSAfterWindow.firstIndex(where: { $0.clientId == clientId }) else {
            return false
        }
        queuedSMSAfterWindow.remove(at: idx)
        let newStatus = (lastUnknownSMSClientId == clientId || lateAwaitingCMGSClientId == clientId) ? "unknown" : "failed"
        let detail = (newStatus == "unknown") ? "已取消排队 (仍处于状态未知)" : "已取消排队发送"
        onSMSResult?(clientId, newStatus, detail)
        return true
    }

    public func enqueueSMSForAfterLateWindow(clientId: String, recipient: String, body: String) {
        if let idx = queuedSMSAfterWindow.firstIndex(where: { $0.clientId == clientId }) {
            queuedSMSAfterWindow[idx] = QueuedSMSRetryItem(clientId: clientId, recipient: recipient, body: body)
        } else {
            queuedSMSAfterWindow.append(QueuedSMSRetryItem(clientId: clientId, recipient: recipient, body: body))
        }
        let secs = max(1, remainingLateWindowSeconds)
        onSMSResult?(clientId, "queued_wait", "⏳ 排队等待中 (\(secs)s 后自动发送)")
    }

    @discardableResult
    public func enqueue(_ command: String, priority: ATCommandPriority = .normal, timeout: TimeInterval = 4.0, tag: String = "", batchId: String = "") -> QueuedATCommand {
        let item = QueuedATCommand(id: nextId, command: command, priority: priority, timeout: timeout, tag: tag, batchId: batchId)
        nextId &+= 1
        if let idx = queue.firstIndex(where: { $0.priority < priority }) {
            queue.insert(item, at: idx)
        } else {
            queue.append(item)
        }

        if priority == .high && isSyncingBarrier && initialBarrierProbeWindowElapsed && !barrierSawProbeResponse {
            exitSyncBarrier()
        } else {
            pumpQueue()
        }
        return item
    }

    public func pumpQueue() {
        guard currentCommand == nil, !isSyncingBarrier, !awaitingSMSPrompt, !queue.isEmpty else { return }
        let next = queue.removeFirst()
        currentCommand = next
        if next.tag == "CMGS_CMD" {
            awaitingSMSPrompt = true
        }
        onWriteRaw?(next.command + "\r\n")
        armCommandTimeout(for: next.id, timeout: next.timeout)
    }

    public func armCommandTimeout(for cmdId: UInt64, timeout: TimeInterval) {
        commandTimer?.cancel()
        commandTimer = scheduler.scheduleOnce(delay: timeout) { [weak self] in
            self?.handleTimeout(cmdId: cmdId)
        }
    }

    private func handleTimeout(cmdId: UInt64) {
        guard let timedOut = currentCommand, timedOut.id == cmdId else { return }
        commandTimer?.cancel()
        commandTimer = nil
        lastTimedOutCommandId = timedOut.id
        currentCommand = nil

        if !timedOut.batchId.isEmpty {
            removeBatch(timedOut.batchId)
        }

        if !pendingSMSClientId.isEmpty && timedOut.batchId == pendingSMSClientId {
            if smsBodyDispatched {
                let cid = pendingSMSClientId
                armLateCMGSWindow(for: cid)
                resetActiveSMSFlags()
                onSMSResult?(cid, "unknown", "等待运营商回执超时(12s)，后台监听迟到回执中")
                onCommandTimedOut?(timedOut)
                enterSyncBarrier()
                return
            } else {
                if timedOut.tag == "CMGS_CMD" {
                    onWriteRaw?("\u{1B}\r\n")
                }
                let cid = pendingSMSClientId
                resetActiveSMSFlags()
                onSMSResult?(cid, "failed", "短信指令超时 (\(timedOut.command))")
            }
        }

        onCommandTimedOut?(timedOut)
        enterSyncBarrier()
    }

    private func armLateCMGSWindow(for clientId: String) {
        isLateCMGSChannelWindowOpen = true
        lateAwaitingCMGSClientId = clientId
        lastUnknownSMSClientId = clientId
        clearAwaitingLateCMGSTrailingOK()
        lateCMGSWindowExpiresAt = scheduler.now.addingTimeInterval(60.0)
        lateCMGSWindowTimer?.cancel()
        lateCMGSWindowTimer = scheduler.scheduleOnce(delay: 60.0) { [weak self] in
            guard let self = self else { return }
            self.close60sChannelWindowOnly()
            if self.isSyncingBarrier && !self.barrierSawProbeResponse {
                self.exitSyncBarrier()
            } else {
                self.flushQueuedSMSAfterWindowIfReady()
            }
        }
    }

    private func close60sChannelWindowOnly() {
        lateCMGSWindowTimer?.cancel()
        lateCMGSWindowTimer = nil
        isLateCMGSChannelWindowOpen = false
        lateAwaitingCMGSClientId = nil
        lateCMGSWindowExpiresAt = nil
    }

    private func armAwaitingLateCMGSTrailingOK() {
        awaitingLateCMGSTrailingOK = true
        lateCMGSTrailingOKTimer?.cancel()
        lateCMGSTrailingOKTimer = scheduler.scheduleOnce(delay: 2.0) { [weak self] in
            guard let self = self else { return }
            self.awaitingLateCMGSTrailingOK = false
            self.lateCMGSTrailingOKTimer = nil
            if self.isSyncingBarrier && !self.barrierSawProbeResponse {
                self.exitSyncBarrier()
            } else {
                self.checkDeferredSMSPollIfIdle()
                self.flushQueuedSMSAfterWindowIfReady()
            }
        }
    }

    private func clearAwaitingLateCMGSTrailingOK() {
        awaitingLateCMGSTrailingOK = false
        lateCMGSTrailingOKTimer?.cancel()
        lateCMGSTrailingOKTimer = nil
    }

    private func armAwaitingOrphanProbeTrailingOK() {
        awaitingOrphanProbeTrailingOK = true
        orphanProbeTrailingOKTimer?.cancel()
        orphanProbeTrailingOKTimer = scheduler.scheduleOnce(delay: 2.0) { [weak self] in
            guard let self = self else { return }
            self.awaitingOrphanProbeTrailingOK = false
            self.orphanProbeTrailingOKTimer = nil
        }
    }

    private func clearAwaitingOrphanProbeTrailingOK() {
        awaitingOrphanProbeTrailingOK = false
        orphanProbeTrailingOKTimer?.cancel()
        orphanProbeTrailingOKTimer = nil
    }

    /// Fixes Bug 2: Arms a 30.0-second expiration timer on `unansweredOrphanProbesSent`
    /// so if the modem dropped the barrier probe(s), the orphan counter automatically clears after 30.0s!
    private func armOrphanProbesCountExpiryTimer() {
        orphanProbesCountExpiryTimer?.cancel()
        guard unansweredOrphanProbesSent > 0 else {
            orphanProbesCountExpiryTimer = nil
            return
        }
        orphanProbesCountExpiryTimer = scheduler.scheduleOnce(delay: 30.0) { [weak self] in
            guard let self = self else { return }
            self.unansweredOrphanProbesSent = 0
            self.orphanProbesCountExpiryTimer = nil
        }
    }

    private func resetOrphanProbesCountTracking() {
        unansweredOrphanProbesSent = 0
        orphanProbesCountExpiryTimer?.cancel()
        orphanProbesCountExpiryTimer = nil
        clearAwaitingOrphanProbeTrailingOK()
    }

    public func clearLateCMGSWindow() {
        close60sChannelWindowOnly()
        clearAwaitingLateCMGSTrailingOK()
    }

    private func flushQueuedSMSAfterWindowIfReady() {
        guard !hasPendingLateSMSReceipt, !isSendingSMS, !isSyncingBarrier, isCallIdleProvider(), !queuedSMSAfterWindow.isEmpty else {
            return
        }
        let nextSMS = queuedSMSAfterWindow.removeFirst()
        onDispatchQueuedSMS?(nextSMS)
    }

    public func abortUnsubmittedSMSPromptWithESC(reason: String) -> Bool {
        guard isSendingSMS && !smsBodyDispatched else { return false }
        commandTimer?.cancel()
        commandTimer = nil
        let cid = pendingSMSClientId
        if !cid.isEmpty {
            removeBatch(cid)
        }
        resetActiveSMSFlags()
        onWriteRaw?("\u{1B}\r\n")
        if !cid.isEmpty {
            onSMSResult?(cid, "failed", reason)
        }
        enterSyncBarrier()
        return true
    }

    public func handlePromptReceivedForCMGS() {
        guard currentCommand?.tag == "CMGS_CMD", !smsBodyDispatched, let active = currentCommand else { return }
        awaitingSMSPrompt = false
        smsBodyDispatched = true
        lastUnknownSMSClientId = nil
        armCommandTimeout(for: active.id, timeout: 12.0)
        onWriteRaw?(pendingSMSBodyHex + "\u{1A}")
    }

    public func handleCMGSReceiptLine() {
        let isExpectedByActiveCMGS = (isSendingSMS && currentCommand?.tag == "CMGS_CMD" && smsBodyDispatched)
        guard !isExpectedByActiveCMGS else { return }

        let targetId = lateAwaitingCMGSClientId ?? lastUnknownSMSClientId
        close60sChannelWindowOnly()
        armAwaitingLateCMGSTrailingOK()

        if let cid = targetId {
            lastUnknownSMSClientId = nil
            let hadQueuedRetry = queuedSMSAfterWindow.contains(where: { $0.clientId == cid })
            queuedSMSAfterWindow.removeAll { $0.clientId == cid }
            let detail = hadQueuedRetry ? "已发送 (收到延迟回执，已自动取消排队重试)" : "已发送 (收到延迟回执)"
            onSMSResult?(cid, "sent", detail)
        }
    }

    /// Fixes Bug 1 & Bug 2:
    /// Called when the dedicated barrier probe data line (`+CGMR:` or `EG25...` from `AT+CGMR`) arrives!
    /// - Inside `isSyncingBarrier`: decrements `unansweredBarrierProbeCount` ONCE and sets `barrierCurrentProbeDecrementedOnDataLine = true`
    ///   so the trailing `OK` does NOT decrement `unansweredBarrierProbeCount` a second time!
    /// - Outside `isSyncingBarrier`: if `currentCommand` is not `AT+CGMR` and `unansweredOrphanProbesSent > 0`,
    ///   decrements `unansweredOrphanProbesSent` and arms `awaitingOrphanProbeTrailingOK = true` (2.0s) to absorb its trailing `OK`.
    public func handleBarrierProbeDataLineReceived() {
        if isSyncingBarrier {
            if unansweredBarrierProbeCount > 0 {
                unansweredBarrierProbeCount -= 1
                barrierCurrentProbeDecrementedOnDataLine = true
            }
            markBarrierProbeRespondedAndStartQuietWindow()
            return
        }

        let isExpectedByCurrentCGMR = (currentCommand?.command.uppercased() == Self.barrierProbeCommand)
        if !isExpectedByCurrentCGMR && unansweredOrphanProbesSent > 0 {
            unansweredOrphanProbesSent -= 1
            if unansweredOrphanProbesSent == 0 {
                orphanProbesCountExpiryTimer?.cancel()
                orphanProbesCountExpiryTimer = nil
            }
            armAwaitingOrphanProbeTrailingOK()
        }
    }

    /// Backwards-compatible alias for tests that call `handleCSQLineReceived()` directly on the engine.
    public func handleCSQLineReceived() {
        // Note: In v15.1, +CSQ: is a regular status poll response and does NOT trigger orphan probe absorption!
        // Only `handleBarrierProbeDataLineReceived()` (triggered by +CGMR: / EG25...) identifies barrier probes.
        if isSyncingBarrier {
            notifyIncomingSerialActivity()
        }
    }

    /// Enters the sync barrier, resets any stale `unansweredOrphanProbesSent`, and sends `AT+CGMR\r\n`.
    public func enterSyncBarrier() {
        commandTimer?.cancel()
        commandTimer = nil
        silenceTimer?.cancel()
        silenceTimer = nil
        barrierProbeTimeoutTimer?.cancel()
        barrierProbeTimeoutTimer = nil

        // Fixes Bug 2: Reset orphan probe counter every time we enter a new sync barrier
        resetOrphanProbesCountTracking()

        currentCommand = nil
        isSyncingBarrier = true
        barrierSawProbeResponse = false
        initialBarrierProbeWindowElapsed = false
        barrierCurrentProbeDecrementedOnDataLine = false
        unansweredBarrierProbeCount = 1
        onWriteRaw?(Self.barrierProbeCommand + "\r\n")
        scheduleBarrierProbeCheck()
    }

    private func scheduleBarrierProbeCheck() {
        barrierProbeTimeoutTimer?.cancel()
        barrierProbeTimeoutTimer = scheduler.scheduleOnce(delay: 2.0) { [weak self] in
            guard let self = self, self.isSyncingBarrier, !self.barrierSawProbeResponse else { return }
            self.initialBarrierProbeWindowElapsed = true
            let hasUrgentCallCommand = self.queue.contains(where: { $0.priority == .high })
            if self.hasPendingLateSMSReceipt && !hasUrgentCallCommand {
                if self.unansweredBarrierProbeCount < Self.maxUnansweredBarrierProbes {
                    self.unansweredBarrierProbeCount += 1
                    self.onWriteRaw?(Self.barrierProbeCommand + "\r\n")
                    self.scheduleBarrierProbeCheck()
                }
            } else {
                self.exitSyncBarrier()
            }
        }
    }

    public func notifyIncomingSerialActivity() {
        if isSyncingBarrier && barrierSawProbeResponse {
            resetSilenceTimer()
        }
    }

    private func markBarrierProbeRespondedAndStartQuietWindow() {
        guard isSyncingBarrier else { return }
        barrierSawProbeResponse = true
        barrierProbeTimeoutTimer?.cancel()
        barrierProbeTimeoutTimer = nil
        resetSilenceTimer()
    }

    private func resetSilenceTimer() {
        silenceTimer?.cancel()
        silenceTimer = scheduler.scheduleOnce(delay: 0.85) { [weak self] in
            guard let self = self, self.isSyncingBarrier else { return }
            self.exitSyncBarrier()
        }
    }

    private func exitSyncBarrier() {
        silenceTimer?.cancel()
        silenceTimer = nil
        barrierProbeTimeoutTimer?.cancel()
        barrierProbeTimeoutTimer = nil
        isSyncingBarrier = false
        barrierSawProbeResponse = false
        initialBarrierProbeWindowElapsed = false
        barrierCurrentProbeDecrementedOnDataLine = false
        if unansweredBarrierProbeCount > 0 {
            unansweredOrphanProbesSent = unansweredBarrierProbeCount
            unansweredBarrierProbeCount = 0
            armOrphanProbesCountExpiryTimer()
        }
        lastTimedOutCommandId = nil
        clearAwaitingLateCMGSTrailingOK()
        pumpQueue()
        checkDeferredSMSPollIfIdle()
        flushQueuedSMSAfterWindowIfReady()
    }

    public func handleTerminalResponse(isError: Bool, line: String) {
        if isError && awaitingLateCMGSTrailingOK {
            clearAwaitingLateCMGSTrailingOK()
        }
        if isError && awaitingOrphanProbeTrailingOK {
            clearAwaitingOrphanProbeTrailingOK()
        }

        // 1. Absorb trailing OK of a late +CMGS:
        if !isError && awaitingLateCMGSTrailingOK {
            clearLateCMGSWindow()
            if isSyncingBarrier {
                markBarrierProbeRespondedAndStartQuietWindow()
            } else {
                checkDeferredSMSPollIfIdle()
                flushQueuedSMSAfterWindowIfReady()
            }
            return
        }

        // 2. Absorb trailing OK of a delayed `AT+CGMR` barrier probe that was preceded by an unsolicited `+CGMR:` / `EG25...` line!
        if !isError && awaitingOrphanProbeTrailingOK {
            clearAwaitingOrphanProbeTrailingOK()
            return
        }

        // 3. Attribute +CMS ERROR to the late SMS ONLY when currentCommand is NOT an SMS command
        if isError && line.hasPrefix("+CMS ERROR:") {
            let activeCmdCanProduceCMSError = (currentCommand?.canProduceCMSError == true)
            if !activeCmdCanProduceCMSError && (isLateCMGSChannelWindowOpen || lateAwaitingCMGSClientId != nil || lastUnknownSMSClientId != nil) {
                let lateId = lateAwaitingCMGSClientId ?? lastUnknownSMSClientId
                lastUnknownSMSClientId = nil
                clearLateCMGSWindow()
                if let cid = lateId {
                    onSMSResult?(cid, "failed", "发送失败 (延迟回执: \(line))")
                }
                if isSyncingBarrier {
                    markBarrierProbeRespondedAndStartQuietWindow()
                } else {
                    checkDeferredSMSPollIfIdle()
                    flushQueuedSMSAfterWindowIfReady()
                }
                return
            }
        }

        // 4. Fixes Bug 1: Inside sync barrier, ONLY decrement `unansweredBarrierProbeCount` on `OK` if it was NOT already decremented by the preceding probe data line!
        if isSyncingBarrier {
            if !isError {
                if barrierCurrentProbeDecrementedOnDataLine {
                    barrierCurrentProbeDecrementedOnDataLine = false
                } else if unansweredBarrierProbeCount > 0 {
                    unansweredBarrierProbeCount -= 1
                }
            } else {
                barrierCurrentProbeDecrementedOnDataLine = false
            }
            markBarrierProbeRespondedAndStartQuietWindow()
            return
        }

        // 5. Ignore premature OK before '>' prompt on CMGS_CMD
        if !isError, currentCommand?.tag == "CMGS_CMD", !smsBodyDispatched {
            return
        }

        commandTimer?.cancel()
        commandTimer = nil

        guard let finished = currentCommand else { return }
        currentCommand = nil

        if isSendingSMS && finished.batchId == pendingSMSClientId {
            if isError {
                removeBatch(finished.batchId)
                let cid = pendingSMSClientId
                resetActiveSMSFlags()
                onSMSResult?(cid, "failed", line)
            } else if finished.tag == "CMGS_CMD" && smsBodyDispatched {
                let cid = pendingSMSClientId
                resetActiveSMSFlags()
                onSMSResult?(cid, "sent", "已发送")
            }
        }

        onCommandCompleted?(finished, isError, line)
        pumpQueue()
        checkDeferredSMSPollIfIdle()
        flushQueuedSMSAfterWindowIfReady()
    }

    public func checkDeferredSMSPollIfIdle() {
        guard needsPendingSMSPoll, isCallIdleProvider(), !isSendingSMS, !isSyncingBarrier, currentCommand == nil else {
            return
        }
        needsPendingSMSPoll = false
        onTriggerDeferredSMSPoll?()
    }

    private func resetActiveSMSFlags() {
        isSendingSMS = false
        awaitingSMSPrompt = false
        smsBodyDispatched = false
        pendingSMSClientId = ""
        pendingSMSBodyHex = ""
    }

    public func removeBatch(_ batchId: String) {
        guard !batchId.isEmpty else { return }
        queue.removeAll { $0.batchId == batchId }
    }

    public func removeTag(_ tag: String) {
        guard !tag.isEmpty else { return }
        queue.removeAll { $0.tag == tag }
    }

    public func resetAll() {
        commandTimer?.cancel()
        commandTimer = nil
        silenceTimer?.cancel()
        silenceTimer = nil
        barrierProbeTimeoutTimer?.cancel()
        barrierProbeTimeoutTimer = nil
        resetOrphanProbesCountTracking()
        clearLateCMGSWindow()
        lastUnknownSMSClientId = nil
        queuedSMSAfterWindow.removeAll()
        queue.removeAll()
        currentCommand = nil
        isSyncingBarrier = false
        barrierSawProbeResponse = false
        initialBarrierProbeWindowElapsed = false
        barrierCurrentProbeDecrementedOnDataLine = false
        unansweredBarrierProbeCount = 0
        lastTimedOutCommandId = nil
        resetActiveSMSFlags()
    }
}

// MARK: - 4. Schema-Versioned SMS Store with N-1 (.bak) & N-2 (.bak2) Snapshots, Lock-Safe Fallback & Self-Healing (Fixes Bugs 3, 4, 5, 6)
public struct StoredSMSItem: Codable, Equatable {
    public var id: String
    public var dedupKey: String
    public var peer: String
    public var dir: String // "in" or "out"
    public var body: String
    public var time: String
    public var status: String // "received", "pending", "queued_wait", "sent", "failed", "unknown"
    public var otp: String
    public var timestamp: Double

    public init(id: String, dedupKey: String, peer: String, dir: String, body: String, time: String, status: String, otp: String, timestamp: Double) {
        self.id = id
        self.dedupKey = dedupKey
        self.peer = peer
        self.dir = dir
        self.body = body
        self.time = time
        self.status = status
        self.otp = otp
        self.timestamp = timestamp
    }
}

public struct SMSStoreEnvelope: Codable, Equatable {
    public static let currentSchemaVersion: Int = 1
    public var schemaVersion: Int
    public var messages: [StoredSMSItem]

    public init(schemaVersion: Int = SMSStoreEnvelope.currentSchemaVersion, messages: [StoredSMSItem]) {
        self.schemaVersion = schemaVersion
        self.messages = messages
    }
}

public struct SMSStoreLoadResult {
    public let messages: [StoredSMSItem]
    public let seenKeys: Set<String>
    public let didMigrateOrModify: Bool
    public let loadFailedReadOnlyGuard: Bool
    public let quarantinedFiles: [String]
}

public enum SMSStoreMigrator {
    public static let canonicalFileName = "djphone_sms_store.json"
    public static let backupFileName = "djphone_sms_store.json.bak"
    public static let backup2FileName = "djphone_sms_store.json.bak2"
    public static let sessionFallbackFileName = "djphone_sms_session_fallback.json"
    public static let legacyFileNames: [String] = [
        "djphone_sms_store_v5.json",
        "djphone_sms_store_v6.json",
        "djphone_sms_store_v7.json",
        "djphone_sms_store_v8.json",
        "djphone_sms_store_v9.json",
        "djphone_sms_store_v10.json",
        "djphone_sms_store_v11.json",
        "djphone_sms_store_v12.json"
    ]

    public static var primaryWriteOptions: Data.WritingOptions {
        #if os(iOS)
        return [.atomic, .completeFileProtection]
        #else
        return [.atomic]
        #endif
    }

    /// Fixes Bug 3: Session fallback store uses `.completeFileProtectionUntilFirstUserAuthentication`
    /// so it CAN be created and written even while the iPhone screen is locked!
    public static var fallbackWriteOptions: Data.WritingOptions {
        #if os(iOS)
        return [.atomic, .completeFileProtectionUntilFirstUserAuthentication]
        #else
        return [.atomic]
        #endif
    }

    public static func decodeItems(from data: Data) -> [StoredSMSItem]? {
        if let env = try? JSONDecoder().decode(SMSStoreEnvelope.self, from: data) {
            return env.messages
        }
        if let rawList = try? JSONDecoder().decode([StoredSMSItem].self, from: data) {
            return rawList
        }
        return nil
    }

    public static func encodeEnvelope(messages: [StoredSMSItem]) -> Data? {
        let env = SMSStoreEnvelope(schemaVersion: SMSStoreEnvelope.currentSchemaVersion, messages: messages)
        return try? JSONEncoder().encode(env)
    }

    /// Fixes Bug 5: Maintains TWO backup generations (`djphone_sms_store.json.bak` [N-1] and `djphone_sms_store.json.bak2` [N-2]),
    /// and ONLY rotates `.bak -> .bak2` and `canonical -> .bak` when `prevItems != messages` (so redundant saves never flush out good backups!).
    @discardableResult
    public static func writeStoreAtomicallyWithBackup(messages: [StoredSMSItem], in directory: URL) -> Bool {
        guard let encoded = encodeEnvelope(messages: messages) else { return false }
        let fm = FileManager.default
        let canonicalURL = directory.appendingPathComponent(canonicalFileName)
        let backupURL = directory.appendingPathComponent(backupFileName)
        let backup2URL = directory.appendingPathComponent(backup2FileName)
        do {
            if fm.fileExists(atPath: canonicalURL.path),
               let prevData = try? Data(contentsOf: canonicalURL),
               let prevItems = decodeItems(from: prevData) {
                if prevItems != messages {
                    // Rotate valid N-1 (.bak) -> N-2 (.bak2) before rotating canonical -> .bak
                    if fm.fileExists(atPath: backupURL.path),
                       let bakData = try? Data(contentsOf: backupURL),
                       decodeItems(from: bakData) != nil {
                        try bakData.write(to: backup2URL, options: primaryWriteOptions)
                    }
                    try prevData.write(to: backupURL, options: primaryWriteOptions)
                }
            } else if !fm.fileExists(atPath: backupURL.path) {
                try encoded.write(to: backupURL, options: primaryWriteOptions)
            }
            try encoded.write(to: canonicalURL, options: primaryWriteOptions)
            return true
        } catch {
            return false
        }
    }

    /// Fixes Bug 3: Writes session fallback store using `fallbackWriteOptions` (`.completeFileProtectionUntilFirstUserAuthentication`),
    /// which succeeds even when the device is locked!
    @discardableResult
    public static func writeSessionFallbackStore(messages: [StoredSMSItem], in directory: URL) -> Bool {
        guard let encoded = encodeEnvelope(messages: messages) else { return false }
        let fallbackURL = directory.appendingPathComponent(sessionFallbackFileName)
        do {
            try encoded.write(to: fallbackURL, options: fallbackWriteOptions)
            return true
        } catch {
            return false
        }
    }

    /// Fixes Bug 4, Bug 5, Bug 6:
    /// - Recovers from `.bak` (N-1) or `.bak2` (N-2) if `djphone_sms_store.json` is corrupted.
    /// - If `djphone_sms_store.json` and all backups fail to read/decode:
    ///   * When `isProtectedDataAvailable == false` (device is locked), enters temporary `loadFailedReadOnlyGuard = true`
    ///     (returning deduplicated, `maxCount`-capped fallback messages) and waits for unlock.
    ///   * When `isProtectedDataAvailable == true` (device is ALREADY unlocked, meaning the files on disk are genuinely corrupted!),
    ///     quarantines `djphone_sms_store.json` (and `.bak`/`.bak2`) to `.corrupt`, rebuilds `djphone_sms_store.json`
    ///     from available fallback/legacy sources, and returns `loadFailedReadOnlyGuard = false` so the app self-heals!
    public static func loadAndMigrateIfNeeded(
        in directory: URL,
        maxCount: Int = 500,
        isProtectedDataAvailable: Bool = true
    ) -> SMSStoreLoadResult {
        let fm = FileManager.default
        let canonicalURL = directory.appendingPathComponent(canonicalFileName)
        let backupURL = directory.appendingPathComponent(backupFileName)
        let backup2URL = directory.appendingPathComponent(backup2FileName)
        let fallbackURL = directory.appendingPathComponent(sessionFallbackFileName)

        var allSources: [(url: URL, isRemovableAfterMerge: Bool, items: [StoredSMSItem])] = []
        var quarantined: [String] = []

        // 1. Scan legacy vN files
        for legName in legacyFileNames {
            let legURL = directory.appendingPathComponent(legName)
            if fm.fileExists(atPath: legURL.path) {
                if let data = try? Data(contentsOf: legURL), let decoded = decodeItems(from: data) {
                    allSources.append((legURL, true, decoded))
                } else if isProtectedDataAvailable {
                    let corruptURL = directory.appendingPathComponent(legName + ".corrupt")
                    try? fm.removeItem(at: corruptURL)
                    try? fm.moveItem(at: legURL, to: corruptURL)
                    quarantined.append(legName)
                }
            }
        }

        // 2. Check canonical store, .bak (N-1), and .bak2 (N-2)
        var canonicalAndBackupsAllFailed = false
        var canonicalWasRawArray = false
        var recoveredFromBackup = false

        if fm.fileExists(atPath: canonicalURL.path) {
            if let data = try? Data(contentsOf: canonicalURL), let decoded = decodeItems(from: data) {
                if (try? JSONDecoder().decode(SMSStoreEnvelope.self, from: data)) == nil {
                    canonicalWasRawArray = true
                }
                allSources.append((canonicalURL, false, decoded))
            } else if let bakData = try? Data(contentsOf: backupURL), let bakItems = decodeItems(from: bakData) {
                recoveredFromBackup = true
                allSources.append((backupURL, false, bakItems))
            } else if let bak2Data = try? Data(contentsOf: backup2URL), let bak2Items = decodeItems(from: bak2Data) {
                recoveredFromBackup = true
                allSources.append((backup2URL, false, bak2Items))
            } else {
                canonicalAndBackupsAllFailed = true
            }
        } else if fm.fileExists(atPath: backupURL.path),
                  let bakData = try? Data(contentsOf: backupURL),
                  let bakItems = decodeItems(from: bakData) {
            recoveredFromBackup = true
            allSources.append((backupURL, false, bakItems))
        } else if fm.fileExists(atPath: backup2URL.path),
                  let bak2Data = try? Data(contentsOf: backup2URL),
                  let bak2Items = decodeItems(from: bak2Data) {
            recoveredFromBackup = true
            allSources.append((backup2URL, false, bak2Items))
        }

        // 3. Check session fallback file
        let shouldRemoveFallbackAfterMerge = !canonicalAndBackupsAllFailed || isProtectedDataAvailable
        if fm.fileExists(atPath: fallbackURL.path),
           let fbData = try? Data(contentsOf: fallbackURL),
           let fbItems = decodeItems(from: fbData) {
            allSources.append((fallbackURL, shouldRemoveFallbackAfterMerge, fbItems))
        }

        // 4. Fixes Bug 6: Always deduplicate, sort, and cap at `maxCount` BEFORE any return path!
        var mergedById: [String: StoredSMSItem] = [:]
        var dedupKeyToId: [String: String] = [:]
        var modified = canonicalWasRawArray || recoveredFromBackup || allSources.contains(where: { $0.isRemovableAfterMerge })

        for source in allSources {
            for var item in source.items {
                if item.status == "pending" || item.status == "queued_wait" {
                    item.status = "unknown"
                    item.time = "状态未知 (上次发送中断)"
                    modified = true
                }
                let effectiveKey = item.dedupKey.isEmpty ? item.id : item.dedupKey
                if let existingId = dedupKeyToId[effectiveKey], let existing = mergedById[existingId] {
                    if item.timestamp >= existing.timestamp {
                        mergedById.removeValue(forKey: existingId)
                        mergedById[item.id] = item
                        dedupKeyToId[effectiveKey] = item.id
                    }
                } else if let existing = mergedById[item.id] {
                    if item.timestamp >= existing.timestamp {
                        mergedById[item.id] = item
                        dedupKeyToId[effectiveKey] = item.id
                    }
                } else {
                    mergedById[item.id] = item
                    dedupKeyToId[effectiveKey] = item.id
                }
            }
        }

        var mergedList = Array(mergedById.values).sorted { $0.timestamp < $1.timestamp }
        if mergedList.count > maxCount {
            mergedList = Array(mergedList.suffix(maxCount))
            modified = true
        }

        var seenKeys = Set<String>()
        for item in mergedList {
            if !item.dedupKey.isEmpty {
                seenKeys.insert(item.dedupKey)
            }
        }

        // 5. Fixes Bug 4: Distinguish locked device vs. unlocked genuine file corruption!
        if canonicalAndBackupsAllFailed {
            if !isProtectedDataAvailable {
                // Device is currently locked: do not touch main store, enter temporary read-only guard until unlock
                return SMSStoreLoadResult(
                    messages: mergedList,
                    seenKeys: seenKeys,
                    didMigrateOrModify: false,
                    loadFailedReadOnlyGuard: true,
                    quarantinedFiles: quarantined
                )
            } else {
                // Device is ALREADY unlocked, so canonical and backups are genuinely corrupted!
                // Quarantine them to `.corrupt` and rebuild the main store from `mergedList` so we self-heal!
                for fileName in [canonicalFileName, backupFileName, backup2FileName] {
                    let fileURL = directory.appendingPathComponent(fileName)
                    if fm.fileExists(atPath: fileURL.path) {
                        let corruptURL = directory.appendingPathComponent(fileName + ".corrupt")
                        try? fm.removeItem(at: corruptURL)
                        try? fm.moveItem(at: fileURL, to: corruptURL)
                        quarantined.append(fileName)
                    }
                }
                modified = true
            }
        }

        if modified {
            if writeStoreAtomicallyWithBackup(messages: mergedList, in: directory) {
                for source in allSources where source.isRemovableAfterMerge {
                    try? fm.removeItem(at: source.url)
                }
            }
        }

        return SMSStoreLoadResult(
            messages: mergedList,
            seenKeys: seenKeys,
            didMigrateOrModify: modified,
            loadFailedReadOnlyGuard: false,
            quarantinedFiles: quarantined
        )
    }
}

// MARK: - 5. Pure Table-Driven AT Line Parser & Stateful Multi-Line +CMGL Stream Decoder
public enum ATParsedEvent: Equatable {
    case ok
    case error(line: String)
    case cgmr(revision: String)
    case csq(rssi: Int, bars: Int)
    case cpin(ready: Bool, state: String)
    case cops(operatorName: String)
    case cereg(stat: Int, description: String, isRegistered: Bool)
    case imsConfig(volteEnabled: Bool, imsRegistered: Bool)
    case csca(number: String)
    case cmti
    case cmglHeader(index: String, isIncoming: Bool, sender: String, time: String)
    case cmglMessage(index: String, isIncoming: Bool, sender: String, time: String, rawBody: String)
    case cmgsReceipt
    case ring
    case clip(caller: String)
    case clcc(dir: Int, stat: Int, number: String)
    case callEnded(reason: String?)
    case other(line: String)
}

public enum ATLineParser {
    public static func parse(_ rawLine: String, fallbackTime: String = "00:00") -> ATParsedEvent {
        let line = rawLine.trimmingCharacters(in: .whitespacesAndNewlines)
        if line == "OK" {
            return .ok
        }
        if line == "ERROR" || line.hasPrefix("+CME ERROR:") || line.hasPrefix("+CMS ERROR:") {
            return .error(line: line)
        }
        if line.hasPrefix("+CGMR:") {
            let rev = line.replacingOccurrences(of: "+CGMR:", with: "").trimmingCharacters(in: .whitespaces)
            return .cgmr(revision: rev)
        }
        if line.uppercased().hasPrefix("EG25") || line.uppercased().hasPrefix("EC20") {
            return .cgmr(revision: line)
        }
        if line.hasPrefix("+CMGS:") {
            return .cmgsReceipt
        }
        if line.hasPrefix("+CSQ:") {
            let parts = line.replacingOccurrences(of: "+CSQ:", with: "").trimmingCharacters(in: .whitespaces).components(separatedBy: ",")
            if let rssi = Int(parts.first ?? "0") {
                let cleanRSSI = (rssi == 99) ? 0 : rssi
                let bars: Int
                if rssi == 99 || rssi <= 0 { bars = 0 }
                else if rssi < 10 { bars = 1 }
                else if rssi < 15 { bars = 2 }
                else if rssi < 20 { bars = 3 }
                else if rssi < 25 { bars = 4 }
                else { bars = 5 }
                return .csq(rssi: cleanRSSI, bars: bars)
            }
        }
        if line.hasPrefix("+CPIN:") {
            let st = line.replacingOccurrences(of: "+CPIN:", with: "").trimmingCharacters(in: .whitespaces)
            return .cpin(ready: st == "READY", state: (st == "READY") ? "已就绪" : st)
        }
        if line.hasPrefix("+COPS:") {
            let comps = line.components(separatedBy: "\"")
            if comps.count >= 2 {
                return .cops(operatorName: ATCodecUtils.mapPLMNToOperator(comps[1]))
            } else {
                return .cops(operatorName: "未注册运营商")
            }
        }
        if line.hasPrefix("+CEREG:") || line.hasPrefix("+CREG:") {
            let parts = line.components(separatedBy: ",")
            if parts.count >= 2, let stat = Int(parts[1].trimmingCharacters(in: .whitespaces)) {
                switch stat {
                case 1: return .cereg(stat: 1, description: "已注册 (本地网)", isRegistered: true)
                case 5: return .cereg(stat: 5, description: "已注册 (漫游)", isRegistered: true)
                case 2: return .cereg(stat: 2, description: "正在搜网...", isRegistered: false)
                case 3: return .cereg(stat: 3, description: "注册被拒绝", isRegistered: false)
                default: return .cereg(stat: stat, description: "未注册 (\(stat))", isRegistered: false)
                }
            }
        }
        if line.hasPrefix("+QCFG: \"ims\"") {
            let parts = line.components(separatedBy: ",")
            if parts.count >= 2 {
                let mode = Int(parts[1].trimmingCharacters(in: .whitespaces)) ?? 0
                let volte = (mode == 1 || mode == 2)
                var imsReg = false
                if parts.count >= 3 {
                    let stat = Int(parts[2].trimmingCharacters(in: .whitespaces)) ?? 0
                    imsReg = (stat == 1)
                }
                return .imsConfig(volteEnabled: volte, imsRegistered: imsReg)
            }
        }
        if line.hasPrefix("+CSCA:") {
            let comps = line.components(separatedBy: "\"")
            if comps.count >= 2 {
                return .csca(number: ATCodecUtils.decodeUCS2OrPlain(comps[1]))
            }
        }
        if line.hasPrefix("+CMTI:") {
            return .cmti
        }
        if line.hasPrefix("+CMGL:") {
            let comps = line.components(separatedBy: ",")
            let idxStr = comps.first?.replacingOccurrences(of: "+CMGL:", with: "").trimmingCharacters(in: .whitespaces) ?? "0"
            let quotes = line.components(separatedBy: "\"")
            if quotes.count >= 4 {
                let statStr = quotes[1].uppercased()
                let isIncoming = (statStr == "REC UNREAD" || statStr == "REC READ" || statStr == "0" || statStr == "1")
                let sender = quotes[3]
                let time = (quotes.count >= 6) ? quotes[5] : fallbackTime
                return .cmglHeader(index: idxStr, isIncoming: isIncoming, sender: sender, time: time)
            }
        }
        if line == "RING" || line.hasPrefix("+CRING:") {
            return .ring
        }
        if line.hasPrefix("+CLIP:") {
            let quotes = line.components(separatedBy: "\"")
            if quotes.count >= 2 {
                let caller = quotes[1].isEmpty ? "未知来电" : quotes[1]
                return .clip(caller: caller)
            }
        }
        if line.hasPrefix("+CLCC:") {
            let body = line.replacingOccurrences(of: "+CLCC:", with: "").trimmingCharacters(in: .whitespaces)
            let parts = body.components(separatedBy: ",")
            if parts.count >= 6, let dir = Int(parts[1]), let stat = Int(parts[2]) {
                let quotes = line.components(separatedBy: "\"")
                let num = (quotes.count >= 2) ? quotes[1] : ""
                return .clcc(dir: dir, stat: stat, number: num)
            }
        }
        if line == "BUSY" {
            return .callEnded(reason: "对方忙线")
        }
        if line == "NO ANSWER" || line == "NO DIALTONE" {
            return .callEnded(reason: "无应答")
        }
        if line == "NO CARRIER" {
            return .callEnded(reason: nil)
        }
        return .other(line: line)
    }

    public static func isBoundaryEvent(_ event: ATParsedEvent) -> Bool {
        switch event {
        case .other:
            return false
        default:
            return true
        }
    }
}

public final class ATStreamDecoder {
    private struct PendingCMGL {
        let index: String
        let isIncoming: Bool
        let sender: String
        let time: String
        var bodyLines: [String] = []
    }

    private var pendingCMGL: PendingCMGL? = nil

    public init() {}

    public func reset() {
        pendingCMGL = nil
    }

    public func feedLine(_ rawLine: String, fallbackTime: String = "00:00") -> [ATParsedEvent] {
        let trimmed = rawLine.trimmingCharacters(in: .whitespacesAndNewlines)

        if pendingCMGL == nil && trimmed.isEmpty {
            return []
        }

        let candidate = ATLineParser.parse(trimmed, fallbackTime: fallbackTime)

        if var current = pendingCMGL {
            if trimmed.isEmpty {
                if current.bodyLines.isEmpty {
                    current.bodyLines.append("")
                    pendingCMGL = current
                }
                return []
            }

            if ATLineParser.isBoundaryEvent(candidate) {
                let rawBody = current.bodyLines.joined(separator: "\n")
                let completedMsg = ATParsedEvent.cmglMessage(
                    index: current.index,
                    isIncoming: current.isIncoming,
                    sender: current.sender,
                    time: current.time,
                    rawBody: rawBody
                )
                pendingCMGL = nil

                if case let .cmglHeader(idx, isIn, snd, tm) = candidate {
                    pendingCMGL = PendingCMGL(index: idx, isIncoming: isIn, sender: snd, time: tm)
                    return [completedMsg]
                }
                return [completedMsg, candidate]
            } else {
                if current.bodyLines == [""] {
                    current.bodyLines = [trimmed]
                } else {
                    current.bodyLines.append(trimmed)
                }
                pendingCMGL = current
                return []
            }
        } else {
            if case let .cmglHeader(idx, isIn, snd, tm) = candidate {
                pendingCMGL = PendingCMGL(index: idx, isIncoming: isIn, sender: snd, time: tm)
                return []
            }
            return [candidate]
        }
    }
}

// MARK: - 6. Testable Driver-Level Coordinator for CallKit, Notifications & Result-Driven CLCC Watchdog
public enum CallKitEndReasonType: String, Equatable {
    case failed = "failed"
    case unanswered = "unanswered"
    case remoteEnded = "remoteEnded"
}

public struct LocalHangupOutcome: Equatable {
    public let didBeginHangup: Bool
    public let endedCallKitUUID: UUID?
    public init(didBeginHangup: Bool, endedCallKitUUID: UUID?) {
        self.didBeginHangup = didBeginHangup
        self.endedCallKitUUID = endedCallKitUUID
    }
}

public protocol DriverSystemBridgeProtocol: AnyObject {
    func reportNewIncomingCall(uuid: UUID, caller: String, completion: @escaping (Error?) -> Void)
    func updateCallCaller(uuid: UUID, caller: String)
    func reportCallEnded(uuid: UUID, reason: CallKitEndReasonType)
    func postOrUpdateNotification(id: String, title: String, body: String)
}

public final class CallSessionCoordinator {
    public let callSM: ATCallStateMachine
    public let queueCore: ATCommandQueueCore
    private let scheduler: ATScheduler
    private weak var bridge: DriverSystemBridgeProtocol?

    public private(set) var activeCallUUID: UUID? = nil
    public private(set) var callKitRejectedCurrentCall: Bool = false
    public private(set) var currentCallNotificationId: String = ""
    public private(set) var lastNotifiedCaller: String = ""
    public private(set) var lastReportedCallKitCaller: String = ""
    public private(set) var watchdogVerifySawAnyCallLine: Bool = false
    public var showNotificationPreview: Bool = false

    public var onWatchdogHangupTriggered: ((CallLifecycleState, String) -> Void)?

    private var ringClipWaitTimer: ATCancellableTimer? = nil
    private var callSessionSeq: UInt64 = 0

    public init(callSM: ATCallStateMachine, queueCore: ATCommandQueueCore, scheduler: ATScheduler, bridge: DriverSystemBridgeProtocol?) {
        self.callSM = callSM
        self.queueCore = queueCore
        self.scheduler = scheduler
        self.bridge = bridge

        self.callSM.onRequestWatchdogCLCCVerification = { [weak self] _, _ in
            guard let self = self else { return }
            self.watchdogVerifySawAnyCallLine = false
            self.queueCore.removeTag("CLCC_WATCHDOG_VERIFY")
            _ = self.queueCore.enqueue("AT+CLCC", priority: .high, timeout: 4.0, tag: "CLCC_WATCHDOG_VERIFY")
        }

        self.callSM.onTransientStateTimedOut = { [weak self] timedOutState, number in
            self?.handleTransientWatchdogExpired(timedOutState: timedOutState, number: number)
        }
    }

    @discardableResult
    public func handleCLCCLine(dir: Int, stat: Int, number: String) -> (didTransitionToConnected: Bool, isOutgoing: Bool) {
        guard callSM.state != .hangingUp else { return (false, dir == 0) }
        let resolvedNum = !number.isEmpty ? number : (callSM.activeNumber.isEmpty ? "未知号码" : callSM.activeNumber)
        let outgoingFlag = (dir == 0)
        watchdogVerifySawAnyCallLine = true

        if stat == 0 {
            let changed = callSM.transition(to: .connected, number: resolvedNum, outgoing: outgoingFlag)
            return (changed, outgoingFlag)
        } else if callSM.state != .connecting {
            switch stat {
            case 2:
                _ = callSM.transition(to: .dialing, number: resolvedNum, outgoing: outgoingFlag)
            case 3:
                _ = callSM.transition(to: .alerting, number: resolvedNum, outgoing: outgoingFlag)
            case 4, 5:
                _ = callSM.transition(to: .incoming, number: resolvedNum, outgoing: false)
                reportOrUpdateIncomingCall(caller: resolvedNum)
            default:
                break
            }
        }
        return (false, outgoingFlag)
    }

    public func handleWatchdogCLCCVerifyCommandCompleted(isError: Bool) {
        guard callSM.isAwaitingWatchdogCLCCVerification else { return }
        if isError {
            callSM.handleWatchdogVerificationInconclusiveOrRetry()
            return
        }
        if !watchdogVerifySawAnyCallLine && callSM.state != .connected {
            callSM.confirmWatchdogCLCCVerificationFailed()
        } else if watchdogVerifySawAnyCallLine && callSM.state != .connected {
            callSM.handleWatchdogVerificationInconclusiveOrRetry()
        }
    }

    public func resetSessionFlags() {
        ringClipWaitTimer?.cancel()
        ringClipWaitTimer = nil
        activeCallUUID = nil
        callKitRejectedCurrentCall = false
        currentCallNotificationId = ""
        lastNotifiedCaller = ""
        lastReportedCallKitCaller = ""
        watchdogVerifySawAnyCallLine = false
    }

    public func startOutgoingDial(number: String) -> UUID {
        resetSessionFlags()
        let uuid = UUID()
        activeCallUUID = uuid
        _ = callSM.transition(to: .dialing, number: number, outgoing: true)
        return uuid
    }

    public func handleLocalHangup() -> LocalHangupOutcome {
        let oldUUID = activeCallUUID
        guard callSM.beginHangup() != nil else {
            return LocalHangupOutcome(didBeginHangup: false, endedCallKitUUID: nil)
        }
        resetSessionFlags()
        return LocalHangupOutcome(didBeginHangup: true, endedCallKitUUID: oldUUID)
    }

    public func handleTransientWatchdogExpired(timedOutState: CallLifecycleState, number: String) {
        let oldUUID = activeCallUUID
        guard callSM.beginHangup() != nil else { return }
        resetSessionFlags()

        if let uuid = oldUUID {
            bridge?.reportCallEnded(uuid: uuid, reason: .unanswered)
        }

        queueCore.removeTag("CLCC")
        queueCore.removeTag("CLCC_WATCHDOG_VERIFY")
        _ = queueCore.abortUnsubmittedSMSPromptWithESC(reason: "通话超时挂断中断")
        _ = queueCore.enqueue("ATH", priority: .high, tag: "ATH_DONE")
        _ = queueCore.enqueue("AT+CHUP", priority: .high)
        _ = queueCore.enqueue("AT+QPCMV=0", priority: .high, tag: "QPCMV_OFF")

        onWatchdogHangupTriggered?(timedOutState, number)
    }

    public func handleRingArrived() {
        guard callSM.state.canAcceptIncomingRing else { return }
        guard !callSM.isIncomingRingSuppressed(for: "未知来电") else { return }

        resetSessionFlags()
        callSessionSeq &+= 1
        currentCallNotificationId = "djphone_call_session_\(callSessionSeq)"
        _ = callSM.transition(to: .incoming, number: "未知来电", outgoing: false)

        ringClipWaitTimer?.cancel()
        ringClipWaitTimer = scheduler.scheduleOnce(delay: 0.6) { [weak self] in
            guard let self = self, self.callSM.state == .incoming else { return }
            let caller = self.callSM.activeNumber.isEmpty ? "未知来电" : self.callSM.activeNumber
            self.reportOrUpdateIncomingCall(caller: caller)
        }
    }

    public func handleClipArrived(caller: String) {
        guard callSM.state != .hangingUp else { return }
        if callSM.state == .idle && callSM.isIncomingRingSuppressed(for: caller) {
            return
        }
        ringClipWaitTimer?.cancel()
        ringClipWaitTimer = nil

        if currentCallNotificationId.isEmpty {
            callSessionSeq &+= 1
            currentCallNotificationId = "djphone_call_session_\(callSessionSeq)"
        }

        if callSM.state == .idle || callSM.state == .incoming {
            _ = callSM.transition(to: .incoming, number: caller, outgoing: false)
        }
        if !callSM.isOutgoing {
            reportOrUpdateIncomingCall(caller: caller)
        }
    }

    public func reportOrUpdateIncomingCall(caller: String) {
        if currentCallNotificationId.isEmpty {
            callSessionSeq &+= 1
            currentCallNotificationId = "djphone_call_session_\(callSessionSeq)"
        }

        if caller != lastNotifiedCaller {
            lastNotifiedCaller = caller
            let title = showNotificationPreview ? "大疆4G模块来电 (\(caller))" : "DG电话 - 收到新来电"
            let body = showNotificationPreview ? "来电号码: \(caller)" : "请打开 DG电话 查看详情"
            bridge?.postOrUpdateNotification(id: currentCallNotificationId, title: title, body: body)
        }

        guard !callKitRejectedCurrentCall else { return }

        if let existingUUID = activeCallUUID {
            guard caller != lastReportedCallKitCaller else { return }
            lastReportedCallKitCaller = caller
            bridge?.updateCallCaller(uuid: existingUUID, caller: caller)
        } else {
            let uuid = UUID()
            activeCallUUID = uuid
            lastReportedCallKitCaller = caller
            bridge?.reportNewIncomingCall(uuid: uuid, caller: caller) { [weak self] error in
                guard let self = self else { return }
                if error != nil {
                    self.callKitRejectedCurrentCall = true
                    self.activeCallUUID = nil
                }
            }
        }
    }

    public func handleCallEnded(isDialFailure: Bool) -> CallKitEndReasonType {
        let wasIncomingUnanswered = (!callSM.isOutgoing && callSM.state == .incoming)
        let wasOutgoingUnconnected = (callSM.isOutgoing && callSM.state != .connected)

        let reason: CallKitEndReasonType
        if isDialFailure {
            reason = .failed
        } else if wasIncomingUnanswered || wasOutgoingUnconnected {
            reason = .unanswered
        } else {
            reason = .remoteEnded
        }

        if let uuid = activeCallUUID {
            activeCallUUID = nil
            bridge?.reportCallEnded(uuid: uuid, reason: reason)
        }
        callSM.completeHangupToIdle()
        resetSessionFlags()
        return reason
    }
}

// MARK: - 7. Codec & Sanitization Utilities
public enum ATCodecUtils {
    public static func sanitizePhoneNumber(_ raw: String) -> String {
        let allowed = CharacterSet(charactersIn: "0123456789+*#")
        return String(raw.unicodeScalars.filter { allowed.contains($0) })
    }

    public static func sanitizeDTMF(_ raw: String) -> String {
        let allowed = CharacterSet(charactersIn: "0123456789*#ABCDabcd")
        guard let first = raw.unicodeScalars.first, allowed.contains(first) else { return "" }
        return String(first).uppercased()
    }

    public static func sanitizeAPN(_ raw: String) -> String {
        let allowed = CharacterSet(charactersIn: "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_")
        return String(raw.unicodeScalars.filter { allowed.contains($0) })
    }

    public static func isValidUCS2Length(_ text: String) -> Bool {
        let count = text.utf16.count
        return count > 0 && count <= 70
    }

    public static func encodeToUCS2Hex(_ text: String) -> String {
        var hex = ""
        for codeUnit in text.utf16 {
            hex += String(format: "%04X", codeUnit)
        }
        return hex
    }

    public static func decodeUCS2OrPlain(_ input: String) -> String {
        let trimmed = input.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.count >= 4 && trimmed.count % 4 == 0 && trimmed.allSatisfy({ $0.isHexDigit }) {
            var utf16Units: [UInt16] = []
            var idx = trimmed.startIndex
            while idx < trimmed.endIndex {
                let nextIdx = trimmed.index(idx, offsetBy: 4)
                if let val = UInt16(trimmed[idx..<nextIdx], radix: 16) {
                    utf16Units.append(val)
                }
                idx = nextIdx
            }
            return String(utf16CodeUnits: utf16Units, count: utf16Units.count)
        }
        return trimmed
    }

    public static func extractOTPCode(from text: String) -> String? {
        let keywords = ["验证码", "校验码", "动态码", "确认码", "随机码", "code", "OTP", "Code", "verification"]
        guard keywords.contains(where: { text.contains($0) }) else { return nil }
        let pattern = "(?<!\\d)(\\d{4,8})(?!\\d)"
        if let regex = try? NSRegularExpression(pattern: pattern),
           let match = regex.firstMatch(in: text, range: NSRange(text.startIndex..., in: text)),
           let range = Range(match.range(at: 1), in: text) {
            return String(text[range])
        }
        return nil
    }

    public static func mapPLMNToOperator(_ plmn: String) -> String {
        let clean = plmn.trimmingCharacters(in: .whitespacesAndNewlines)
        switch clean {
        case "46000", "46002", "46004", "46007", "46008":
            return "中国移动"
        case "46001", "46006", "46009":
            return "中国联通"
        case "46003", "46005", "46011":
            return "中国电信"
        case "46015":
            return "中国广电"
        default:
            let up = clean.uppercased()
            if up.contains("TELECOM") || up.contains("CHN-CT") { return "中国电信" }
            if up.contains("UNICOM") || up.contains("CHN-CU") { return "中国联通" }
            if up.contains("MOBILE") || up.contains("CMCC") { return "中国移动" }
            return decodeUCS2OrPlain(clean)
        }
    }
}
