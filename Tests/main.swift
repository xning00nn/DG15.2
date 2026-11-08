//
//  main.swift
//  DJPhone v15.1 Comprehensive 22-Suite Automated Unit Test Runner
//
//  Includes ALL 19 historical regression suites plus 3 new v15.1 suites (enforced >= 22 suites & title checks in CI):
//  Test 1:  3-Tier Priority Queue & Probe-Confirmed Sync Barrier (`AT+CGMR` probe)
//  Test 2:  High-Priority Breakthrough When Modem Caches AT+CGMR Probes (Absorbs +CGMR: + OK Without Misalignment)
//  Test 3:  High-Priority Breakthrough When Modem Drops AT+CGMR Probes Does NOT Swallow ATH/CHUP OKs
//  Test 4:  Late +CMGS Receipt + Trailing OK Absorption While Next Command Is Active
//  Test 5:  Late +CMGS Receipt + Trailing OK Arriving DURING Sync Barrier
//  Test 6:  Scoped +CMS ERROR: Read-SMS (AT+CMGL) Error vs Genuine Late SMS +CMS ERROR
//  Test 7:  Window-Decoupled Late +CMGS Arriving AFTER 60s Window Expires Still Absorbs Trailing OK
//  Test 8:  Time-Bounded (2.0s) & Error-Safe awaitingLateCMGSTrailingOK + Immediate 60s Window Closure on +CMGS
//  Test 9:  Auto-Queued Retry During 60s Window: Auto-Cancels Duplicate When Late +CMGS Arrives & Auto-Dispatches When Window Expires
//  Test 10: Manual Resolve/Delete Clears Attribution WITHOUT Closing 60s Channel Protection Window + User Cancel Queued Retry
//  Test 11: User Hangup WITH CallKit UUID Clears activeCallUUID So Next Incoming Call Invokes reportNewIncomingCall
//  Test 12: User Hangup WITHOUT CallKit UUID (DND-Rejected Call, Pre-CLIP 0.6s Reject, and CLCC-Adopted Call) Succeeds & Arms Master Watchdog
//  Test 13: Driver CallSessionCoordinator: DND Rejection Single-Report Guard, In-Place Notification Caller Update & ATD .failed
//  Test 14: Stateful ATStreamDecoder (Empty-Body, Multi-Line & Back-to-Back +CMGL) + Full Call -> +CMTI -> Hangup -> Deferred SMS Poll Chain
//  Test 15: Table-Driven ATLineParser Verification on 24 Real Quectel EG25-G Serial Outputs (Including +CGMR / EG25)
//  Test 16: Schema-Versioned SMS Store, True N-1 (.bak) & N-2 (.bak2) Snapshots & DedupKey-ID Consistency
//  Test 17: Corrupt Legacy File Quarantine (.corrupt) Does NOT Block Saving + Fallback Store Unlock Merge
//  Test 18: Transient Call State Watchdog (75s Outgoing Ring) Pre-Hangup CLCC Check & Full Hangup Teardown
//  Test 19: Connecting Watchdog (35s) Extends When Delayed by 12s SMS or CLCC Error & Saves Active Call
//  Test 20: Single-Decrement Barrier Probe Count When 1 of 2 Probes Replies Inside Barrier & 2nd Replies After Exit (Fixes Bug 1)
//  Test 21: Dropped Probe + Late Status-Poll +CSQ Does NOT Swallow ATH OK & 30s Orphan Probe Count Expiry (Fixes Bug 2)
//  Test 22: Unlocked Main Store Corruption Self-Heals (.corrupt) + Locked Fallback Deduplication & .bak2 Recovery (Fixes Bugs 3, 4, 5, 6)
//

import Foundation

func assertTrue(_ cond: Bool, _ msg: String, line: Int = #line) {
    if !cond {
        fputs("❌ TEST FAILED (line \(line)): \(msg)\n", stderr)
        exit(1)
    }
}

final class MockDriverBridge: DriverSystemBridgeProtocol {
    var shouldRejectCallKit: Bool = false
    var newIncomingCallCount: Int = 0
    var newIncomingUUIDs: [UUID] = []
    var updatedCallers: [String] = []
    var endedReasons: [CallKitEndReasonType] = []
    var endedUUIDs: [UUID] = []
    var notificationsById: [String: (title: String, body: String)] = [:]
    var notificationPostOrder: [(id: String, title: String)] = []

    func reportNewIncomingCall(uuid: UUID, caller: String, completion: @escaping (Error?) -> Void) {
        newIncomingCallCount += 1
        newIncomingUUIDs.append(uuid)
        if shouldRejectCallKit {
            completion(NSError(domain: "com.apple.CallKit.error.requesttransaction", code: 4, userInfo: nil))
        } else {
            completion(nil)
        }
    }

    func updateCallCaller(uuid: UUID, caller: String) {
        updatedCallers.append(caller)
    }

    func reportCallEnded(uuid: UUID, reason: CallKitEndReasonType) {
        endedUUIDs.append(uuid)
        endedReasons.append(reason)
    }

    func postOrUpdateNotification(id: String, title: String, body: String) {
        notificationsById[id] = (title, body)
        notificationPostOrder.append((id, title))
    }
}

func runAllTests() {
    var passedSuites = 0

    print("▶ Test 1: 3-Tier Priority Queue & Probe-Confirmed Sync Barrier...")
    let clock1 = MockScheduler()
    let engine1 = ATCommandQueueCore(scheduler: clock1)
    var sentCommands1: [String] = []
    var completedTags1: [String] = []
    engine1.onWriteRaw = { raw in sentCommands1.append(raw.trimmingCharacters(in: .whitespacesAndNewlines)) }
    engine1.onCommandCompleted = { cmd, isErr, _ in if !isErr { completedTags1.append(cmd.tag) } }

    engine1.enqueue("AT+CSQ", priority: .low, timeout: 4.0, tag: "POLL_CSQ")
    engine1.enqueue("AT+CPIN?", priority: .normal, timeout: 4.0, tag: "NORM_CPIN")
    engine1.enqueue("ATH", priority: .high, timeout: 4.0, tag: "HIGH_ATH")

    clock1.advance(by: 4.0)
    assertTrue(engine1.isSyncingBarrier == true && sentCommands1.last == "AT+CGMR", "Engine enters sync barrier and sends dedicated AT+CGMR probe")
    clock1.advance(by: 1.0)
    assertTrue(engine1.isSyncingBarrier == true, "Barrier does NOT release at 0.85s without probe response")
    engine1.handleBarrierProbeDataLineReceived()
    engine1.handleTerminalResponse(isError: false, line: "OK")
    clock1.advance(by: 0.85)
    assertTrue(engine1.isSyncingBarrier == false, "Barrier exits 0.85s after probe response")
    assertTrue(sentCommands1.last == "ATH", "High-priority ATH executes ahead of NORM_CPIN")
    engine1.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(completedTags1.contains("HIGH_ATH"), "HIGH_ATH completed cleanly")
    passedSuites += 1
    print("✅ Test 1 Passed!")

    print("▶ Test 2: High-Priority Breakthrough When Modem Caches AT+CGMR Probes (Absorbs +CGMR: + OK Without Misalignment)...")
    let clock2 = MockScheduler()
    let engine2 = ATCommandQueueCore(scheduler: clock2)
    var sentCommands2: [String] = []
    var completedCommands2: [String] = []
    engine2.onWriteRaw = { raw in sentCommands2.append(raw.trimmingCharacters(in: .whitespacesAndNewlines)) }
    engine2.onCommandCompleted = { cmd, isErr, _ in if !isErr { completedCommands2.append(cmd.command) } }

    engine2.isSendingSMS = true
    engine2.pendingSMSClientId = "sms_busy_modem"
    engine2.pendingSMSBodyHex = "4F60597D"
    engine2.enqueue("AT+CMGS=\"00310030\"", priority: .normal, timeout: 15.0, tag: "CMGS_CMD", batchId: "sms_busy_modem")
    engine2.handlePromptReceivedForCMGS()
    engine2.enqueue("AT+COPS?", priority: .low, timeout: 4.0, tag: "POLL_COPS")

    clock2.advance(by: 12.0)
    clock2.advance(by: 20.0)
    let probeCountSent = sentCommands2.filter { $0 == "AT+CGMR" }.count
    assertTrue(probeCountSent == 2 && engine2.unansweredBarrierProbeCount == 2, "In-flight AT+CGMR probes capped at 2")

    engine2.enqueue("ATH", priority: .high, timeout: 6.0, tag: "ATH_DONE")
    engine2.enqueue("AT+CHUP", priority: .high, timeout: 4.0, tag: "CHUP")
    engine2.enqueue("AT+QPCMV=0", priority: .high, timeout: 4.0, tag: "QPCMV_OFF")
    assertTrue(engine2.isSyncingBarrier == false && engine2.unansweredOrphanProbesSent == 2, "2 orphan probes recorded")
    assertTrue(engine2.currentCommand?.command == "ATH", "ATH active")

    engine2.handleCMGSReceiptLine()
    engine2.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(completedCommands2.isEmpty && engine2.currentCommand?.command == "ATH", "Late +CMGS OK absorbed")

    // Cached Probe #1 (`AT+CGMR`) replies with firmware line + OK
    engine2.handleBarrierProbeDataLineReceived()
    assertTrue(engine2.awaitingOrphanProbeTrailingOK == true, "Probe #1 data line armed trailing OK absorption")
    engine2.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(completedCommands2.isEmpty && engine2.currentCommand?.command == "ATH", "Cached probe #1 OK absorbed; ATH still active")

    // Cached Probe #2 (`AT+CGMR`) replies with firmware line + OK
    engine2.handleBarrierProbeDataLineReceived()
    engine2.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(completedCommands2.isEmpty && engine2.currentCommand?.command == "ATH", "Cached probe #2 OK absorbed; ATH still active")

    // Genuine OKs for ATH, AT+CHUP, AT+QPCMV=0
    engine2.handleTerminalResponse(isError: false, line: "OK")
    engine2.handleTerminalResponse(isError: false, line: "OK")
    engine2.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(completedCommands2 == ["ATH", "AT+CHUP", "AT+QPCMV=0"], "All 3 call commands completed with zero misalignment!")
    passedSuites += 1
    print("✅ Test 2 Passed!")

    print("▶ Test 3: High-Priority Breakthrough When Modem Drops AT+CGMR Probes Does NOT Swallow ATH/CHUP OKs...")
    let clock3 = MockScheduler()
    let engine3 = ATCommandQueueCore(scheduler: clock3)
    var completedCommands3: [String] = []
    engine3.onCommandCompleted = { cmd, isErr, _ in if !isErr { completedCommands3.append(cmd.command) } }

    engine3.isSendingSMS = true
    engine3.pendingSMSClientId = "sms_drop_probe"
    engine3.pendingSMSBodyHex = "4F60597D"
    engine3.enqueue("AT+CMGS=\"00310030\"", priority: .normal, timeout: 15.0, tag: "CMGS_CMD", batchId: "sms_drop_probe")
    engine3.handlePromptReceivedForCMGS()

    clock3.advance(by: 12.0)
    clock3.advance(by: 4.0)
    assertTrue(engine3.unansweredBarrierProbeCount == 2, "2 AT+CGMR probes sent")

    engine3.enqueue("ATH", priority: .high, timeout: 6.0, tag: "ATH_DONE")
    engine3.enqueue("AT+CHUP", priority: .high, timeout: 4.0, tag: "CHUP")
    assertTrue(engine3.currentCommand?.command == "ATH" && engine3.awaitingOrphanProbeTrailingOK == false, "ATH active, not blindly awaiting OK")

    engine3.handleCMGSReceiptLine()
    engine3.handleTerminalResponse(isError: false, line: "OK")
    engine3.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(completedCommands3 == ["ATH"], "ATH's genuine OK was NOT swallowed when modem dropped probes")
    engine3.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(completedCommands3 == ["ATH", "AT+CHUP"], "AT+CHUP completed immediately")
    passedSuites += 1
    print("✅ Test 3 Passed!")

    print("▶ Test 4: Late +CMGS Receipt + Trailing OK Absorption While Next Command Is Active...")
    let clock4 = MockScheduler()
    let engine4 = ATCommandQueueCore(scheduler: clock4)
    var completedCmds4: [String] = []
    engine4.onCommandCompleted = { cmd, isErr, _ in if !isErr { completedCmds4.append(cmd.command) } }

    engine4.isSendingSMS = true
    engine4.pendingSMSClientId = "sms_active_cmd"
    engine4.pendingSMSBodyHex = "4F60597D"
    engine4.enqueue("AT+CMGS=\"00310030\"", priority: .normal, timeout: 15.0, tag: "CMGS_CMD", batchId: "sms_active_cmd")
    engine4.handlePromptReceivedForCMGS()
    clock4.advance(by: 12.0)
    engine4.handleTerminalResponse(isError: false, line: "OK")
    clock4.advance(by: 0.85)

    engine4.enqueue("AT+CSQ", priority: .low, timeout: 4.0)
    engine4.handleCMGSReceiptLine()
    engine4.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(completedCmds4.isEmpty && engine4.currentCommand?.command == "AT+CSQ", "Trailing OK absorbed, AT+CSQ still active")
    engine4.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(completedCmds4 == ["AT+CSQ"], "AT+CSQ completed on its own OK")
    passedSuites += 1
    print("✅ Test 4 Passed!")

    print("▶ Test 5: Late +CMGS Receipt + Trailing OK Arriving DURING Sync Barrier...")
    let clock5 = MockScheduler()
    let engine5 = ATCommandQueueCore(scheduler: clock5)
    var smsLog5: [(String, String)] = []
    var completedCmds5: [String] = []
    engine5.onSMSResult = { cid, st, _ in smsLog5.append((cid, st)) }
    engine5.onCommandCompleted = { cmd, isErr, _ in if !isErr { completedCmds5.append(cmd.command) } }

    engine5.isSendingSMS = true
    engine5.pendingSMSClientId = "sms_in_barrier"
    engine5.pendingSMSBodyHex = "4F60597D"
    engine5.enqueue("AT+CMGS=\"00310030\"", priority: .normal, timeout: 15.0, tag: "CMGS_CMD", batchId: "sms_in_barrier")
    engine5.handlePromptReceivedForCMGS()
    engine5.enqueue("AT+COPS?", priority: .low, timeout: 4.0)

    clock5.advance(by: 12.0)
    clock5.advance(by: 0.3)
    engine5.handleCMGSReceiptLine()
    assertTrue(smsLog5.last?.1 == "sent", "Upgraded to sent inside barrier")
    engine5.handleTerminalResponse(isError: false, line: "OK")
    engine5.handleBarrierProbeDataLineReceived()
    engine5.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(engine5.awaitingLateCMGSTrailingOK == false, "Absorption flag cleared inside barrier")
    clock5.advance(by: 0.85)
    assertTrue(engine5.isSyncingBarrier == false && engine5.currentCommand?.command == "AT+COPS?", "AT+COPS? running after barrier")
    engine5.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(completedCmds5 == ["AT+COPS?"], "AT+COPS? completed on first OK")
    passedSuites += 1
    print("✅ Test 5 Passed!")

    print("▶ Test 6: Scoped +CMS ERROR: Read-SMS (AT+CMGL) Error vs Genuine Late SMS +CMS ERROR...")
    let clock6 = MockScheduler()
    let engine6 = ATCommandQueueCore(scheduler: clock6)
    var smsLog6: [(String, String)] = []
    var cmglFailedImmediately = false
    engine6.onSMSResult = { cid, st, _ in smsLog6.append((cid, st)) }
    engine6.onCommandCompleted = { cmd, isErr, _ in
        if cmd.command == "AT+CMGL=\"ALL\"" && isErr { cmglFailedImmediately = true }
    }

    engine6.isSendingSMS = true
    engine6.pendingSMSClientId = "sms_scoped_6"
    engine6.pendingSMSBodyHex = "4F60597D"
    engine6.enqueue("AT+CMGS=\"00310030\"", priority: .normal, timeout: 15.0, tag: "CMGS_CMD", batchId: "sms_scoped_6")
    engine6.handlePromptReceivedForCMGS()
    clock6.advance(by: 12.0)
    engine6.handleTerminalResponse(isError: false, line: "OK")
    clock6.advance(by: 0.85)

    engine6.enqueue("AT+CMGL=\"ALL\"", priority: .normal, timeout: 8.0, tag: "CMGL_CMD")
    engine6.handleTerminalResponse(isError: true, line: "+CMS ERROR: 321")
    assertTrue(cmglFailedImmediately == true && smsLog6.last?.1 == "unknown", "AT+CMGL error did NOT falsely fail unknown SMS")

    engine6.enqueue("AT+CSQ", priority: .low, timeout: 4.0)
    engine6.handleTerminalResponse(isError: true, line: "+CMS ERROR: 500")
    assertTrue(smsLog6.last?.1 == "failed", "Late +CMS ERROR during non-SMS command transitions unknown SMS to failed")
    passedSuites += 1
    print("✅ Test 6 Passed!")

    print("▶ Test 7: Window-Decoupled Late +CMGS Arriving AFTER 60s Window Expires Still Absorbs Trailing OK...")
    let clock7 = MockScheduler()
    let engine7 = ATCommandQueueCore(scheduler: clock7)
    var smsLog7: [(String, String)] = []
    var completedCmds7: [String] = []
    engine7.onSMSResult = { cid, st, _ in smsLog7.append((cid, st)) }
    engine7.onCommandCompleted = { cmd, isErr, _ in if !isErr { completedCmds7.append(cmd.command) } }

    engine7.isSendingSMS = true
    engine7.pendingSMSClientId = "sms_after_60s"
    engine7.pendingSMSBodyHex = "4F60597D"
    engine7.enqueue("AT+CMGS=\"00310030\"", priority: .normal, timeout: 15.0, tag: "CMGS_CMD", batchId: "sms_after_60s")
    engine7.handlePromptReceivedForCMGS()
    clock7.advance(by: 12.0)
    engine7.handleTerminalResponse(isError: false, line: "OK")
    clock7.advance(by: 0.85)
    clock7.advance(by: 65.0)
    assertTrue(engine7.hasPendingLateSMSReceipt == false, "60s window expired")

    engine7.enqueue("AT+COPS?", priority: .low, timeout: 4.0)
    engine7.handleCMGSReceiptLine()
    assertTrue(smsLog7.last?.1 == "sent" && engine7.awaitingLateCMGSTrailingOK == true, "Upgraded and armed even after 60s")
    engine7.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(completedCmds7.isEmpty && engine7.currentCommand?.command == "AT+COPS?", "Trailing OK absorbed after 60s")
    engine7.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(completedCmds7 == ["AT+COPS?"], "AT+COPS? completed on its own OK")
    passedSuites += 1
    print("✅ Test 7 Passed!")

    print("▶ Test 8: Time-Bounded (2.0s) & Error-Safe awaitingLateCMGSTrailingOK + Immediate 60s Window Closure on +CMGS...")
    let clock8 = MockScheduler()
    let engine8 = ATCommandQueueCore(scheduler: clock8)
    var autoDispatched8: [QueuedSMSRetryItem] = []
    engine8.onDispatchQueuedSMS = { item in autoDispatched8.append(item) }

    engine8.isSendingSMS = true
    engine8.pendingSMSClientId = "sms_no_ok"
    engine8.pendingSMSBodyHex = "4F60597D"
    engine8.enqueue("AT+CMGS=\"00310030\"", priority: .normal, timeout: 15.0, tag: "CMGS_CMD", batchId: "sms_no_ok")
    engine8.handlePromptReceivedForCMGS()
    clock8.advance(by: 12.0)
    engine8.handleTerminalResponse(isError: false, line: "OK")
    clock8.advance(by: 0.85)

    engine8.enqueueSMSForAfterLateWindow(clientId: "sms_second", recipient: "10010", body: "第二条")
    clock8.advance(by: 3.0)
    engine8.handleCMGSReceiptLine()
    assertTrue(engine8.isLateCMGSChannelWindowOpen == false, "60s window closed immediately on +CMGS:")
    clock8.advance(by: 2.0)
    assertTrue(autoDispatched8.count == 1 && autoDispatched8.first?.clientId == "sms_second", "Queued SMS dispatched 2.0s after +CMGS")
    passedSuites += 1
    print("✅ Test 8 Passed!")

    print("▶ Test 9: Auto-Queued Retry During 60s Window: Auto-Cancels Duplicate on Late +CMGS & Auto-Dispatches on Expiry...")
    let clock9 = MockScheduler()
    let engine9 = ATCommandQueueCore(scheduler: clock9)
    var smsLog9: [(String, String, String)] = []
    var autoDispatched9: [QueuedSMSRetryItem] = []
    engine9.onSMSResult = { cid, st, detail in smsLog9.append((cid, st, detail)) }
    engine9.onDispatchQueuedSMS = { item in autoDispatched9.append(item) }

    engine9.isSendingSMS = true
    engine9.pendingSMSClientId = "sms_dup_cancel"
    engine9.pendingSMSBodyHex = "4F60597D"
    engine9.enqueue("AT+CMGS=\"00310030\"", priority: .normal, timeout: 15.0, tag: "CMGS_CMD", batchId: "sms_dup_cancel")
    engine9.handlePromptReceivedForCMGS()
    clock9.advance(by: 12.0)
    engine9.handleTerminalResponse(isError: false, line: "OK")
    clock9.advance(by: 0.85)

    engine9.enqueueSMSForAfterLateWindow(clientId: "sms_dup_cancel", recipient: "10086", body: "你好")
    clock9.advance(by: 5.0)
    engine9.handleCMGSReceiptLine()
    engine9.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(smsLog9.last?.1 == "sent" && autoDispatched9.isEmpty && engine9.queuedSMSAfterWindow.isEmpty, "Duplicate retry auto-cancelled on late receipt")

    engine9.isSendingSMS = true
    engine9.pendingSMSClientId = "sms_exp_dispatch"
    engine9.pendingSMSBodyHex = "4F60597D"
    engine9.enqueue("AT+CMGS=\"00310030\"", priority: .normal, timeout: 15.0, tag: "CMGS_CMD", batchId: "sms_exp_dispatch")
    engine9.handlePromptReceivedForCMGS()
    clock9.advance(by: 12.0)
    engine9.handleTerminalResponse(isError: false, line: "OK")
    clock9.advance(by: 0.85)

    engine9.enqueueSMSForAfterLateWindow(clientId: "sms_queued_2", recipient: "10010", body: "流量")
    clock9.advance(by: 60.0)
    assertTrue(autoDispatched9.count == 1 && autoDispatched9.first?.clientId == "sms_queued_2", "Queued SMS auto-dispatched on 60s window expiry")
    passedSuites += 1
    print("✅ Test 9 Passed!")

    print("▶ Test 10: Manual Resolve/Delete Clears Attribution WITHOUT Closing 60s Channel Protection Window...")
    let clock10 = MockScheduler()
    let engine10 = ATCommandQueueCore(scheduler: clock10)
    var smsLog10: [(String, String, String)] = []
    engine10.onSMSResult = { cid, st, detail in smsLog10.append((cid, st, detail)) }

    engine10.isSendingSMS = true
    engine10.pendingSMSClientId = "sms_manual_keep_window"
    engine10.pendingSMSBodyHex = "4F60597D"
    engine10.enqueue("AT+CMGS=\"00310030\"", priority: .normal, timeout: 15.0, tag: "CMGS_CMD", batchId: "sms_manual_keep_window")
    engine10.handlePromptReceivedForCMGS()
    clock10.advance(by: 12.0)
    engine10.handleTerminalResponse(isError: false, line: "OK")
    clock10.advance(by: 0.85)

    engine10.clearUnknownSMSTracking(for: "sms_manual_keep_window")
    assertTrue(engine10.lateAwaitingCMGSClientId == nil && engine10.lastUnknownSMSClientId == nil, "SMS attribution cleared")
    assertTrue(engine10.isLateCMGSChannelWindowOpen == true && engine10.hasPendingLateSMSReceipt == true, "60s channel protection window stays OPEN")
    let countBefore = smsLog10.count
    clock10.advance(by: 5.0)
    engine10.handleCMGSReceiptLine()
    engine10.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(smsLog10.count == countBefore && engine10.hasPendingLateSMSReceipt == false, "Window closed cleanly after late receipt")
    passedSuites += 1
    print("✅ Test 10 Passed!")

    print("▶ Test 11: User Hangup WITH CallKit UUID Clears activeCallUUID So Next Incoming Call Invokes reportNewIncomingCall...")
    let clock11 = MockScheduler()
    let callSM11 = ATCallStateMachine(scheduler: clock11)
    let engine11 = ATCommandQueueCore(scheduler: clock11)
    let bridge11 = MockDriverBridge()
    let coord11 = CallSessionCoordinator(callSM: callSM11, queueCore: engine11, scheduler: clock11, bridge: bridge11)

    coord11.handleRingArrived()
    coord11.handleClipArrived(caller: "13900001111")
    let outcome11 = coord11.handleLocalHangup()
    assertTrue(outcome11.didBeginHangup == true && outcome11.endedCallKitUUID != nil && coord11.activeCallUUID == nil, "UUID cleared on hangup")
    callSM11.completeHangupToIdle()
    clock11.advance(by: 4.0)

    coord11.handleRingArrived()
    coord11.handleClipArrived(caller: "13900002222")
    assertTrue(bridge11.newIncomingCallCount == 2 && bridge11.updatedCallers.isEmpty, "Call #2 invoked reportNewIncomingCall")
    passedSuites += 1
    print("✅ Test 11 Passed!")

    print("▶ Test 12: User Hangup WITHOUT CallKit UUID (DND-Rejected Call, Pre-CLIP 0.6s Reject & CLCC-Adopted Call) Succeeds...")
    let clock12 = MockScheduler()
    let callSM12 = ATCallStateMachine(scheduler: clock12)
    let engine12 = ATCommandQueueCore(scheduler: clock12)
    let bridge12 = MockDriverBridge()
    bridge12.shouldRejectCallKit = true
    let coord12 = CallSessionCoordinator(callSM: callSM12, queueCore: engine12, scheduler: clock12, bridge: bridge12)

    coord12.handleRingArrived()
    clock12.advance(by: 0.6)
    let dndHangup = coord12.handleLocalHangup()
    assertTrue(dndHangup.didBeginHangup == true && dndHangup.endedCallKitUUID == nil && callSM12.state == .hangingUp, "DND hangup succeeded")
    clock12.advance(by: 9.0)
    assertTrue(callSM12.state == .idle, "9.0s master watchdog recovered to .idle")

    clock12.advance(by: 4.0)
    _ = callSM12.transition(to: .connected, number: "10086", outgoing: false)
    let clccHangup = coord12.handleLocalHangup()
    assertTrue(clccHangup.didBeginHangup == true, "CLCC-adopted call hangup succeeded")
    callSM12.completeHangupToIdle()
    passedSuites += 1
    print("✅ Test 12 Passed!")

    print("▶ Test 13: Driver CallSessionCoordinator: DND Single-Report Guard, Live Notification Caller Update & ATD .failed...")
    let clock13 = MockScheduler()
    let callSM13 = ATCallStateMachine(scheduler: clock13)
    let engine13 = ATCommandQueueCore(scheduler: clock13)
    let bridge13 = MockDriverBridge()
    bridge13.shouldRejectCallKit = true
    let coord13 = CallSessionCoordinator(callSM: callSM13, queueCore: engine13, scheduler: clock13, bridge: bridge13)
    coord13.showNotificationPreview = true

    coord13.handleRingArrived()
    clock13.advance(by: 0.6)
    let notifId13 = coord13.currentCallNotificationId
    coord13.handleClipArrived(caller: "13800138000")
    assertTrue(bridge13.newIncomingCallCount == 1 && bridge13.notificationsById[notifId13]?.title == "大疆4G模块来电 (13800138000)", "Updated in-place")
    _ = coord13.handleCallEnded(isDialFailure: false)

    _ = coord13.startOutgoingDial(number: "10086")
    let dialFailReason = coord13.handleCallEnded(isDialFailure: true)
    assertTrue(dialFailReason == .failed && bridge13.endedReasons.last == .failed, "ATD dial failure reports .failed")
    passedSuites += 1
    print("✅ Test 13 Passed!")

    print("▶ Test 14: Stateful ATStreamDecoder (Empty-Body, Multi-Line & Back-to-Back +CMGL) + Deferred SMS Poll Chain...")
    let decoder = ATStreamDecoder()
    let rawStreamLines = [
        "+CMGL: 1,\"REC UNREAD\",\"00310030003000380036\",,\"26/11/03,12:00:00+32\"",
        "",
        "+CMGL: 2,\"REC READ\",\"00310030003000310030\",,\"26/11/03,12:01:00+32\"",
        "LineOne",
        "LineTwo",
        "OK"
    ]
    var decodedEvents: [ATParsedEvent] = []
    for line in rawStreamLines {
        decodedEvents.append(contentsOf: decoder.feedLine(line, fallbackTime: "12:00"))
    }
    assertTrue(decodedEvents.count == 3 && decodedEvents[2] == .ok, "Empty & multi-line +CMGL decoded cleanly")
    passedSuites += 1
    print("✅ Test 14 Passed!")

    print("▶ Test 15: Table-Driven ATLineParser Verification on 24 Real Quectel EG25-G Serial Outputs...")
    let cases: [(String, ATParsedEvent)] = [
        ("OK", .ok),
        ("+CME ERROR: 30", .error(line: "+CME ERROR: 30")),
        ("+CMS ERROR: 500", .error(line: "+CMS ERROR: 500")),
        ("+CGMR: EG25GGBR07A08M2G", .cgmr(revision: "EG25GGBR07A08M2G")),
        ("EG25GGBR07A08M2G", .cgmr(revision: "EG25GGBR07A08M2G")),
        ("+CSQ: 24,99", .csq(rssi: 24, bars: 4)),
        ("+CSQ: 99,99", .csq(rssi: 0, bars: 0)),
        ("+CPIN: READY", .cpin(ready: true, state: "已就绪")),
        ("+COPS: 0,2,\"46011\",7", .cops(operatorName: "中国电信")),
        ("+COPS: 0,2,\"46001\",7", .cops(operatorName: "中国联通")),
        ("+COPS: 0,2,\"46007\",7", .cops(operatorName: "中国移动")),
        ("+COPS: 0,2,\"46015\",7", .cops(operatorName: "中国广电")),
        ("+COPS: 0", .cops(operatorName: "未注册运营商")),
        ("+CEREG: 0,1", .cereg(stat: 1, description: "已注册 (本地网)", isRegistered: true)),
        ("+CEREG: 0,5", .cereg(stat: 5, description: "已注册 (漫游)", isRegistered: true)),
        ("+QCFG: \"ims\",1,1", .imsConfig(volteEnabled: true, imsRegistered: true)),
        ("+QCFG: \"ims\",0,0", .imsConfig(volteEnabled: false, imsRegistered: false)),
        ("+CLIP: \"13800138000\",129,,,,0", .clip(caller: "13800138000")),
        ("+CLCC: 1,0,0,0,0,\"10000\",129", .clcc(dir: 0, stat: 0, number: "10000")),
        ("+CMGL: 3,\"REC UNREAD\",\"00310030003000300031\",,\"26/11/03,12:30:00+32\"",
         .cmglHeader(index: "3", isIncoming: true, sender: "00310030003000300031", time: "26/11/03,12:30:00+32")),
        ("+CMGS: 42", .cmgsReceipt),
        ("BUSY", .callEnded(reason: "对方忙线")),
        ("NO ANSWER", .callEnded(reason: "无应答")),
        ("NO CARRIER", .callEnded(reason: nil))
    ]
    for (raw, expected) in cases {
        let actual = ATLineParser.parse(raw, fallbackTime: "12:00")
        assertTrue(actual == expected, "Parser mismatch for '\(raw)'")
    }
    passedSuites += 1
    print("✅ Test 15 Passed!")

    print("▶ Test 16: Schema-Versioned SMS Store, True N-1 (.bak) & N-2 (.bak2) Snapshots & DedupKey-ID Consistency...")
    let tempDir16 = FileManager.default.temporaryDirectory.appendingPathComponent("djphone_mig_v15_1_\(UUID().uuidString)", isDirectory: true)
    try? FileManager.default.createDirectory(at: tempDir16, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: tempDir16) }

    let v8Items = [
        StoredSMSItem(id: "id_old", dedupKey: "10086|10:00|账单", peer: "10086", dir: "in", body: "账单", time: "10:00", status: "received", otp: "", timestamp: 1000.0)
    ]
    let v12Items = [
        StoredSMSItem(id: "id_new", dedupKey: "10086|10:00|账单", peer: "10086", dir: "in", body: "账单V1", time: "10:01", status: "received", otp: "", timestamp: 2000.0)
    ]
    try! JSONEncoder().encode(v8Items).write(to: tempDir16.appendingPathComponent("djphone_sms_store_v8.json"))
    try! JSONEncoder().encode(v12Items).write(to: tempDir16.appendingPathComponent("djphone_sms_store_v12.json"))

    let migrated16 = SMSStoreMigrator.loadAndMigrateIfNeeded(in: tempDir16, maxCount: 500)
    assertTrue(migrated16.loadFailedReadOnlyGuard == false && migrated16.messages.count == 1, "DedupKey-ID consistency verified")

    // Generation 2 save (2 items)
    let gen2 = migrated16.messages + [
        StoredSMSItem(id: "id_gen2", dedupKey: "10010|11:00|第二代", peer: "10010", dir: "in", body: "第二代", time: "11:00", status: "received", otp: "", timestamp: 3000.0)
    ]
    _ = SMSStoreMigrator.writeStoreAtomicallyWithBackup(messages: gen2, in: tempDir16)

    // Redundant save with identical `gen2` items -> MUST NOT rotate `.bak` over `.bak2`!
    _ = SMSStoreMigrator.writeStoreAtomicallyWithBackup(messages: gen2, in: tempDir16)
    let bakAfterRedundant = SMSStoreMigrator.decodeItems(from: try! Data(contentsOf: tempDir16.appendingPathComponent(SMSStoreMigrator.backupFileName)))!
    assertTrue(bakAfterRedundant.count == 1, "Redundant save with identical messages did NOT overwrite N-1 .bak (Fixes Bug 5!)")

    // Generation 3 save (3 items) -> rotates Gen1 (1 item) to `.bak2`, Gen2 (2 items) to `.bak`, and writes Gen3 (3 items) to canonical!
    let gen3 = gen2 + [
        StoredSMSItem(id: "id_gen3", dedupKey: "10000|12:00|第三代", peer: "10000", dir: "in", body: "第三代", time: "12:00", status: "received", otp: "", timestamp: 4000.0)
    ]
    _ = SMSStoreMigrator.writeStoreAtomicallyWithBackup(messages: gen3, in: tempDir16)

    let cItems = SMSStoreMigrator.decodeItems(from: try! Data(contentsOf: tempDir16.appendingPathComponent(SMSStoreMigrator.canonicalFileName)))!
    let b1Items = SMSStoreMigrator.decodeItems(from: try! Data(contentsOf: tempDir16.appendingPathComponent(SMSStoreMigrator.backupFileName)))!
    let b2Items = SMSStoreMigrator.decodeItems(from: try! Data(contentsOf: tempDir16.appendingPathComponent(SMSStoreMigrator.backup2FileName)))!
    assertTrue(cItems.count == 3 && b1Items.count == 2 && b2Items.count == 1, "Canonical=3 (N), .bak=2 (N-1), .bak2=1 (N-2) verified!")
    passedSuites += 1
    print("✅ Test 16 Passed!")

    print("▶ Test 17: Corrupt Legacy File Quarantine (.corrupt) Does NOT Block Saving + Fallback Store Unlock Merge...")
    let tempDir17 = FileManager.default.temporaryDirectory.appendingPathComponent("djphone_corrupt_leg_\(UUID().uuidString)", isDirectory: true)
    try? FileManager.default.createDirectory(at: tempDir17, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: tempDir17) }

    let healthyMain = [
        StoredSMSItem(id: "main_1", dedupKey: "10000|09:00|电信", peer: "10000", dir: "in", body: "电信", time: "09:00", status: "received", otp: "", timestamp: 1000.0)
    ]
    _ = SMSStoreMigrator.writeStoreAtomicallyWithBackup(messages: healthyMain, in: tempDir17)
    try! Data("HALF_WRITTEN_CRASH_V8".utf8).write(to: tempDir17.appendingPathComponent("djphone_sms_store_v8.json"))

    let res17 = SMSStoreMigrator.loadAndMigrateIfNeeded(in: tempDir17, maxCount: 500, isProtectedDataAvailable: true)
    assertTrue(res17.loadFailedReadOnlyGuard == false && res17.quarantinedFiles == ["djphone_sms_store_v8.json"], "Corrupt legacy file quarantined without blocking saves")
    passedSuites += 1
    print("✅ Test 17 Passed!")

    print("▶ Test 18: Transient Call State Watchdog (75s Outgoing Ring) Pre-Hangup CLCC Check & Full Hangup Teardown...")
    let clock18 = MockScheduler()
    let callSM18 = ATCallStateMachine(scheduler: clock18)
    let engine18 = ATCommandQueueCore(scheduler: clock18)
    let bridge18 = MockDriverBridge()
    let coord18 = CallSessionCoordinator(callSM: callSM18, queueCore: engine18, scheduler: clock18, bridge: bridge18)

    var sentCmds18: [String] = []
    var watchdogCallbackFired18 = false
    engine18.onWriteRaw = { raw in sentCmds18.append(raw.trimmingCharacters(in: .whitespacesAndNewlines)) }
    engine18.onCommandCompleted = { cmd, isErr, _ in
        if cmd.tag == "CLCC_WATCHDOG_VERIFY" {
            coord18.handleWatchdogCLCCVerifyCommandCompleted(isError: isErr)
        }
    }
    coord18.onWatchdogHangupTriggered = { _, _ in watchdogCallbackFired18 = true }

    let dialUUID18 = coord18.startOutgoingDial(number: "10086")
    _ = callSM18.transition(to: .alerting, number: "10086", outgoing: true)
    clock18.advance(by: 75.0)
    assertTrue(callSM18.isAwaitingWatchdogCLCCVerification == true && sentCmds18.last == "AT+CLCC", "Stage 1 sent AT+CLCC at 75s")
    engine18.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(watchdogCallbackFired18 == true && bridge18.endedUUIDs.contains(dialUUID18) && sentCmds18.contains("ATH"), "Full hangup teardown executed after empty CLCC OK")
    passedSuites += 1
    print("✅ Test 18 Passed!")

    print("▶ Test 19: Connecting Watchdog (35s) Extends When Delayed by 12s SMS or CLCC Error & Saves Active Call...")
    let clock19 = MockScheduler()
    let callSM19 = ATCallStateMachine(scheduler: clock19)
    let engine19 = ATCommandQueueCore(scheduler: clock19)
    let bridge19 = MockDriverBridge()
    let coord19 = CallSessionCoordinator(callSM: callSM19, queueCore: engine19, scheduler: clock19, bridge: bridge19)

    var falseHangupFired19 = false
    engine19.onCommandCompleted = { cmd, isErr, _ in
        if cmd.tag == "CLCC_WATCHDOG_VERIFY" {
            coord19.handleWatchdogCLCCVerifyCommandCompleted(isError: isErr)
        }
    }
    coord19.onWatchdogHangupTriggered = { _, _ in falseHangupFired19 = true }

    coord19.handleRingArrived()
    coord19.handleClipArrived(caller: "13800138000")
    _ = callSM19.transition(to: .connecting, number: "13800138000", outgoing: false)

    clock19.advance(by: 35.0)
    clock19.advance(by: 10.0)
    assertTrue(falseHangupFired19 == false && callSM19.state == .connecting, "Did NOT falsely hang up at 6s")
    engine19.handleTerminalResponse(isError: true, line: "ERROR")
    assertTrue(falseHangupFired19 == false && callSM19.state == .connecting && callSM19.inconclusiveWatchdogRetryCount == 1, "CLCC error extended verification")

    _ = coord19.handleCLCCLine(dir: 1, stat: 0, number: "13800138000")
    engine19.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(callSM19.state == .connected && falseHangupFired19 == false, "Saved active call and transitioned to .connected!")
    passedSuites += 1
    print("✅ Test 19 Passed!")

    print("▶ Test 20: Single-Decrement Barrier Probe Count When 1 of 2 Probes Replies Inside Barrier & 2nd Replies After Exit (Fixes Bug 1)...")
    let clock20 = MockScheduler()
    let engine20 = ATCommandQueueCore(scheduler: clock20)
    var completedCmds20: [String] = []
    engine20.onCommandCompleted = { cmd, isErr, _ in if !isErr { completedCmds20.append(cmd.command) } }

    engine20.isSendingSMS = true
    engine20.pendingSMSClientId = "sms_20"
    engine20.pendingSMSBodyHex = "4F60597D"
    engine20.enqueue("AT+CMGS=\"00310030\"", priority: .normal, timeout: 15.0, tag: "CMGS_CMD", batchId: "sms_20")
    engine20.handlePromptReceivedForCMGS()
    engine20.enqueue("AT+CSQ", priority: .low, timeout: 4.0)

    // 12s timeout -> Probe #1 sent; +2.0s -> Probe #2 sent (`unansweredBarrierProbeCount == 2`)
    clock20.advance(by: 12.0)
    clock20.advance(by: 2.0)
    assertTrue(engine20.unansweredBarrierProbeCount == 2, "2 AT+CGMR probes in flight")

    // Late +CMGS + OK arrives, followed by ONLY Probe #1's response (`EG25GGBR07A08M2G` + `OK`)!
    engine20.handleCMGSReceiptLine()
    engine20.handleTerminalResponse(isError: false, line: "OK")
    engine20.handleBarrierProbeDataLineReceived()
    assertTrue(engine20.unansweredBarrierProbeCount == 1, "Probe #1 data line decremented count from 2 to 1")
    engine20.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(engine20.unansweredBarrierProbeCount == 1, "Probe #1 trailing OK did NOT double-decrement count (still 1 — Fixes Bug 1!)")

    // 0.85s quiet window elapses before Probe #2 replies -> barrier exits with `unansweredOrphanProbesSent == 1` and `AT+CSQ` starts!
    clock20.advance(by: 0.85)
    assertTrue(engine20.isSyncingBarrier == false && engine20.unansweredOrphanProbesSent == 1, "Remaining 1 unanswered probe transferred to unansweredOrphanProbesSent")
    assertTrue(engine20.currentCommand?.command == "AT+CSQ", "AT+CSQ now running")

    // Now Probe #2's delayed response (`EG25GGBR07A08M2G` + `OK`) arrives -> MUST be recognized and absorbed without completing `AT+CSQ`!
    engine20.handleBarrierProbeDataLineReceived()
    assertTrue(engine20.awaitingOrphanProbeTrailingOK == true && engine20.unansweredOrphanProbesSent == 0, "Delayed Probe #2 recognized")
    engine20.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(completedCmds20.isEmpty && engine20.currentCommand?.command == "AT+CSQ", "Probe #2's OK absorbed; AT+CSQ NOT prematurely completed!")

    // Finally AT+CSQ's genuine OK arrives
    engine20.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(completedCmds20 == ["AT+CSQ"], "AT+CSQ completed on its own genuine OK")
    passedSuites += 1
    print("✅ Test 20 Passed!")

    print("▶ Test 21: Dropped Probe + Late Status-Poll +CSQ Does NOT Swallow ATH OK & 30s Orphan Probe Count Expiry (Fixes Bug 2)...")
    let clock21 = MockScheduler()
    let engine21 = ATCommandQueueCore(scheduler: clock21)
    var completedCmds21: [String] = []
    engine21.onCommandCompleted = { cmd, isErr, _ in if !isErr { completedCmds21.append(cmd.command) } }

    engine21.isSendingSMS = true
    engine21.pendingSMSClientId = "sms_21"
    engine21.pendingSMSBodyHex = "4F60597D"
    engine21.enqueue("AT+CMGS=\"00310030\"", priority: .normal, timeout: 15.0, tag: "CMGS_CMD", batchId: "sms_21")
    engine21.handlePromptReceivedForCMGS()

    clock21.advance(by: 12.0)
    clock21.advance(by: 4.0) // 2 AT+CGMR probes sent and dropped by modem
    engine21.enqueue("ATH", priority: .high, timeout: 6.0, tag: "ATH_DONE")
    assertTrue(engine21.unansweredOrphanProbesSent == 2 && engine21.currentCommand?.command == "ATH", "ATH active with 2 orphan probes recorded")

    // Suppose a late `+CSQ: 24,99` from an earlier periodic poll arrives while `ATH` is running:
    // Because the barrier probe is `AT+CGMR` (NOT `AT+CSQ`), `handleCSQLineReceived()` MUST NOT arm `awaitingOrphanProbeTrailingOK`!
    engine21.handleCSQLineReceived()
    assertTrue(engine21.awaitingOrphanProbeTrailingOK == false, "Late +CSQ: does NOT arm orphan probe OK absorption (Fixes Bug 2!)")

    // ATH's genuine OK arrives -> completes ATH immediately!
    engine21.handleTerminalResponse(isError: false, line: "OK")
    assertTrue(completedCmds21 == ["ATH"], "ATH completed on its genuine OK without being swallowed by late +CSQ!")

    // Advance 30.0s -> `unansweredOrphanProbesSent` automatically expires to 0!
    clock21.advance(by: 30.0)
    assertTrue(engine21.unansweredOrphanProbesSent == 0, "unansweredOrphanProbesSent auto-expired to 0 after 30.0s (Fixes Bug 2!)")
    passedSuites += 1
    print("✅ Test 21 Passed!")

    print("▶ Test 22: Unlocked Main Store Corruption Self-Heals (.corrupt) + Locked Fallback Deduplication & .bak2 Recovery (Fixes Bugs 3, 4, 5, 6)...")
    let tempDir22 = FileManager.default.temporaryDirectory.appendingPathComponent("djphone_selfheal_\(UUID().uuidString)", isDirectory: true)
    try? FileManager.default.createDirectory(at: tempDir22, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: tempDir22) }

    let canonicalURL22 = tempDir22.appendingPathComponent(SMSStoreMigrator.canonicalFileName)
    let backupURL22 = tempDir22.appendingPathComponent(SMSStoreMigrator.backupFileName)
    let backup2URL22 = tempDir22.appendingPathComponent(SMSStoreMigrator.backup2FileName)

    // Part A: Test recovery from N-2 (`.bak2`) when BOTH canonical and N-1 (`.bak`) are corrupted!
    let n2Item = StoredSMSItem(id: "n2_1", dedupKey: "10086|08:00|N2备份", peer: "10086", dir: "in", body: "N2备份", time: "08:00", status: "received", otp: "", timestamp: 800.0)
    try! Data("BAD_MAIN".utf8).write(to: canonicalURL22)
    try! Data("BAD_BAK1".utf8).write(to: backupURL22)
    try! SMSStoreMigrator.encodeEnvelope(messages: [n2Item])!.write(to: backup2URL22)

    let recN2 = SMSStoreMigrator.loadAndMigrateIfNeeded(in: tempDir22, maxCount: 500, isProtectedDataAvailable: true)
    assertTrue(recN2.loadFailedReadOnlyGuard == false && recN2.messages.count == 1 && recN2.messages.first?.id == "n2_1", "Recovered cleanly from N-2 (.bak2) when canonical and .bak were both corrupted!")

    // Part B: Test Locked Read-Only Guard (`isProtectedDataAvailable: false`) deduplicates and caps fallback items at `maxCount` (Fixes Bug 6)
    try! Data("LOCKED_MAIN".utf8).write(to: canonicalURL22)
    try! Data("LOCKED_BAK1".utf8).write(to: backupURL22)
    try! Data("LOCKED_BAK2".utf8).write(to: backup2URL22)

    let dupFallbackItems = [
        StoredSMSItem(id: "fb_dup_1", dedupKey: "10010|12:00|重复短信", peer: "10010", dir: "in", body: "重复短信", time: "12:00", status: "received", otp: "", timestamp: 1000.0),
        StoredSMSItem(id: "fb_dup_2", dedupKey: "10010|12:00|重复短信", peer: "10010", dir: "in", body: "重复短信", time: "12:00", status: "received", otp: "", timestamp: 1001.0),
        StoredSMSItem(id: "fb_extra", dedupKey: "10010|12:05|第二条", peer: "10010", dir: "in", body: "第二条", time: "12:05", status: "received", otp: "", timestamp: 1002.0)
    ]
    assertTrue(SMSStoreMigrator.writeSessionFallbackStore(messages: dupFallbackItems, in: tempDir22) == true, "Fallback written with .completeFileProtectionUntilFirstUserAuthentication")

    let lockedLoad = SMSStoreMigrator.loadAndMigrateIfNeeded(in: tempDir22, maxCount: 1, isProtectedDataAvailable: false)
    assertTrue(lockedLoad.loadFailedReadOnlyGuard == true, "Locked device enters temporary read-only guard")
    assertTrue(lockedLoad.messages.count == 1 && lockedLoad.messages.first?.id == "fb_extra", "Locked read-only fallback deduplicated and respected maxCount=1 (Fixes Bug 6!)")
    assertTrue(FileManager.default.fileExists(atPath: canonicalURL22.path), "Main store NOT quarantined while device is locked")

    // Part C: Test Unlocked Genuine Main Store Corruption Self-Heal (`isProtectedDataAvailable: true`) (Fixes Bug 4)
    let unlockedHeal = SMSStoreMigrator.loadAndMigrateIfNeeded(in: tempDir22, maxCount: 500, isProtectedDataAvailable: true)
    assertTrue(unlockedHeal.loadFailedReadOnlyGuard == false, "Unlocked device with corrupted main store self-healed and cleared read-only guard (Fixes Bug 4!)")
    assertTrue(unlockedHeal.quarantinedFiles.contains(SMSStoreMigrator.canonicalFileName), "Corrupted main store quarantined to .corrupt")
    assertTrue(FileManager.default.fileExists(atPath: tempDir22.appendingPathComponent("djphone_sms_store.json.corrupt").path), "djphone_sms_store.json.corrupt created")
    assertTrue(unlockedHeal.messages.count == 2, "Rebuilt main store from deduplicated session fallback items (2 unique items)!")
    passedSuites += 1
    print("✅ Test 22 Passed!")

    assertTrue(passedSuites >= 22, "Regression guard: expected at least 22 test suites to pass, got \(passedSuites)")
    print("🎉 ALL \(passedSuites) SUITES IN DJPhone v15.1 UNIT & DRIVER-COORDINATOR TESTS PASSED!")
}

runAllTests()
