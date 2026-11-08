//
//  DJPhoneApp.swift
//  DJPhone v15.1
//

import SwiftUI
import UIKit
import UserNotifications

@main
struct DJPhoneApp: App {
    @StateObject private var modemDriver = QuectelModemDriver.shared

    var body: some Scene {
        WindowGroup {
            ContentView()
                .environmentObject(modemDriver)
                .onAppear {
                    UIApplication.shared.isIdleTimerDisabled = true
                    UNUserNotificationCenter.current().requestAuthorization(options: [.alert, .sound, .badge]) { _, _ in }
                    modemDriver.initializeBridge()
                }
        }
    }
}
