# DJPhone v15.2 (在 v15.1 基础上的小改动，未编译/未实机验证)

- Info.plist 增加 `UILaunchScreen`：修复新款 iPhone 上 App 被当成旧设备、上下出现黑边的问题。
- AT 通道错误提示改成人话（“连接被拒绝：host:port 上没有服务在监听”），不再显示系统英文原文。
- 新增“探测端口”（模块与网络 → 高级设置）：扫描模块地址上的常见 TCP 端口，结果写入终端，用来确认模块有没有对网络开放 AT 或管理服务。
- AT 通道未连接时，运营商/信号/注册/SIM 显示“—”，不再显示容易误导的占位值。

- 新增“探测 USB”（实验）：通过 IOKit 注册表枚举 USB 设备，并对找到的设备尝试 IOServiceOpen，用你这台 iPhone 的实测结果来验证“iOS 是否向 App 开放该模块”。预期结果是看不到设备，或打开被拒绝；如果显示 IOServiceOpen 成功，才说明文档里的 USB 直连思路在你的安装方式下可行，届时再实现 USB 传输层。

## 没有做的（以及原因）
没有加入“iPhone 直接读写模块 USB 串口/声卡”的代码：iOS 没有访问通用 USB 外设的公开接口（Apple 工程师在开发者论坛的回复），DriverKit 的 USB 仅限 iPadOS；TrollStore 只支持到 iOS 17.0，iPhone 17 无法使用。这条路在未越狱的 iPhone 上走不通。

# DJPhone v15.1 - Production-Hardened iOS Client for DJI 1st Gen 4G Module

## Key Fixes & Architecture Upgrades in v15.1:
1. **Dedicated `AT+CGMR` Barrier Probe, Single-Decrement Counting & 30s Orphan Expiry (Fixes Bug 1 & Bug 2)**:
   - Uses `AT+CGMR` as the barrier probe (never used by periodic status polling) so a late `+CSQ:` from a timed-out status poll never triggers orphan probe absorption or swallows `ATH`'s `OK`.
   - Decrements `unansweredBarrierProbeCount` only once per probe inside the barrier (on the `+CGMR:` / `EG25...` data line, without double-decrementing on its trailing `OK`).
   - Resets `unansweredOrphanProbesSent` on every `enterSyncBarrier()` and expires it after 30.0s.
2. **Lock-Screen-Writable Session Fallback Store (`.completeFileProtectionUntilFirstUserAuthentication`) (Fixes Bug 3)**:
   - Writes `djphone_sms_session_fallback.json` with `.completeFileProtectionUntilFirstUserAuthentication` so messages arriving while locked are persisted to disk immediately.
3. **Unlocked Main Store Corruption Self-Healing & Deduplicated Read-Only Fallback (Fixes Bug 4 & Bug 6)**:
   - When `isProtectedDataAvailable == true` (device unlocked) and both `djphone_sms_store.json` and backups fail to decode, quarantines the corrupted files to `.corrupt` and rebuilds the main store immediately (`loadFailedReadOnlyGuard = false`).
   - Deduplicates and caps messages at `maxCount` (500) before returning in the locked read-only fallback path.
4. **Two-Generation N-1 (`.bak`) & N-2 (`.bak2`) Backup Rotation with Change Check (Fixes Bug 5)**:
   - Rotates `.bak -> .bak2` and `canonical -> .bak` only when `prevItems != messages`, and supports recovery from `.bak2`.
5. **CI Stale-Version Check & 22-Suite Automated Test Runner (Fixes Bug 7)**:
   - Added explicit stale-version header verification step in `.github/workflows/build-ipa.yml` alongside `PASSED_COUNT >= 22` and mandatory title checks.
