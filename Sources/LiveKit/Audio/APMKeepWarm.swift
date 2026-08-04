/*
 * Copyright 2026 LiveKit
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Foundation

internal import LiveKitWebRTC

/// Keeps the shared, factory-scoped WebRTC Audio Processing Module (APM) **capture pipeline warm
/// without an SFU / room**, by running a self-contained **local loopback** peer connection whose
/// sole job is to hold an audio send stream in a negotiated, connected state.
///
/// ## Why this exists
/// `capturePostProcessingDelegate` is wired to the shared APM (`RTC.audioProcessingModule`), and
/// the APM's capture callback only runs while an audio **sender** is pulling the mic source through
/// it. `startLocalRecording()` warms the ADM (mic engine) but does NOT create a sender, so at rest
/// the capture callback never fires — wake-word spotting and any pre-connect capture are dark until
/// a real room publishes the mic (~1.7s of APM (re)init at connect).
///
/// Because there is exactly **one** APM (owned by the single `peerConnectionFactory`,
/// `RTC.swift:47`), a throwaway loopback sender created from that same factory warms the **identical**
/// capture path the real `Room` connect later reuses. So:
/// - the capture callback delivers real mic frames at rest (wake word + pre-connect buffer work),
/// - and the real mic publish becomes a no-op APM reconfigure (no connect-time spin-up).
///
/// Two local peer connections (offerer holds the mic track; answerer receives) exchange
/// offer/answer + **host** ICE candidates over loopback — no STUN/TURN, no network egress, no
/// server. Received media is discarded. This is room-free: nothing connects to LiveKit.
///
/// Lifecycle: call ``start()`` once the audio session is up (e.g. app launch, after
/// `AudioManager` is configured) and ``stop()`` to tear it down. Idempotent.
/// Public entry point for app code. The `APMKeepWarm` class itself is internal because it conforms
/// to the internally-imported `LKRTCPeerConnectionDelegate`, which can't appear in a public API.
public enum APMKeepWarmControl {
    /// Start the room-free loopback that warms the shared APM capture pipeline. Idempotent.
    public static func start() { APMKeepWarm.shared.start() }
    /// Tear the loopback down.
    public static func stop() { APMKeepWarm.shared.stop() }
    /// Whether the warmer is currently running.
    public static var isRunning: Bool { APMKeepWarm.shared.isRunning }
}

final class APMKeepWarm: NSObject, @unchecked Sendable {
    static let shared = APMKeepWarm()

    private let lock = NSLock()
    private var running = false
    private var offerer: LKRTCPeerConnection?
    private var answerer: LKRTCPeerConnection?
    private var audioSource: LKRTCAudioSource?
    private var audioTrack: LKRTCAudioTrack?

    /// Scoped critical section usable from async contexts (bare `NSLock.lock()` is banned in async).
    private func locked<T>(_ body: () -> T) -> T {
        lock.lock(); defer { lock.unlock() }
        return body()
    }

    var isRunning: Bool { locked { running } }

    /// Bring up the loopback sender that warms the shared APM. No-op if already running.
    func start() {
        let shouldStart: Bool = locked {
            if running { return false }
            running = true
            return true
        }
        guard shouldStart else { return }
        Task { [weak self] in await self?.bringUp() }
    }

    /// Tear down the loopback sender.
    func stop() {
        let (o, a): (LKRTCPeerConnection?, LKRTCPeerConnection?) = locked {
            running = false
            let pair = (offerer, answerer)
            offerer = nil; answerer = nil; audioTrack = nil; audioSource = nil
            return pair
        }
        o?.close()
        a?.close()
        print("[APMKeepWarm] stopped")
    }

    // MARK: - Bring-up

    private func bringUp() async {
        let config = DispatchQueue.liveKitWebRTC.sync { LKRTCConfiguration() }
        config.sdpSemantics = .unifiedPlan
        config.iceServers = []                 // host candidates only → local loopback, no network
        config.continualGatheringPolicy = .gatherOnce

        guard let o = RTC.createPeerConnection(config, constraints: .defaultPCConstraints),
              let a = RTC.createPeerConnection(config, constraints: .defaultPCConstraints)
        else {
            print("[APMKeepWarm] failed to create peer connections")
            return
        }
        o.delegate = self
        a.delegate = self

        // The audio SOURCE is backed by the ADM mic capture; attaching it to a negotiated sender is
        // what pulls the mic through the shared APM (and thus fires `capturePostProcessingDelegate`).
        let src = RTC.createAudioSource(nil)
        let track = RTC.createAudioTrack(source: src)
        track.isEnabled = true

        let tInit = DispatchQueue.liveKitWebRTC.sync { LKRTCRtpTransceiverInit() }
        tInit.direction = .sendOnly
        guard o.addTransceiver(with: track, init: tInit) != nil else {
            print("[APMKeepWarm] failed to add transceiver")
            o.close(); a.close()
            return
        }

        let stored: Bool = locked {
            guard running else { return false }
            offerer = o; answerer = a; audioSource = src; audioTrack = track
            return true
        }
        guard stored else { o.close(); a.close(); return }

        do {
            // Loopback offer/answer. ICE candidates are cross-fed in the delegate; with only host
            // candidates the two PCs connect locally (no network), which starts the send stream.
            let offer = try await Self.offer(o)
            try await Self.setLocal(o, offer)
            try await Self.setRemote(a, offer)
            let answer = try await Self.answer(a)
            try await Self.setLocal(a, answer)
            try await Self.setRemote(o, answer)
            print("[APMKeepWarm] loopback negotiated — warming shared APM (no SFU)")
        } catch {
            print("[APMKeepWarm] negotiation failed: \(error)")
        }
    }

    // MARK: - SDP async wrappers (mirror Transport)

    private static func offer(_ pc: LKRTCPeerConnection) async throws -> LKRTCSessionDescription {
        let constraints = LKRTCMediaConstraints(mandatoryConstraints: nil, optionalConstraints: nil)
        return try await withCheckedThrowingContinuation { cont in
            pc.offer(for: constraints) { sd, error in
                if let error { cont.resume(throwing: error) }
                else if let sd { cont.resume(returning: sd) }
                else { cont.resume(throwing: LiveKitError(.invalidState, message: "no offer sdp")) }
            }
        }
    }

    private static func answer(_ pc: LKRTCPeerConnection) async throws -> LKRTCSessionDescription {
        let constraints = LKRTCMediaConstraints(mandatoryConstraints: nil, optionalConstraints: nil)
        return try await withCheckedThrowingContinuation { cont in
            pc.answer(for: constraints) { sd, error in
                if let error { cont.resume(throwing: error) }
                else if let sd { cont.resume(returning: sd) }
                else { cont.resume(throwing: LiveKitError(.invalidState, message: "no answer sdp")) }
            }
        }
    }

    private static func setLocal(_ pc: LKRTCPeerConnection, _ sd: LKRTCSessionDescription) async throws {
        try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
            pc.setLocalDescription(sd) { error in
                if let error { cont.resume(throwing: error) } else { cont.resume() }
            }
        }
    }

    private static func setRemote(_ pc: LKRTCPeerConnection, _ sd: LKRTCSessionDescription) async throws {
        try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
            pc.setRemoteDescription(sd) { error in
                if let error { cont.resume(throwing: error) } else { cont.resume() }
            }
        }
    }
}

// MARK: - LKRTCPeerConnectionDelegate

extension APMKeepWarm: LKRTCPeerConnectionDelegate {
    // Cross-feed ICE candidates to the OTHER peer so the loopback connects locally.
    nonisolated func peerConnection(_ pc: LKRTCPeerConnection, didGenerate candidate: LKRTCIceCandidate) {
        lock.lock()
        let other = (pc === offerer) ? answerer : offerer
        lock.unlock()
        other?.add(candidate) { error in
            if let error { print("[APMKeepWarm] add ICE failed: \(error)") }
        }
    }

    nonisolated func peerConnection(_ pc: LKRTCPeerConnection, didChange state: LKRTCPeerConnectionState) {
        // Log the offerer reaching `.connected` — that's when the send stream is live and the APM is warm.
        lock.lock()
        let isOfferer = (pc === offerer)
        lock.unlock()
        if isOfferer {
            print("[APMKeepWarm] offerer connectionState → \(state.rawValue) (2=connected)")
        }
    }

    // Required no-ops.
    nonisolated func peerConnectionShouldNegotiate(_: LKRTCPeerConnection) {}
    nonisolated func peerConnection(_: LKRTCPeerConnection, didChange _: LKRTCSignalingState) {}
    nonisolated func peerConnection(_: LKRTCPeerConnection, didChange _: LKRTCIceConnectionState) {}
    nonisolated func peerConnection(_: LKRTCPeerConnection, didChange _: LKRTCIceGatheringState) {}
    nonisolated func peerConnection(_: LKRTCPeerConnection, didAdd _: LKRTCMediaStream) {}
    nonisolated func peerConnection(_: LKRTCPeerConnection, didRemove _: LKRTCMediaStream) {}
    nonisolated func peerConnection(_: LKRTCPeerConnection, didRemove _: [LKRTCIceCandidate]) {}
    nonisolated func peerConnection(_: LKRTCPeerConnection, didOpen _: LKRTCDataChannel) {}
}
