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

import AVFAudio
import Foundation

/// A buffer that captures audio before connecting to the server.
@objcMembers
public final class PreConnectAudioBuffer: NSObject, Sendable, Loggable {
    public typealias OnError = @Sendable (Error) -> Void

    public enum Constants {
        public static let maxSize = 10 * 1024 * 1024 // 10MB
        public static let sampleRate = 24000
        public static let timeout: TimeInterval = 10

        /// How long ``sendAudioData(to:agents:on:)`` waits for the recorder track's
        /// sid to be assigned by the publish ack before sending without it.
        public static let trackSidTimeout: TimeInterval = 5
        /// Interval between track sid availability checks.
        static let trackSidPollInterval: TimeInterval = 0.02
    }

    /// The default data topic used to send the audio buffer.
    public static let dataTopic = "lk.agent.pre-connect-audio-buffer"

    /// The room instance to send the audio buffer to.
    public var room: Room? { state.room }

    /// The audio recorder instance.
    public var recorder: LocalAudioTrackRecorder? { state.recorder }

    private let state = StateSync<State>(State())
    private struct State {
        weak var room: Room?
        var recorder: LocalAudioTrackRecorder?
        var audioStream: LocalAudioTrackRecorder.Stream?
        var timeoutTask: AnyTaskCancellable?
        var sent: Bool = false
        var onError: OnError?
    }

    /// Initialize the audio buffer with a room instance.
    /// - Parameters:
    ///   - room: The room instance to send the audio buffer to.
    ///   - onError: The error handler to call when an error occurs while sending the audio buffer.
    public init(room: Room?, onError: OnError? = nil) {
        state.mutate {
            $0.room = room
            $0.onError = onError
        }
        super.init()
    }

    deinit {
        stopRecording()
    }

    public func setErrorHandler(_ onError: OnError?) {
        state.mutate { $0.onError = onError }
    }

    /// Start capturing audio.
    /// - Parameters:
    ///   - timeout: The timeout for the remote participant to subscribe to the audio track.
    /// The room connection needs to be established and the remote participant needs to subscribe to the audio track
    /// before the timeout is reached. Otherwise, the audio stream will be flushed without sending.
    ///   - recorder: Optional custom recorder instance. If not provided, a new one will be created.
    public func startRecording(timeout: TimeInterval = Constants.timeout, recorder: LocalAudioTrackRecorder? = nil) async throws {
        room?.add(delegate: self)

        let roomOptions = room?._state.roomOptions
        let newRecorder = recorder ?? LocalAudioTrackRecorder(
            track: LocalAudioTrack.createTrack(options: roomOptions?.defaultAudioCaptureOptions,
                                               reportStatistics: roomOptions?.reportRemoteTrackStatistics ?? false),
            format: .pcmFormatInt16,
            sampleRate: Constants.sampleRate,
            maxSize: Constants.maxSize,
        )

        let stream = try await newRecorder.start()
        log("Started capturing audio", .info)

        state.mutate { state in
            state.recorder = newRecorder
            state.audioStream = stream
            state.timeoutTask = Task { [weak self] in
                try await Task.sleep(nanoseconds: UInt64(timeout) * NSEC_PER_SEC)
                try Task.checkCancellation()
                self?.stopRecording(flush: true)
            }.cancellable()
            state.sent = false
        }
    }

    /// Stop capturing audio.
    /// - Parameters:
    ///   - flush: If `true`, the audio stream will be flushed immediately without sending.
    public func stopRecording(flush: Bool = false) {
        guard let recorder, recorder.isRecording else { return }

        recorder.stop()
        log("Stopped capturing audio", .info)

        if flush, let stream = state.audioStream {
            log("Flushing audio stream", .info)
            Task {
                for await _ in stream {}
            }
            room?.remove(delegate: self)
        }
    }

    /// Send the audio data to the room.
    /// - Parameters:
    ///   - room: The room instance to send the audio data.
    ///   - agents: The agents to send the audio data to.
    ///   - topic: The topic to send the audio data.
    public func sendAudioData(to room: Room, agents: [Participant.Identity], on topic: String = dataTopic) async throws {
        guard !agents.isEmpty else { return }

        guard !state.sent else { return }
        state.mutate { $0.sent = true }

        guard let recorder else {
            throw LiveKitError(.invalidState, message: "Recorder is nil")
        }

        guard let audioStream = state.audioStream else {
            throw LiveKitError(.invalidState, message: "Audio stream is nil")
        }

        // The recorder's track only receives its sid once the publish is acked by
        // the server (`LocalParticipant.add(publication:)`). The agent can reach
        // `.active` before that ack lands — on a slow publish (e.g. Bluetooth HFP
        // mic warm-up) — and this send is one-shot, so stamping the sid eagerly
        // would emit `trackId: ""` with no chance to correct it. Agents discard a
        // buffer they can't correlate to a track, so wait for the sid first.
        let trackId = await waitForTrackSid(of: recorder)
        if trackId == nil {
            log("Track sid unavailable after \(Constants.trackSidTimeout)s, sending without trackId", .warning)
        }

        let streamOptions = StreamByteOptions(
            topic: topic,
            attributes: [
                "sampleRate": "\(recorder.sampleRate)",
                "channels": "\(recorder.channels)",
                "trackId": trackId ?? "",
            ],
            destinationIdentities: agents,
        )
        let writer = try await room.localParticipant.streamBytes(options: streamOptions)

        var sentSize = 0
        for await chunk in audioStream {
            do {
                try await writer.write(chunk)
            } catch {
                try await writer.close(reason: error.localizedDescription)
                throw error
            }
            sentSize += chunk.count
        }
        try await writer.close()

        log("Sent \(recorder.duration(sentSize))s = \(sentSize / 1024)KB of audio data to \(agents.count) agent(s) \(agents) trackId: \(trackId ?? "<none>")", .info)
    }

    /// Poll for the recorder track's sid until it is assigned by the publish ack.
    /// - Returns: The sid, or `nil` if it did not arrive within
    /// ``Constants/trackSidTimeout`` (or the task was cancelled).
    private func waitForTrackSid(of recorder: LocalAudioTrackRecorder) async -> String? {
        if let sid = recorder.track.sid?.stringValue { return sid }

        let sleepNanos = UInt64(Constants.trackSidPollInterval * Double(NSEC_PER_SEC))
        let deadline = Date().addingTimeInterval(Constants.trackSidTimeout)
        while Date() < deadline {
            do {
                try await Task.sleep(nanoseconds: sleepNanos)
            } catch {
                return recorder.track.sid?.stringValue // cancelled
            }
            if let sid = recorder.track.sid?.stringValue { return sid }
        }
        return nil
    }
}

extension PreConnectAudioBuffer: RoomDelegate {
    public func room(_: Room, participant _: LocalParticipant, remoteDidSubscribeTrack _: LocalTrackPublication) {
        log("Subscribed by remote participant, stopping audio", .info)
        stopRecording()
    }

    public func room(_ room: Room, participant: Participant, didUpdateState state: ParticipantState) {
        guard participant.kind == .agent, state == .active, let agent = participant.identity else { return }
        log("Detected active agent participant: \(agent), sending audio", .info)

        Task {
            do {
                try await sendAudioData(to: room, agents: [agent])
            } catch {
                log("Unable to send preconnect audio: \(error)", .error)
                self.state.onError?(error)
            }
        }
    }
}
