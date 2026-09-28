// Copyright (c) 2025-present Polymath Robotics, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "audio/audio_output_sink.hpp"
#include "livekit/audio_stream.h"
#include "livekit/track.h"
#include "room_connection.hpp"

namespace livekit_ros2_bridge::audio
{

// Abstract pull-based decoded-PCM stream, mirroring the one livekit::AudioStream
// surface the audio output reader uses, so reader-handover and shutdown can be
// tested without a real LiveKit track.
class AudioOutputStream
{
public:
  virtual ~AudioOutputStream() = default;
  virtual bool read(livekit::AudioFrameEvent & out_event) = 0;
  virtual void close() = 0;
};

using AudioOutputStreamFactory = std::function<std::shared_ptr<AudioOutputStream>(
  const std::shared_ptr<livekit::Track> & track, std::size_t capacity)>;

// Plays the audio output track: subscribes to `lkros.audio.out` by name, reads
// decoded PCM on one thread per track, and feeds the playback sink. Created only
// when `audio.out.sink` is set. Readers stop on unsubscribe, unpublish,
// subscription failure, participant disconnect, and shutdown; see onConnected()
// for why reconnects leave them running.
class AudioOutputManager
{
public:
  AudioOutputManager(RoomConnection & room_connection, std::string sink_fragment);
  AudioOutputManager(
    RoomConnection & room_connection,
    std::shared_ptr<AudioOutputSinkInterface> sink,
    AudioOutputStreamFactory stream_factory);
  ~AudioOutputManager();

  AudioOutputManager(const AudioOutputManager &) = delete;
  AudioOutputManager & operator=(const AudioOutputManager &) = delete;
  AudioOutputManager(AudioOutputManager &&) = delete;
  AudioOutputManager & operator=(AudioOutputManager &&) = delete;

  // Room event handlers. Every public handler serializes on event_mutex_ so the
  // reader map's check-then-act in subscribeOutputTrack cannot interleave.
  void onRemoteTrackPublished(const RemoteTrackEvent & event);
  void onRemoteTrackUnpublished(const RemoteTrackEvent & event);
  void onRemoteTrackSubscribed(const RemoteTrackEvent & event);
  void onRemoteTrackUnsubscribed(const RemoteTrackEvent & event);
  void onRemoteTrackSubscriptionFailed(const RemoteTrackSubscriptionFailedEvent & event);
  void onParticipantDisconnected(const livekit::ParticipantDisconnectedEvent & event);

  // Runs from on_remote_tracks_ready and subscribes every output-named track
  // the snapshot reports as unsubscribed, including ones a full restart
  // re-announced while Reconnecting. Running readers are left alone. Idempotent.
  void onConnected();

private:
  struct Reader
  {
    std::string participant_identity;
    std::string track_sid;
    std::atomic<bool> stop{false};
    std::shared_ptr<AudioOutputStream> stream;
  };

  void subscribeOutputTrack(const RemoteTrackEvent & event);
  void stopReader(const std::string & track_sid, const char * reason);
  void snapshotSubscribe();

  RoomConnection & room_connection_;
  std::shared_ptr<AudioOutputSinkInterface> sink_;
  AudioOutputStreamFactory stream_factory_;

  // Serializes public handlers; taken once at the top of each handler and never
  // recursively. stopReader() and the reader map use mutex_, which may be taken
  // while event_mutex_ is held.
  std::mutex event_mutex_;
  std::mutex mutex_;
  std::map<std::string, std::shared_ptr<Reader>> readers_;
  std::atomic<std::uint64_t> last_reader_id_{0};
  std::atomic<bool> is_shutdown_{false};

  // The destructor waits for live_readers_ to reach zero, so no reader thread is
  // still inside LiveKit when the SDK shuts down. Each reader drops its references,
  // then decrements and notifies under wait_mutex_ as its last act.
  std::atomic<std::size_t> live_readers_{0};
  std::mutex wait_mutex_;
  std::condition_variable reader_exited_;
};

}  // namespace livekit_ros2_bridge::audio
