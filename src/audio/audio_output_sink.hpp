// Copyright 2025 Polymath Robotics, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <gst/gst.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <string>

#include "rclcpp/clock.hpp"
#include "utils/gstreamer_resources.hpp"
#include "utils/pipeline_failure_handler.hpp"

// The appsrc element is held as a plain GstElementPtr; appsrc call sites cast
// with GST_APP_SRC, so this header does not force gst-app includes on all
// transitive consumers.

namespace livekit_ros2_bridge::audio
{

// Wire name of the bridge-owned appsrc in the audio output playback pipeline. The
// startup validator reserves it so a sink fragment cannot define its own
// endpoint.
inline constexpr char kBridgeAppSrcName[] = "bridge_audio_out_src";

// Most audio appsrc holds before dropping the oldest, bounding added delay.
inline constexpr GstClockTime kAudioOutputMaxBacklog = 200 * GST_MSECOND;

// Builds the playback pipeline description: the bridge-owned appsrc, capped at
// kAudioOutputMaxBacklog, then audioconvert, audioresample, and the verbatim
// sink fragment. Shared with startup validation so the validated pipeline
// matches the one that runs.
std::string buildAudioOutputSinkPipelineDescription(const std::string & sink_fragment);

// PTS and duration for one interleaved S16 buffer.
struct AudioOutputBufferTiming
{
  GstClockTime pts;
  GstClockTime duration;
};

// Computes PTS/DURATION for one interleaved S16 buffer from its per-channel
// frame count. A non-positive channel count is treated as mono; a non-positive
// rate falls back to 48000 Hz.
AudioOutputBufferTiming computeAudioOutputBufferTiming(
  std::size_t sample_count, int channels, int sample_rate, GstClockTime next_pts);

// Turns off sync on every sink, including ones added later. A synced sink whose
// device delay exceeds its declared latency plays silence without an error.
void disableAudioOutputSinkSync(GstElement * pipeline);

// Abstract playback edge used by AudioOutputManager so its reader-handover and
// shutdown logic can be driven with a fake sink in tests. The concrete
// AudioOutputSink below is the production implementation.
class AudioOutputSinkInterface
{
public:
  virtual ~AudioOutputSinkInterface() = default;
  virtual bool bind(std::uint64_t reader_id, int sample_rate, int num_channels) = 0;
  virtual void push(std::uint64_t reader_id, const std::int16_t * samples, std::size_t count) = 0;
  virtual void unbind(std::uint64_t reader_id) = 0;
  virtual void stop() = 0;
};

// Plays received PCM through appsrc (capped, drops oldest) → audioconvert →
// audioresample → the configured sink fragment. The pipeline starts on the
// first frame, since appsrc caps come from that frame's rate and channels.
//
// Refcounted: each reader thread holds a copy, so the sink can outlive a reader.
// Frames arrive on reader threads and failures on GStreamer bus threads; one
// reader owns the sink at a time (see owner_).
class AudioOutputSink : public AudioOutputSinkInterface
{
public:
  explicit AudioOutputSink(std::string sink_fragment);
  ~AudioOutputSink() override;

  AudioOutputSink(const AudioOutputSink &) = delete;
  AudioOutputSink & operator=(const AudioOutputSink &) = delete;
  AudioOutputSink(AudioOutputSink &&) = delete;
  AudioOutputSink & operator=(AudioOutputSink &&) = delete;

  // Claims the sink (first caller wins) and starts the pipeline with this frame's
  // caps. Returns true when this reader owns the sink, even if the start failed.
  bool bind(std::uint64_t reader_id, int sample_rate, int num_channels) override;

  // Pushes one interleaved S16 frame. Non-owner frames and push failures are
  // logged and dropped.
  void push(std::uint64_t reader_id, const std::int16_t * samples, std::size_t count) override;

  // Releases the claim on reader finalize so the next output track can claim
  // on its first frame; the bridge does not know who published either track.
  // No-op when this reader did not own the sink.
  void unbind(std::uint64_t reader_id) override;

  // Stops the pipeline and disables restarts. Idempotent.
  void stop() override;

  // True from just before a pipeline is set to PLAYING until it is stopped.
  // Lock-free, so callers can observe lifecycle without contending on mutex_.
  bool hasActivePipeline() const;

  // Number of startPipelineLocked() invocations. The restart loop is
  // rate-bounded, so tests assert growth rather than absolute counts.
  std::size_t pipelineStartAttempts() const;

private:
  void startPipelineLocked();
  void stopPipelineLocked();
  void restartPipeline();
  void onBusMessage(GstMessage * message);
  void logIgnoredOnce(std::uint64_t reader_id);

  std::string sink_fragment_;

  // Guards pipeline_/appsrc_/caps state. The failure path (onBusMessage →
  // schedule) must stay lock-free against this mutex: the sync bus handler can
  // fire from inside startPipelineLocked() while a caller holds it.
  std::mutex mutex_;
  // Throttles the ~4/s restart-failure log while the device is gone.
  rclcpp::Clock log_clock_{RCL_STEADY_TIME};
  utils::GstElementPtr pipeline_;
  utils::GstElementPtr appsrc_element_;
  int caps_rate_ = 0;
  int caps_channels_ = 0;

  // PTS of the next buffer; reset to 0 on each pipeline start. Kept as a running
  // value so sample_count * GST_SECOND never overflows.
  GstClockTime next_pts_ = 0;

  // Counts pipeline starts, for tests.
  std::atomic<std::size_t> pipeline_start_attempts_{0};

  // Lock-free mirror of pipeline_ != nullptr, read by push().
  std::atomic<bool> pipeline_active_{false};

  // 0 = unclaimed; otherwise the owning reader_id. Claim/release only via CAS.
  std::atomic<std::uint64_t> owner_{0};
  std::atomic<bool> is_shutdown_{false};

  // Last non-owner that was logged, so each is logged once. max() is never a real id.
  std::atomic<std::uint64_t> last_ignored_reader_{std::numeric_limits<std::uint64_t>::max()};

  utils::PipelineFailureHandler failure_handler_;
};

}  // namespace livekit_ros2_bridge::audio
