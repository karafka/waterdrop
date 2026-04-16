# frozen_string_literal: true

# Integration test for resource cleanup when init_transactions fails.
#
# When a transactional producer's init_transactions call fails (e.g., Kafka is unreachable),
# the native rdkafka client must be properly destroyed. Without cleanup, each failed attempt
# permanently leaks:
#   - Native threads (main thread + broker threads from the started rd_kafka_t)
#   - Pipe file descriptors (one pair per broker thread)
#   - Global instrumentation callback entries
#
# This test creates transactional producers pointing at a non-routable broker so that
# init_transactions always times out, then verifies that native threads, pipe FDs, and
# callback registry entries return to baseline after each failed attempt.

require "waterdrop"
require "logger"
require "securerandom"

LINUX = RUBY_PLATFORM.include?("linux")

unless LINUX
  warn "SKIP: This test requires Linux /proc filesystem for thread and FD counting"
  exit(0)
end

ATTEMPTS = 5

def native_thread_count
  Dir.glob("/proc/self/task/*").size
end

def pipe_fd_count
  Dir.glob("/proc/self/fd/*").count do |fd_path|
    File.readlink(fd_path).start_with?("pipe:")
  rescue Errno::ENOENT
    false
  end
end

def callbacks_size
  stats = ::Karafka::Core::Instrumentation
    .statistics_callbacks
    .instance_variable_get(:@callbacks).size
  errors = ::Karafka::Core::Instrumentation
    .error_callbacks
    .instance_variable_get(:@callbacks).size
  oauth = ::Karafka::Core::Instrumentation
    .oauthbearer_token_refresh_callbacks
    .instance_variable_get(:@callbacks).size

  stats + errors + oauth
end

baseline_threads = native_thread_count
baseline_pipes = pipe_fd_count
baseline_callbacks = callbacks_size

warn "Baseline: threads=#{baseline_threads} pipes=#{baseline_pipes} callbacks=#{baseline_callbacks}"

ATTEMPTS.times do |i|
  producer = WaterDrop::Producer.new do |config|
    config.kafka = {
      # Non-routable address so init_transactions times out
      "bootstrap.servers": "127.0.0.1:19091",
      "transactional.id": "leak-test-#{i}-#{SecureRandom.hex(4)}",
      "transaction.timeout.ms": 5_000,
      "message.timeout.ms": 5_000,
      "socket.timeout.ms": 1_000,
      "metadata.request.timeout.ms": 1_000,
      "socket.connection.setup.timeout.ms": 1_000
    }
    config.max_wait_timeout = 10_000
    config.logger = Logger.new("/dev/null")
  end

  begin
    producer.client
    warn "  Attempt #{i + 1}: init_transactions succeeded (unexpected)"
  rescue StandardError => e
    warn "  Attempt #{i + 1}: failed as expected (#{e.class})"
  end

  producer.close

  # Brief pause to let any thread cleanup finish
  sleep(0.2)
end

# Force GC to rule out finalizer-based cleanup masking the leak
GC.start
GC.start
sleep(0.5)

final_threads = native_thread_count
final_pipes = pipe_fd_count
final_callbacks = callbacks_size

warn "Final: threads=#{final_threads} pipes=#{final_pipes} callbacks=#{final_callbacks}"

thread_growth = final_threads - baseline_threads
pipe_growth = final_pipes - baseline_pipes
callback_growth = final_callbacks - baseline_callbacks

failed = false

# Allow a small tolerance for threads (Ruby's own background threads can fluctuate by 1-2)
if thread_growth > 2
  warn "FAIL: Native threads grew by #{thread_growth} (#{baseline_threads} -> #{final_threads})"
  failed = true
end

if pipe_growth > 0
  warn "FAIL: Pipe FDs grew by #{pipe_growth} (#{baseline_pipes} -> #{final_pipes})"
  failed = true
end

if callback_growth > 0
  warn "FAIL: Callbacks grew by #{callback_growth} (#{baseline_callbacks} -> #{final_callbacks})"
  failed = true
end

if failed
  warn "FAIL: Resources leaked after #{ATTEMPTS} failed init_transactions attempts"
  exit(1)
else
  warn "PASS: No resource leaks detected after #{ATTEMPTS} failed init_transactions attempts"
  exit(0)
end
