# frozen_string_literal: true

# When a producer is used before forking (pre-fork producing), the child inherits the parent's
# Poller singleton state including its @producers map and @thread reference. After fork, the
# parent's poller thread is dead in the child. Without the ensure_same_process! fix in
# Poller#unregister, calling producer.close in the child would deadlock: unregister would find
# the parent's stale state, call state.wait_for_close, and block forever waiting on a latch
# the dead poller thread would never release.
#
# This test verifies that:
#   1. A pre-fork producer can be closed in the child without hanging
#   2. A new producer can be created and used in the child after closing the inherited one
#   3. The child completes within a reasonable time (no deadlock)

require "waterdrop"
require "logger"
require "securerandom"
require "timeout"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
CHILD_TIMEOUT = 15

topic_name = generate_topic("unregister-fork")

# Create and USE the producer before forking. This is critical: the producer must be fully
# initialized (rdkafka client created, registered with Poller, poller thread running) so
# the child inherits all that stale state.
producer = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": BOOTSTRAP_SERVERS
  }
  config.logger = Logger.new($stdout, level: Logger::INFO)
end

producer.produce_sync(topic: topic_name, payload: "pre-fork-message")

# Give the poller thread time to fully start and register the producer
sleep(0.5)

reader, writer = IO.pipe

pid = fork do
  reader.close

  # In the child, the parent's poller thread is dead. Closing the producer triggers
  # Poller#unregister. Without the fix, this hangs forever.
  begin
    producer.close
    writer.puts("close:ok")
  rescue => e
    writer.puts("close:error:#{e.class}:#{e.message}")
  end

  # Verify we can create a fresh producer and use it in the child
  begin
    new_producer = WaterDrop::Producer.new do |config|
      config.kafka = {
        "bootstrap.servers": BOOTSTRAP_SERVERS
      }
      config.logger = Logger.new($stdout, level: Logger::INFO)
    end

    new_producer.produce_sync(topic: topic_name, payload: "post-fork-message")
    writer.puts("produce:ok")
    new_producer.close
  rescue => e
    writer.puts("produce:error:#{e.class}:#{e.message}")
  end

  writer.close
  exit!(0)
end

writer.close

# Wait for the child with a timeout. If the bug is present, the child hangs forever
# on producer.close, so this timeout is the deadlock detector.
begin
  Timeout.timeout(CHILD_TIMEOUT) do
    Process.waitpid(pid)
  end
rescue Errno::ECHILD
  # Already reaped, that's fine
rescue Timeout::Error
  # Child is hung — the bug is present
  Process.kill("KILL", pid)

  begin
    Process.waitpid(pid)
  rescue Errno::ECHILD
    nil
  end

  warn "FAIL: Child process hung for #{CHILD_TIMEOUT}s (deadlock in Poller#unregister)"
  exit(1)
end

# Parse child results
results = {}

while (line = reader.gets)
  key, status, *rest = line.strip.split(":")
  results[key] = { status: status, detail: rest.join(":") }
end

reader.close

# Close the parent producer
producer.close

# Validate results
if results["close"]&.fetch(:status) != "ok"
  warn "FAIL: producer.close in child failed: #{results["close"]}"
  exit(1)
end

if results["produce"]&.fetch(:status) != "ok"
  warn "FAIL: new producer in child failed: #{results["produce"]}"
  exit(1)
end

warn "PASS: Pre-fork producer closed without deadlock, new producer works in child"
exit(0)
