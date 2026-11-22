# frozen_string_literal: true

# When an FD polling producer disconnects or closes, librdkafka must not write its queue wakeup
# byte into a file that reuses the queue pipe fd number. The poller closes the pipe before the
# native client is destroyed, and librdkafka keeps writing on queue ops (statistics, errors)
# until its IO events are disabled.
#
# The `poller.producer_unregistered` listener runs after the pipe is closed and before the
# client is destroyed. It opens files, which get the lowest free fd numbers, so the freed pipe
# fds among them, and waits for a few statistics intervals. All of those files must stay empty.
# After a disconnect, the producer reconnects and produces again.

require "waterdrop"
require "tempfile"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
FILES_COUNT = 16

files = []
armed = false

producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "statistics.interval.ms": 100
  }
  config.polling.mode = :fd
end

# Statistics are enabled only when someone listens before the client is built
producer.monitor.subscribe("statistics.emitted") { |_event| nil }

producer.monitor.subscribe("poller.producer_unregistered") do |_event|
  next unless armed

  FILES_COUNT.times { files << Tempfile.new("stale-fd") }

  # Give librdkafka time to put a few statistics ops on the queue
  sleep(0.5)
end

# Runs the action with the listener armed and checks the files it opened
check_action = lambda do |name, &action|
  armed = true
  result = action.call
  armed = false

  polluted = files.reject { |file| File.empty?(file.path) }

  unless files.size == FILES_COUNT
    warn "FAIL: #{name}: expected #{FILES_COUNT} files opened between pipe close and destroy, got #{files.size}"
    exit(1)
  end

  unless polluted.empty?
    warn "FAIL: #{name}: #{polluted.size} files got a byte from librdkafka"
    exit(1)
  end

  files.each(&:close!)
  files.clear

  result
end

topic = generate_topic("stale-fd")

producer.produce_sync(topic: topic, payload: "x")

disconnected = check_action.call("disconnect") { producer.disconnect }
unless disconnected
  warn "FAIL: disconnect: producer did not disconnect"
  exit(1)
end

report = producer.produce_sync(topic: topic, payload: "y")
unless report.error.nil? && report.offset >= 0
  warn "FAIL: disconnect: producer did not produce after reconnect (#{report.error.inspect}, offset #{report.offset})"
  exit(1)
end

check_action.call("close") { producer.close }
