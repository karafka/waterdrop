# frozen_string_literal: true

# When an FD polling producer closes, librdkafka must not write its queue wakeup byte into a
# file that reuses the queue pipe fd number. The poller closes the pipe before the native client
# is destroyed, and librdkafka keeps writing on queue ops (statistics, errors) until its IO
# events are disabled.
#
# The `poller.producer_unregistered` listener runs after the pipe is closed and before the
# client is destroyed. It opens files, which get the lowest free fd numbers, so the freed pipe
# fds among them, and waits for a few statistics intervals. All of those files must stay empty.

require "waterdrop"
require "tempfile"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
FILES_COUNT = 16

def check(failed, condition, message)
  if condition
    puts "ok: #{message}"
    failed
  else
    puts "FAIL: #{message}"
    true
  end
end

failed = false
files = []

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
  FILES_COUNT.times { files << Tempfile.new("stale-fd") }

  # Give librdkafka time to put a few statistics ops on the queue
  sleep(0.5)
end

producer.produce_sync(topic: generate_topic("stale-fd"), payload: "x")
producer.close

polluted = files.reject { |file| File.empty?(file.path) }

failed = check(failed, files.size == FILES_COUNT, "files were opened between pipe close and destroy")
failed = check(failed, polluted.empty?, "no file got a byte from librdkafka (#{polluted.size} did)")

files.each(&:close!)

exit(failed ? 1 : 0)
