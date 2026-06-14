# frozen_string_literal: true

# Integration test reproducing the critical recursive-lock deadlock that happened when closing an
# idempotent producer (with `reload_on_idempotent_fatal_error` enabled) that still has buffered
# messages, where the final flush surfaces a fatal librdkafka error.
#
# `#close` holds `@operating_mutex` while it performs the final `flush`. The buffered message is
# (re)dispatched from that flush and hits a fatal error. The idempotent fatal-error reload path then
# called `@operating_mutex.synchronize` AGAIN on the same thread, which Ruby rejects with
# `ThreadError: deadlock; recursive locking`. `#close` re-raised that error and left the producer
# stuck in the `:closing` state with the native client never closed (native threads + pipe FDs
# leaked, callbacks never removed).
#
# After the fix, `#close` must complete cleanly: no `ThreadError`, the producer ends up `:closed`,
# the underlying client is torn down, and the producer rejects further use.

require "waterdrop"
require "logger"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

producer = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "enable.idempotence": true,
    "request.required.acks": "all"
  }
  config.max_wait_timeout = 5_000
  config.reload_on_idempotent_fatal_error = true
  config.wait_backoff_on_idempotent_fatal_error = 100
  config.max_attempts_on_idempotent_fatal_error = 3
  config.logger = Logger.new($stdout, level: Logger::WARN)
end

# Force the client to connect so we can stub its `#produce`.
client = producer.client

# Inject a fatal error on every produce. The buffered message hits this during the close flush,
# which is exactly the path that used to recursively lock `@operating_mutex`.
client.define_singleton_method(:produce) do |**_kwargs|
  error = Rdkafka::RdkafkaError.new(-138, "Simulated idempotent fatal error")
  error.define_singleton_method(:fatal?) { true }
  raise error
end

# Buffer a message so that `#close` performs a final flush that triggers the fatal-error path.
producer.buffer(topic: generate_topic("idem-close-deadlock"), payload: "data")

# Run close in a watchdog thread so both the recursive-lock ThreadError and any hang are observable
# and time-bounded instead of wedging the whole spec.
close_error = nil
closer = Thread.new do
  producer.close
rescue => e
  close_error = e
end

completed = closer.join(30)

failures = []

if completed.nil?
  failures << "close() hung (did not return within 30s) - producer is deadlocked"
  closer.kill
end

if close_error
  failures << "close() raised #{close_error.class}: #{close_error.message}"
end

failures << "producer status is #{producer.status} (expected closed)" unless producer.status.closed?

# A properly closed producer must reject further use rather than be wedged mid-close.
begin
  producer.produce_sync(topic: "post-close", payload: "y")
  failures << "producer accepted a produce after close (expected ProducerClosedError)"
rescue WaterDrop::Errors::ProducerClosedError
  # expected - producer is fully closed
rescue => e
  failures << "post-close produce raised #{e.class} (expected ProducerClosedError)"
end

if failures.empty?
  puts "PASS: idempotent producer closed cleanly despite a fatal error during the final flush"
  exit(0)
else
  failures.each { |failure| warn "FAIL: #{failure}" }
  exit(1)
end
