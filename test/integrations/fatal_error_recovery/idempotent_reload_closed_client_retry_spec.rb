# frozen_string_literal: true

# Integration test that DETERMINISTICALLY exercises the closed-client-during-reload retry.
#
# When several threads share an idempotent producer with `reload_on_idempotent_fatal_error`, one
# thread's reload closes the shared librdkafka client while a sibling thread is still inside
# `client.produce`. Unlike `#close`/`#disconnect`, the idempotent reload does not drain the
# operations counter before closing, so it is the only path that closes the client with produces in
# flight. Racing with `@client.close`, the sibling calls into a client being torn down and
# librdkafka raises `Rdkafka::ClosedProducerError` (producer already flagged closed) or
# `Rdkafka::ClosedInnerError` (nil inner handle) - which one depends purely on how far `close` got.
# Neither is an `Rdkafka::RdkafkaError`, so before the fix they escaped every rescue and leaked out
# of `produce_sync` to the caller, defeating the transparent reload. The fix retries such a produce
# against the freshly reloaded client.
#
# The sibling `idempotent_concurrent_reload_spec` only hits this window by accident, under thread
# scheduling luck (that is exactly why it was flaky). This spec pins the exact interleaving instead:
#
#   1. Park one produce *inside* `client.produce` on the old client. We do this by wrapping ONLY the
#      external rdkafka client's `#produce` - no WaterDrop internals are stubbed. The wrapped call
#      announces it is inside produce and then blocks.
#   2. Reload the producer for real from another thread by injecting a librdkafka fatal error and
#      driving a produce through it. That closes the old client and rebuilds `@client`.
#   3. Release the parked produce. It now calls into the CLOSED old client, gets the closed-client
#      error, and must transparently recover against the freshly reloaded client and deliver.
#
# Without the fix the parked produce raises `ClosedProducerError`/`ClosedInnerError` out of
# `produce_sync` and the spec fails. With the fix it recovers and delivers.

require "waterdrop"
require "waterdrop/producer/testing"
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
  config.wait_backoff_on_idempotent_fatal_error = 10
  config.max_attempts_on_idempotent_fatal_error = 10
  config.logger = Logger.new($stdout, level: Logger::ERROR)
end

producer.singleton_class.include(WaterDrop::Producer::Testing)

topic = generate_topic("idem-reload-closed-client")

# Connect and confirm the producer is healthy, then capture the exact client object the producer is
# currently using. This is the object the reload will close out from under the parked produce.
producer.produce_sync(topic: topic, payload: "warmup")
old_client = producer.client

reloads = 0
producer.monitor.subscribe("producer.reloaded") { reloads += 1 }

entered_produce = Queue.new
release_produce = Queue.new
parked = false

# Wrap ONLY the external rdkafka client's `#produce`. The first call (the victim thread) announces it
# is inside produce and blocks until we have reloaded the producer out from under it. Every later
# call on this same object (the fatal-trigger produce below, issued before `@client` is swapped)
# runs straight through.
original_produce = old_client.method(:produce)
old_client.define_singleton_method(:produce) do |**kwargs|
  unless parked
    parked = true
    entered_produce << true
    release_produce.pop
  end

  original_produce.call(**kwargs)
end

victim_error = nil
victim_report = nil

victim = Thread.new do
  victim_report = producer.produce_sync(topic: topic, payload: "victim")
rescue => e
  # Any error here (in particular a leaked ClosedProducerError/ClosedInnerError) is a failure - the
  # produce should have transparently recovered.
  victim_error = e
end

# Wait until the victim is provably parked INSIDE `client.produce` on the old client.
entered_produce.pop

# Reload the producer for real: inject a fatal error and drive a produce from this thread. That
# produce enters the idempotent reload path, closes `old_client` and rebuilds `@client` against a
# fresh client. Because `parked` is already true, this produce is not intercepted by the wrapper.
producer.trigger_test_fatal_error(47, "Injected fatal to reload out from under the parked produce")
producer.produce_sync(topic: topic, payload: "trigger-reload")

# Sanity: the reload really happened and really closed the client the victim is holding, otherwise
# the test would prove nothing.
raise "expected a reload to happen before releasing the victim" if reloads.zero?
raise "expected the old client to be closed by the reload" unless old_client.closed?

# Release the victim. Its parked produce now runs against the CLOSED old client, which raises
# ClosedProducerError/ClosedInnerError. The fix must catch that, retry against the freshly reloaded
# client, and deliver the message.
release_produce << true
victim.join

producer.close

failures = []

if victim_error
  failures << "victim produce leaked #{victim_error.class}: #{victim_error.message}"
end

failures << "victim produce returned no delivery report" unless victim_report
failures << "no reload happened - the reload path was never exercised" if reloads.zero?

if failures.empty?
  puts "PASS: produce racing a concurrent idempotent reload recovered (#{reloads} reload(s))"
  exit(0)
else
  failures.each { |failure| warn "FAIL: #{failure}" }
  exit(1)
end
