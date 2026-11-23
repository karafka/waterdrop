# frozen_string_literal: true

# Integration test for a producer whose `bootstrap.servers` comes from an unset or empty ENV
# variable, or that has no kafka config at all.
#
# Its first `produce_sync` raises `WaterDrop::Errors::ConfigurationInvalidError` naming
# `kafka.bootstrap.servers` right away, instead of blocking until `message.timeout.ms` and failing
# like an unreachable broker. A producer with the `Buffered` client does not need the setting.

require "waterdrop"

# Upper bound for a fail-fast error; a missing broker list blocks for `message.timeout.ms`
MAX_TIME = 5

setups = {
  "nil" => ->(config) { config.kafka = { "bootstrap.servers": ENV["WATERDROP_UNSET_SERVERS"] } },
  "empty" => ->(config) { config.kafka = { "bootstrap.servers": "" } },
  "no kafka config" => ->(_config) {}
}

setups.each do |label, setup|
  producer = WaterDrop::Producer.new do |config|
    config.deliver = true
    setup.call(config)
  end

  started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  error = nil

  begin
    producer.produce_sync(topic: "bootstrap-servers-invalid", payload: "test")
  rescue => e
    error = e
  end

  elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at

  unless error.is_a?(WaterDrop::Errors::ConfigurationInvalidError)
    warn "FAIL: #{label}: expected ConfigurationInvalidError, got #{error.inspect}"
    exit(1)
  end

  unless error.message.include?("kafka.bootstrap.servers")
    warn "FAIL: #{label}: error does not name kafka.bootstrap.servers: #{error.message}"
    exit(1)
  end

  if elapsed > MAX_TIME
    warn "FAIL: #{label}: raised after #{elapsed.round(1)}s"
    exit(1)
  end

  producer.close
end

producer = WaterDrop::Producer.new do |config|
  config.client_class = WaterDrop::Clients::Buffered
  config.kafka = { "bootstrap.servers": ENV["WATERDROP_UNSET_SERVERS"] }
end

producer.produce_sync(topic: "bootstrap-servers-invalid", payload: "test")

unless producer.client.messages.size == 1
  warn "FAIL: Buffered client: expected 1 message, got #{producer.client.messages.size}"
  exit(1)
end

producer.close
