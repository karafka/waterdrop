# frozen_string_literal: true

# Integration test for the `oauth.token_provider_listener` "not set" values.
#
# A producer configured with `nil` or `false` as `oauth.token_provider_listener` passes the
# config validation, subscribes no oauth listener and delivers messages.

require "waterdrop"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

topic = generate_topic("oauth-listener-nil")

[nil, false].each_with_index do |listener, index|
  producer = WaterDrop::Producer.new do |config|
    config.deliver = true
    config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    config.oauth.token_provider_listener = listener
  end

  offset = producer.produce_sync(topic: topic, payload: listener.inspect).offset

  unless offset == index
    warn "FAIL: #{listener.inspect}: expected offset #{index}, got #{offset}"
    exit(1)
  end

  producer.close
end
