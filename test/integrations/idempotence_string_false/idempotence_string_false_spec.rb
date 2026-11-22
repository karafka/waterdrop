# frozen_string_literal: true

# A producer with `enable.idempotence` set to the String "false" is not idempotent: it may use an
# `acks: 0` variant. With the String "true" it is idempotent and refuses such a variant.

require "waterdrop"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

def build_producer(idempotence)
  WaterDrop::Producer.new do |config|
    config.deliver = true
    config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS, "enable.idempotence": idempotence }
  end
end

topic = generate_topic("idempotence-string-false")

producer = build_producer("false")

unless producer.idempotent? == false
  warn 'FAIL: "false" producer is not idempotent'
  exit(1)
end

begin
  report = producer.variant(topic_config: { acks: 0 }).produce_sync(topic: topic, payload: "x")

  unless report.offset.negative?
    warn 'FAIL: "false" producer delivers via an acks: 0 variant'
    exit(1)
  end
rescue WaterDrop::Errors::VariantInvalidError => e
  warn %(FAIL: "false" producer refused an acks: 0 variant: #{e.message})
  exit(1)
end

producer.close

producer = build_producer("true")

unless producer.idempotent? == true
  warn 'FAIL: "true" producer is idempotent'
  exit(1)
end

refused = begin
  producer.variant(topic_config: { acks: 0 })
  false
rescue WaterDrop::Errors::VariantInvalidError
  true
end

unless refused
  warn 'FAIL: "true" producer refuses an acks: 0 variant'
  exit(1)
end

producer.close
