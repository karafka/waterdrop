# frozen_string_literal: true

# A producer with `enable.idempotence` set to the String "false" is not idempotent: it may use an
# `acks: 0` variant. With the String "true" it is idempotent and refuses such a variant.

require "waterdrop"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

failed = false

# Records and prints a single assertion result instead of raising, so every check runs.
def check(failed, condition, message)
  if condition
    puts "  ok: #{message}"
    failed
  else
    puts "  FAIL: #{message}"
    true
  end
end

def build_producer(idempotence)
  WaterDrop::Producer.new do |config|
    config.deliver = true
    config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS, "enable.idempotence": idempotence }
  end
end

topic = generate_topic("idempotence-string-false")

producer = build_producer("false")
failed = check(failed, producer.idempotent? == false, '"false" producer is not idempotent')

begin
  report = producer.variant(topic_config: { acks: 0 }).produce_sync(topic: topic, payload: "x")
  failed = check(failed, report.offset.negative?, '"false" producer delivers via an acks: 0 variant')
rescue WaterDrop::Errors::VariantInvalidError => e
  failed = check(failed, false, %("false" producer refused an acks: 0 variant: #{e.message}))
end

producer.close

producer = build_producer("true")
failed = check(failed, producer.idempotent? == true, '"true" producer is idempotent')

begin
  producer.variant(topic_config: { acks: 0 })
  failed = check(failed, false, '"true" producer refuses an acks: 0 variant')
rescue WaterDrop::Errors::VariantInvalidError
  failed = check(failed, true, '"true" producer refuses an acks: 0 variant')
end

producer.close

exit(failed ? 1 : 0)
