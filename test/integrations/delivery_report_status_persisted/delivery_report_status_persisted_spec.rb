# frozen_string_literal: true

# Integration test showing the delivery report fields for a message the broker acknowledged.
#
# Every `Rdkafka::Producer::DeliveryReport` carries the persistence status of the message, the
# produce latency (in microseconds, from the `produce` call until the delivery report) and the id
# of the broker that took the write. The report comes back from `produce_sync`, from the handle of
# `produce_async` and in the `message.acknowledged` event payload.

require "waterdrop"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

# `acks: all` so a `PERSISTED` status means the write is on every in-sync replica.
producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS, acks: "all" }
end

topic = generate_topic("dr-persisted")

acknowledged = Queue.new

producer.monitor.subscribe("message.acknowledged") do |event|
  acknowledged << event[:delivery_report]
end

sync_report = producer.produce_sync(topic: topic, payload: "sync")
async_report = producer.produce_async(topic: topic, payload: "async").wait
event_reports = Array.new(2) { acknowledged.pop }

broker_ids = producer.client.metadata(topic).brokers.map { |broker| broker[:broker_id] }

producer.close

[["produce_sync", sync_report], ["produce_async", async_report]].each do |name, report|
  unless report.error.nil?
    warn "FAIL: #{name}: report carries no error"
    exit(1)
  end

  unless report.persisted?
    warn "FAIL: #{name}: message is persisted"
    exit(1)
  end

  unless report.status == Rdkafka::Bindings::RD_KAFKA_MSG_STATUS_PERSISTED
    warn "FAIL: #{name}: status is RD_KAFKA_MSG_STATUS_PERSISTED"
    exit(1)
  end

  unless !report.possibly_persisted? && !report.not_persisted?
    warn "FAIL: #{name}: no other status predicate matches"
    exit(1)
  end

  unless report.latency.is_a?(Integer) && report.latency.positive?
    warn "FAIL: #{name}: latency is a positive number of microseconds (#{report.latency})"
    exit(1)
  end

  unless broker_ids.include?(report.broker_id)
    warn "FAIL: #{name}: broker id #{report.broker_id.inspect} is a cluster broker #{broker_ids}"
    exit(1)
  end
end

unless event_reports.all? { |report| report.persisted? && report.latency && report.broker_id }
  warn "FAIL: message.acknowledged reports carry status, latency and broker id"
  exit(1)
end

puts "PASS: delivery reports of persisted messages carry status, latency and broker id"
