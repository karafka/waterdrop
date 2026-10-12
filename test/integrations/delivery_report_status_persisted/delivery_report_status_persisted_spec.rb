# frozen_string_literal: true

# Integration test showing the delivery report fields for a message the broker acknowledged.
#
# Every `Rdkafka::Producer::DeliveryReport` carries the persistence status of the message, the
# produce latency (in microseconds, from the `produce` call until the delivery report) and the id
# of the broker that took the write. The report comes back from `produce_sync`, from the handle of
# `produce_async` and in the `message.acknowledged` event payload.

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
  failed = check(failed, report.error.nil?, "#{name}: report carries no error")
  failed = check(failed, report.persisted?, "#{name}: message is persisted")
  failed = check(
    failed,
    report.status == Rdkafka::Bindings::RD_KAFKA_MSG_STATUS_PERSISTED,
    "#{name}: status is RD_KAFKA_MSG_STATUS_PERSISTED"
  )
  failed = check(
    failed,
    !report.possibly_persisted? && !report.not_persisted?,
    "#{name}: no other status predicate matches"
  )
  failed = check(
    failed,
    report.latency.is_a?(Integer) && report.latency.positive?,
    "#{name}: latency is a positive number of microseconds (#{report.latency})"
  )
  failed = check(
    failed,
    broker_ids.include?(report.broker_id),
    "#{name}: broker id #{report.broker_id.inspect} is a cluster broker #{broker_ids}"
  )
end

failed = check(
  failed,
  event_reports.all? { |report| report.persisted? && report.latency && report.broker_id },
  "message.acknowledged reports carry status, latency and broker id"
)

if failed
  puts "\nFAIL: delivery report status, latency or broker id regressed for persisted messages"
else
  puts "\nPASS: delivery reports of persisted messages carry status, latency and broker id"
end

exit(failed ? 1 : 0)
