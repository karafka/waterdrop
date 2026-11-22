# frozen_string_literal: true

# A transaction whose commit fails with an abortable error must be aborted exactly once, and the
# caller must get the commit error itself.
#
# A message larger than its topic's `max.message.bytes` makes an async produce inside the
# transaction fail with `msg_size_too_large`, so `commit_transaction` fails with an abortable
# error. It needs no broker ACLs, so it also holds on brokers whose client is a super user. The commit
# error handling aborts the transaction; no second abort may follow, so no `Operation not valid in
# state Ready` (`:state`) error replaces the commit error or is reported as an abort error. The
# client is reloaded once, for the commit error.

require "waterdrop"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

ok_topic = generate_topic("tx-abortable-ok")
small_topic = generate_topic("tx-abortable-small")
create_topic(ok_topic)
create_topic(small_topic, "max.message.bytes": 128)

producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "transactional.id": SecureRandom.uuid
  }
end

events = []
producer.monitor.subscribe("error.occurred") { |event| events << [:error, event[:type]] }
producer.monitor.subscribe("transaction.aborted") { events << [:aborted] }
producer.monitor.subscribe("producer.reload") { |event| events << [:reload, event[:error].code] }

producer.transaction { producer.produce_sync(topic: ok_topic, payload: "warmup") }

events.clear
error = nil

begin
  producer.transaction do
    producer.produce_sync(topic: ok_topic, payload: "ok")
    producer.produce_async(topic: small_topic, payload: "x" * 512)
  end
rescue Rdkafka::RdkafkaError => e
  error = e
end

if error.nil?
  warn "FAIL: the transaction with a too large message fails"
  exit(1)
end

unless error&.abortable?
  warn "FAIL: the caller gets the abortable commit error (#{error&.code})"
  exit(1)
end

unless error&.code != :state
  warn "FAIL: no invalid state error from a second abort"
  exit(1)
end

unless events.count([:aborted]) == 1
  warn "FAIL: the transaction is aborted once"
  exit(1)
end

unless events.include?([:error, "transaction.commit"])
  warn "FAIL: the commit error is reported"
  exit(1)
end

if events.include?([:error, "transaction.abort"])
  warn "FAIL: no abort error is reported"
  exit(1)
end

unless events.select { |event| event.first == :reload } == [[:reload, error&.code]]
  warn "FAIL: the client is reloaded once, for the commit error"
  exit(1)
end

recovered = begin
  producer.transaction { producer.produce_sync(topic: ok_topic, payload: "recovery") }
  true
rescue
  false
end

unless recovered
  warn "FAIL: the next transaction commits"
  exit(1)
end

producer.close

puts "PASS: an abortable commit error aborts once and reaches the caller as is"
