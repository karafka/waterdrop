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

failed = check(failed, !error.nil?, "the transaction with a too large message fails")
failed = check(failed, error&.abortable?, "the caller gets the abortable commit error (#{error&.code})")
failed = check(failed, error&.code != :state, "no invalid state error from a second abort")
failed = check(failed, events.count([:aborted]) == 1, "the transaction is aborted once")
failed = check(failed, events.include?([:error, "transaction.commit"]), "the commit error is reported")
failed = check(failed, !events.include?([:error, "transaction.abort"]), "no abort error is reported")
failed = check(
  failed,
  events.select { |event| event.first == :reload } == [[:reload, error&.code]],
  "the client is reloaded once, for the commit error"
)

recovered = begin
  producer.transaction { producer.produce_sync(topic: ok_topic, payload: "recovery") }
  true
rescue
  false
end

failed = check(failed, recovered, "the next transaction commits")

producer.close

if failed
  puts "\nFAIL: an abortable commit error is not handled with a single abort"
else
  puts "\nPASS: an abortable commit error aborts once and reaches the caller as is"
end

exit(failed ? 1 : 0)
