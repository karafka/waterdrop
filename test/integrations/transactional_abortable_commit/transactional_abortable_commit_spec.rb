# frozen_string_literal: true

# A transaction whose commit fails with an abortable error must be aborted exactly once, and the
# caller must get the commit error itself.
#
# A DENY Write ACL on one topic makes an async produce inside the transaction fail with
# `topic_authorization_failed`, so `commit_transaction` fails with an abortable error. The commit
# error handling aborts the transaction. `#transaction` used to abort it a second time, which
# failed with `Operation not valid in state Ready` (`:state`): that error replaced the commit error
# and reloaded the client.

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
denied_topic = generate_topic("tx-abortable-denied")
create_topic(ok_topic)
create_topic(denied_topic)

acl = {
  resource_type: Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC,
  resource_name: denied_topic,
  resource_pattern_type: Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL,
  principal: "User:ANONYMOUS",
  host: "*",
  operation: Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_WRITE,
  permission_type: Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_DENY
}

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP_SERVERS).admin
admin.create_acl(**acl).wait

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
producer.monitor.subscribe("producer.reload") { events << [:reload] }

producer.transaction { producer.produce_sync(topic: ok_topic, payload: "warmup") }

# The ACL reaches the broker's authorizer asynchronously: retry until a transaction hits it
error = nil
10.times do
  events.clear

  begin
    producer.transaction do
      producer.produce_sync(topic: ok_topic, payload: "ok")
      producer.produce_async(topic: denied_topic, payload: "denied")
    end
  rescue Rdkafka::RdkafkaError => e
    error = e
    break
  end

  sleep(0.5)
end

failed = check(failed, !error.nil?, "the transaction with a denied write fails")
failed = check(failed, error&.abortable?, "the caller gets the abortable commit error (#{error&.code})")
failed = check(failed, error&.code != :state, "no invalid state error from a second abort")
failed = check(failed, events.count([:aborted]) == 1, "the transaction is aborted once")
failed = check(failed, events.include?([:error, "transaction.commit"]), "the commit error is reported")
failed = check(failed, !events.include?([:error, "transaction.abort"]), "no abort error is reported")
failed = check(failed, !events.include?([:reload]), "the client is not reloaded")

recovered = begin
  producer.transaction { producer.produce_sync(topic: ok_topic, payload: "recovery") }
  true
rescue
  false
end

failed = check(failed, recovered, "the next transaction commits")

producer.close
admin.delete_acl(**acl).wait
admin.close

if failed
  puts "\nFAIL: an abortable commit error is not handled with a single abort"
else
  puts "\nPASS: an abortable commit error aborts once and reaches the caller as is"
end

exit(failed ? 1 : 0)
