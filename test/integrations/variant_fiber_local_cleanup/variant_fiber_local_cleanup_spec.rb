# frozen_string_literal: true

# Integration test for issue #836
# Verifies that Fiber.current.waterdrop_clients does not accumulate stale nil entries
# after variant method calls complete.
#
# When a variant wraps a producer method, it stores itself in fiber-local storage and
# should fully remove the entry in the ensure block. Using `= nil` instead of `delete`
# leaves a permanent nil-valued key, causing unbounded hash growth in long-running
# processes or fiber pools.

require "waterdrop"
require "logger"
require "securerandom"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

producer = WaterDrop::Producer.new do |config|
  config.deliver = false
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
  config.logger = Logger.new($stdout, level: Logger::INFO)
end

variant = producer.with(topic_config: { "acks": 1 })

topic = generate_topic("variant-cleanup")

# --- Test 1: Single variant call should not leave stale key ---

fiber1 = Fiber.new do
  variant.produce_async(topic: topic, payload: "hello")

  clients = Fiber.current.waterdrop_clients
  stale = clients.key?(producer.id)

  if stale
    puts "FAILURE: stale nil key found after single variant call"
    puts "  waterdrop_clients: #{clients.inspect}"
  end

  stale
end

single_call_stale = fiber1.resume

# --- Test 2: Multiple variant calls should not accumulate keys ---

fiber2 = Fiber.new do
  5.times { variant.produce_async(topic: topic, payload: "hello") }

  clients = Fiber.current.waterdrop_clients
  stale = clients.key?(producer.id)
  size = clients.size

  if stale
    puts "FAILURE: stale nil key found after multiple variant calls"
    puts "  waterdrop_clients: #{clients.inspect}"
    puts "  hash size: #{size}"
  end

  stale
end

multiple_calls_stale = fiber2.resume

# --- Test 3: Multiple producers should not leave stale keys ---

producer2 = WaterDrop::Producer.new do |config|
  config.deliver = false
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
  config.logger = Logger.new($stdout, level: Logger::INFO)
end

variant2 = producer2.with(topic_config: { "acks": 1 })

fiber3 = Fiber.new do
  variant.produce_async(topic: topic, payload: "hello")
  variant2.produce_async(topic: topic, payload: "world")

  clients = Fiber.current.waterdrop_clients

  stale_count = clients.count { |_k, v| v.nil? }

  if stale_count > 0
    puts "FAILURE: #{stale_count} stale nil key(s) from multiple producers"
    puts "  waterdrop_clients: #{clients.inspect}"
  end

  stale_count > 0
end

multiple_producers_stale = fiber3.resume

producer.close
producer2.close

# --- Report ---

success = !single_call_stale && !multiple_calls_stale && !multiple_producers_stale

if success
  puts "SUCCESS: No stale keys in Fiber.current.waterdrop_clients after variant calls"
else
  puts "FAILURE: Stale nil keys detected in Fiber.current.waterdrop_clients (issue #836)"
end

exit(success ? 0 : 1)
