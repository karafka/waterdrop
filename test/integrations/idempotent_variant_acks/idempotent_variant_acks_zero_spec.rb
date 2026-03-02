# frozen_string_literal: true

# Integration test for WaterDrop idempotent producer variant acks restriction
# Tests that an idempotent producer with default acks: all cannot create a variant with acks: 0.
#
# Idempotent producers require acks: all to guarantee exactly-once semantics.
# Allowing a variant with acks: 0 would break idempotency guarantees and is not permitted.

require "waterdrop"
require "logger"
require "securerandom"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

# Create idempotent producer with acks: all (required for idempotence)
producer = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "enable.idempotence": true,
    "request.required.acks": "all"
  }
  config.max_wait_timeout = 30_000
  config.logger = Logger.new($stdout, level: Logger::INFO)
end

# Track test results
variant_created = false
correct_error_raised = false
error_message = nil

begin
  # Attempt to create a variant with acks: 0 (should not be allowed)
  variant = producer.variant(topic_config: { acks: 0 })

  # If we get here, variant was created (should not happen)
  variant_created = true

  # Try to produce with the variant to trigger validation if deferred
  topic_name = "it-idem-variant-acks-#{SecureRandom.hex(6)}"
  variant.produce_sync(topic: topic_name, payload: "test")
rescue WaterDrop::Errors::VariantInvalidError => e
  # This is the expected error
  correct_error_raised = true
  error_message = e.message
rescue => e
  # Any other error is unexpected
  error_message = "Unexpected error: #{e.class}: #{e.message}"
end

producer.close

# Verify results:
# 1. Variant should not have been created successfully
# 2. VariantInvalidError should have been raised
success = !variant_created && correct_error_raised

if success
  puts "SUCCESS: Idempotent producer correctly rejected variant with acks: 0"
else
  puts "FAILURE: variant_created=#{variant_created}, correct_error_raised=#{correct_error_raised}"
  puts "Error: #{error_message}" if error_message
end

exit(success ? 0 : 1)
