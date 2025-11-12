# frozen_string_literal: true

module WaterDrop
  class Producer
    # Testing utilities for WaterDrop Producer instances.
    #
    # This module provides methods for triggering and querying fatal errors on producers,
    # which is useful for testing error handling and recovery logic (such as automatic
    # producer reloading on fatal errors).
    #
    # @note This module should only be used in test environments.
    # @note Requires karafka-rdkafka >= 0.23.1 which includes Rdkafka::Testing support.
    # @note This module is not auto-loaded by Zeitwerk and must be manually required.
    #
    # @example Including for a single producer instance
    #   require 'waterdrop/producer/testing'
    #
    #   producer = WaterDrop::Producer.new
    #   producer.singleton_class.include(WaterDrop::Producer::Testing)
    #   producer.trigger_test_fatal_error(47, "Test producer fencing")
    #
    # @example Including for all producers in a test suite
    #   # In spec_helper.rb or test setup:
    #   require 'waterdrop/producer/testing'
    #
    #   WaterDrop::Producer.include(WaterDrop::Producer::Testing)
    #
    # @example Testing idempotent producer reload on fatal error
    #   producer = WaterDrop::Producer.new do |config|
    #     config.kafka = { 'bootstrap.servers': 'localhost:9092' }
    #     config.reload_on_idempotent_fatal_error = true
    #   end
    #   producer.singleton_class.include(WaterDrop::Producer::Testing)
    #
    #   # Trigger a fatal error that should cause reload
    #   producer.trigger_test_fatal_error(47, "Invalid producer epoch")
    #
    #   # Produce should succeed after automatic reload
    #   producer.produce_sync(topic: 'test', payload: 'message')
    #
    #   # Fatal error should be cleared after reload
    #   expect(producer.fatal_error).to be_nil
    module Testing
      # Triggers a test fatal error on the underlying rdkafka producer.
      #
      # This method uses librdkafka's test error injection functionality to simulate
      # fatal errors without requiring actual error conditions. This is particularly
      # useful for testing WaterDrop's fatal error handling and automatic reload logic.
      #
      # @param error_code [Integer] The librdkafka error code to trigger.
      #   Common error codes for testing:
      #   - 47 (RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH) - Producer fencing
      #   - 64 (RD_KAFKA_RESP_ERR_INVALID_PRODUCER_ID_MAPPING) - Invalid producer ID
      # @param reason [String] A descriptive reason for the error, used for debugging
      #   and logging purposes
      #
      # @return [Integer] Result code from rd_kafka_test_fatal_error (0 on success)
      #
      # @raise [RuntimeError] If the underlying rdkafka client doesn't support testing
      #
      # @example Trigger producer fencing error
      #   producer.trigger_test_fatal_error(47, "Test producer fencing scenario")
      #
      # @example Trigger invalid producer ID error
      #   producer.trigger_test_fatal_error(64, "Test invalid producer ID mapping")
      def trigger_test_fatal_error(error_code, reason)
        ensure_testing_support!
        client.trigger_test_fatal_error(error_code, reason)
      end

      # Checks if a fatal error has occurred on the underlying rdkafka producer.
      #
      # This method queries librdkafka's fatal error state to retrieve information
      # about any fatal error that has occurred. Fatal errors are serious errors that
      # prevent the producer from continuing normal operation.
      #
      # @return [Hash, nil] A hash containing error details if a fatal error occurred,
      #   or nil if no fatal error is present. The hash contains:
      #   - :error_code [Integer] The librdkafka error code
      #   - :error_string [String] Human-readable error description
      #
      # @example Check for fatal error
      #   if error = producer.fatal_error
      #     puts "Fatal error #{error[:error_code]}: #{error[:error_string]}"
      #   else
      #     puts "No fatal error present"
      #   end
      #
      # @example Verify fatal error after triggering
      #   producer.trigger_test_fatal_error(47, "Test error")
      #   error = producer.fatal_error
      #   expect(error[:error_code]).to eq(47)
      def fatal_error
        ensure_testing_support!
        client.fatal_error
      end

      private

      # Ensures the underlying rdkafka client has testing support available.
      # Automatically requires and includes Rdkafka::Testing if not already present.
      #
      # @return [void]
      # @api private
      def ensure_testing_support!
        return if client.respond_to?(:trigger_test_fatal_error)

        # Require the rdkafka testing module if not already loaded
        require 'rdkafka/producer/testing' unless defined?(::Rdkafka::Testing)

        client.singleton_class.include(::Rdkafka::Testing)
      end
    end
  end
end
