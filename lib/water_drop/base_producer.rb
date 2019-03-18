# frozen_string_literal: true

module WaterDrop
  # Base messages producer that contains all the logic that is exactly the same for both
  # sync and async producers
  class BaseProducer
    class << self
      private

      # Runs the message options validations and raises an error if anything is invalid
      # @param options [Hash] hash that we want to validate
      # @raise [WaterDrop::Errors::InvalidMessageOptions] raised when message options are
      #   somehow invalid and we cannot perform delivery because of that
      def validate!(options)
        validation_result = Schemas::MessageOptions.call(options)
        return true if validation_result.success?

        raise Errors::InvalidMessageOptions, validation_result.errors
      end

      # Upon failed delivery, we may try to resend a message depending on the attempt number
      #   or re-raise an error if we're unable to do that after given number of retries
      #   This method checks that and also instruments errors and retries for the delivery
      # @param attempts_count [Integer] number of attempt (starting from 1) for the delivery
      # @param message [String] message that we want to send to Kafka
      # @param options [Hash] options (including topic) for producer
      # @param error [Kafka::Error] error that occurred
      # @return [Boolean] true if this is a graceful attempt and we can retry or false it this
      #   was the final one and we should deal with the fact, that we cannot deliver a given
      #   message
      def graceful_attempt?(attempts_count, message, options, error)
        scope = "#{to_s.split('::').last.sub('Producer', '_producer').downcase}.call"
        payload = {
          caller: self,
          message: message,
          options: options,
          error: error,
          attempts_count: attempts_count
        }

        if attempts_count > WaterDrop.config.kafka.max_retries
          WaterDrop.monitor.instrument("#{scope}.error", payload)
          false
        else
          WaterDrop.monitor.instrument("#{scope}.retry", payload)
          true
        end
      end
    end
  end
end
