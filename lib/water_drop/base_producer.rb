# frozen_string_literal: true

module WaterDrop
  # Base messages producer that contains all the logic that is exactly the same for both
  # sync and async producers
  class BaseProducer
    class << self
      # Delivery boy method name that we use to invoke producer action
      attr_accessor :method_name

      # Performs message delivery using method_name method
      # @param message [String] message that we want to send to Kafka
      # @param options [Hash] options (including topic) for producer
      # @raise [WaterDrop::Errors::InvalidMessageOptions] raised when message options are
      #   somehow invalid and we cannot perform delivery because of that
      def call(message, options)
        validate!(options)
        return unless WaterDrop.config.deliver
        DeliveryBoy.public_send(method_name, message, options)
      end

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
    end
  end
end
