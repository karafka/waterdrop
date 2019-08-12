# frozen_string_literal: true

module WaterDrop
  class Producer
    # A dummy client that is supposed to be used instead of Rdkafka::Producer in case we don't
    # want to dispatch anything to Kafka
    class DummyClient
      # @return [DummyClient] dummy instance
      def initialize
        @counter = -1
      end

      # @param _args [Object] anything really, this dummy is suppose to support anything
      # @return [self] returns self for chaining cases
      def method_missing(*_args)
        self || super
      end

      # Dummy method for returning the delivery report
      # @param _timeout [Integer] number of seconds to wait for the report
      # @return [::Rdkafka::Producer::DeliveryReport]
      def wait(_timeout = 60)
        ::Rdkafka::Producer::DeliveryReport.new(0, @counter += 1)
      end

      # @param _args [Object] anything really, this dummy is suppose to support anything
      def respond_to_missing?(*_args)
        true
      end
    end
  end
end
