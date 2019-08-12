# frozen_string_literal: true

module WaterDrop
  class Producer
    # A dummy client that is supposed to be used instead of Rdkafka::Producer in case we don't
    # want to dispatch anything to Kafka
    class DummyClient
      # @param _args [Object] anything really, this dummy is suppose to support anything
      # @return [self] returns self for chaining cases
      def method_missing(*_args)
        self || super
      end

      # @param _args [Object] anything really, this dummy is suppose to support anything
      def respond_to_missing?(*_args)
        true
      end
    end
  end
end
