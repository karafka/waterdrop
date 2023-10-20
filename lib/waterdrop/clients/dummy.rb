# frozen_string_literal: true

module WaterDrop
  module Clients
    # A dummy client that is supposed to be used instead of Rdkafka::Producer in case we don't
    # want to dispatch anything to Kafka.
    #
    # It does not store anything and just ignores messages.
    class Dummy
      # @param _producer [WaterDrop::Producer]
      # @return [Dummy] dummy instance
      def initialize(_producer)
        @counter = -1
      end

      # Dummy method for returning the delivery report
      # @param _args [Object] anything that the delivery handle accepts
      # @return [::Rdkafka::Producer::DeliveryReport]
      def wait(*_args)
        ::Rdkafka::Producer::DeliveryReport.new(0, @counter += 1)
      end

      # @param _args [Object] anything really, this dummy is suppose to support anything
      def respond_to_missing?(*_args)
        true
      end

      # Yields the code pretending it is in a transaction
      # Supports our aborting transaction flow
      def transaction
        result = nil
        commit = false

        catch(:abort) do
          result = yield
          commit = true
        end

        commit || raise(WaterDrop::Errors::AbortTransaction)

        result
      rescue StandardError => e
        return if e.is_a?(WaterDrop::Errors::AbortTransaction)

        raise
      end

      # @param _args [Object] anything really, this dummy is suppose to support anything
      # @return [self] returns self for chaining cases
      def method_missing(*_args)
        self || super
      end
    end
  end
end
