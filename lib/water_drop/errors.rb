# frozen_string_literal: true

module WaterDrop
  # Namespace used to encapsulate all the internal errors of WaterDrop
  module Errors
    # Base class for all the WaterDrop internal errors
    BaseError = Class.new(StandardError)

    # Raised when configuration doesn't match with validation contract
    ConfigurationInvalidError = Class.new(BaseError)

    # Raised when we want to use a producer that was not configured
    ProducerNotConfiguredError = Class.new(BaseError)

    # Raised when we want to reconfigure a producer that was already configured
    ProducerAlreadyConfiguredError = Class.new(BaseError)

    # Raised when trying to use connected producer from a forked child process
    # Producers cannot be used in forks if they were already used in the child processes
    ProducerUsedInParentProcess = Class.new(BaseError)

    # Raised when there was an attempt to use a closed producer
    ProducerClosedError = Class.new(BaseError)

    # Raised when we want to send a message that is invalid (impossible topic, etc)
    MessageInvalidError = Class.new(BaseError)

    # Raised when we've got an unexpected status. This should never happen. If it does, please
    # contact us as it is an error.
    StatusInvalidError = Class.new(BaseError)

    # Raised when during messages flushing something bad happened
    class FlushFailureError < BaseError
      attr_reader :dispatched_messages

      # @param dispatched_messages [Array<Rdkafka::Producer::DeliveryHandle>] handlers of the
      #   messages that we've dispatched
      def initialize(dispatched_messages)
        super()
        @dispatched_messages = dispatched_messages
      end
    end
  end
end
