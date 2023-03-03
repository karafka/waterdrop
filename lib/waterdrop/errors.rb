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

    # Raised when there is an inline error during single message produce operations
    ProduceError = Class.new(BaseError)

    # Raised when during messages producing something bad happened inline
    class ProduceManyError < ProduceError
      attr_reader :dispatched

      # @param dispatched [Array<Rdkafka::Producer::DeliveryHandle>] handlers of the
      #   messages that we've dispatched
      def initialize(dispatched)
        super()
        @dispatched = dispatched
      end
    end
  end
end
