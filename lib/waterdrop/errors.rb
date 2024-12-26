# frozen_string_literal: true

module WaterDrop
  # Namespace used to encapsulate all the internal errors of WaterDrop
  module Errors
    # Base class for all the WaterDrop internal errors
    BaseError = Class.new(StandardError)

    # Raised when configuration doesn't match with validation contract
    ConfigurationInvalidError = Class.new(BaseError)

    # Raised when variant alteration is not valid
    VariantInvalidError = Class.new(BaseError)

    # Raised when we want to use a producer that was not configured
    ProducerNotConfiguredError = Class.new(BaseError)

    # Raised when we want to reconfigure a producer that was already configured
    ProducerAlreadyConfiguredError = Class.new(BaseError)

    # Raised when trying to use connected producer from a forked child process
    # Producers cannot be used in forks if they were already used in the child processes
    ProducerUsedInParentProcess = Class.new(BaseError)

    # Raised when there was an attempt to use a closed producer
    ProducerClosedError = Class.new(BaseError)

    # Raised if you attempt to close the producer from within a transaction. This is not allowed.
    ProducerTransactionalCloseAttemptError = Class.new(BaseError)

    # Raised when we want to send a message that is invalid (impossible topic, etc)
    MessageInvalidError = Class.new(BaseError)

    # Raised when we want to commit transactional offset and the input is invalid
    TransactionalOffsetInvalidError = Class.new(BaseError)

    # Raised when transaction attempt happens on a non-transactional producer
    ProducerNotTransactionalError = Class.new(BaseError)

    # Raised when we've got an unexpected status. This should never happen. If it does, please
    # contact us as it is an error.
    StatusInvalidError = Class.new(BaseError)

    # Raised when there is an inline error during single message produce operations
    ProduceError = Class.new(BaseError)

    # Raised when we attempt to perform operation that is only allowed inside of a transaction and
    # there is no transaction around us
    TransactionRequiredError = Class.new(BaseError)

    # Raise it within a transaction to abort it
    # It does not have an `Error` postfix because technically it is not an error as it is used for
    # graceful transaction aborting
    AbortTransaction = Class.new(BaseError)

    # Do not use `break`, `return` or `throw` inside of the transaction blocks
    EarlyTransactionExitNotAllowedError = Class.new(BaseError)

    # Raised when during messages producing something bad happened inline
    class ProduceManyError < ProduceError
      attr_reader :dispatched

      # @param dispatched [Array<Rdkafka::Producer::DeliveryHandle>] handlers of the
      #   messages that we've dispatched
      # @param message [String] error message
      def initialize(dispatched, message)
        super(message)
        @dispatched = dispatched
      end
    end
  end

  # Alias so we can have a nicer API to abort transactions
  # This makes referencing easier
  AbortTransaction = Errors::AbortTransaction
end
