# frozen_string_literal: true

module WaterDrop
  # Main WaterDrop messages producer
  class Producer
    include Sync
    include Async
    include Buffer

    # @return [String] uuid of the current producer
    attr_reader :id
    # @return [Status] producer status object
    attr_reader :status
    # @return [Concurrent::Array] internal messages buffer
    attr_reader :messages
    # @return [Object] monitor we want to use
    attr_reader :monitor
    # @return [Object] dry-configurable config object
    attr_reader :config
    # @return [Rdkafka::Producer] raw rdkafka producer
    attr_reader :client

    # Creates a not-yet-configured instance of the producer
    # @param block [Proc] configuration block
    # @return [Producer] producer instance
    def initialize(&block)
      @mutex = Mutex.new
      @status = Status.new
      @messages = Concurrent::Array.new

      # Finalizer tracking is needed for handling shutdowns gracefully.
      # I don't expect everyone to remember about closing all the producers all the time
      ObjectSpace.define_finalizer(self, proc { close })

      return unless block

      setup(&block)
    end

    # Sets up the whole configuration and initializes all that is needed
    # @param block [Block] configuration block
    def setup(&block)
      raise Errors::ProducerAlreadyConfiguredError, id unless @status.initial?

      @config = Config
                .new
                .setup(&block)
                .config

      @id = @config.id
      @monitor = @config.monitor
      @client = Builder.new.call(self, @config)
      @contract = Contracts::Message.new(max_payload_size: @config.max_payload_size)
      @status.active!
    end

    # Flushes the buffers in a sync way and closes the producer
    def close
      return unless @status.active?

      @monitor.instrument(
        'producer.closed',
        producer: self
      ) do
        @status.closing!

        flush(false)

        @client.close
        @status.closed!
      end
    end

    private

    # Ensures that we don't run any operations when the producer is not configured or when it
    # was already closed
    def ensure_active!
      return if @status.active?

      raise Errors::ProducerNotConfiguredError, id if @status.initial?
      raise Errors::ProducerClosedError, id if @status.closing? || @status.closed?

      # This should never happen
      raise Errors::StatusInvalidError, [id, @status.to_s]
    end

    # Ensures that the message we want to send out to Kafka is actually valid and that it can be
    # sent there
    # @param message [Hash] message we want to send
    # @raise [Karafka::Errors::MessageInvalidError]
    def validate_message!(message)
      result = @contract.call(message)
      return if result.success?

      raise Errors::MessageInvalidError, [
        result.errors.to_h,
        message
      ]
    end
  end
end
