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

    # Creates a not-yet-configured instance of the producer
    # @param block [Proc] configuration block
    # @return [Producer] producer instance
    def initialize(&block)
      @mutex = Mutex.new
      @status = Status.new
      @messages = Concurrent::Array.new

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
      @contract = Contracts::Message.new(max_payload_size: @config.max_payload_size)
      @status.configured!
    end

    # @return [Rdkafka::Producer] raw rdkafka producer
    # @note Client is lazy initialized, keeping in mind also the fact of a potential fork that
    #   can happen any time.
    # @note It is not recommended to fork a producer that is already in use so in case of
    #   bootstrapping a cluster, it's much better to fork configured but not used producers
    def client
      return @client if @pid == Process.pid

      # We should raise an error when trying to use a producer from a fork, that is already
      # connected to Kafka. We allow forking producers only before they are used
      raise Errors::ProducerUsedInParentProcess, Process.pid if @status.connected?

      # We undefine all the finalizers, in case it was a fork, so the finalizers from the parent
      # process don't leak
      ObjectSpace.undefine_finalizer(self)
      # Finalizer tracking is needed for handling shutdowns gracefully.
      # I don't expect everyone to remember about closing all the producers all the time, thus
      # this approach is better. Although it is still worth keeping in mind, that this will
      # block GC from removing a no longer used producer unless closed properly
      ObjectSpace.define_finalizer(self, proc { close })

      @pid = Process.pid
      @status.connected!
      @client = Builder.new.call(self, @config)
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

        client.close
        @status.closed!
      end

      # No need for auto-gc if everything got closed by us
      # This should be used only in case a producer was not closed properly and forgotten
      ObjectSpace.undefine_finalizer(self)
    end

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
