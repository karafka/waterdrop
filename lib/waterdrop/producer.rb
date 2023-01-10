# frozen_string_literal: true

# frozen_string_literal: true

module Rdkafka
  # @private
  # A wrapper around a native kafka that handles all access
  class NativeKafka
    def initialize(inner)
      @inner = inner
      @mutex = Mutex.new

      # Start thread to poll client for delivery callbacks
      @polling_thread = Thread.new do
        loop do
          Rdkafka::Bindings.rd_kafka_poll(@inner, 250)
          # Destroy consumer and exit thread if closing and the poll queue is empty
          break if Thread.current[:closing] && Rdkafka::Bindings.rd_kafka_outq_len(@inner) == 0
        end
      end

      @polling_thread.abort_on_exception = true
      @polling_thread[:closing] = false

      @closing = false
    end

    def close(object_id=nil)
      return if closed?

      # Indicate to the outside world that we are closing
      @closing = true

      # Indicate to polling thread that we're closing
      @polling_thread[:closing] = true

      @mutex.synchronize do
        return unless @inner

        # Wait for the polling thread to finish up
        @polling_thread.join

        Rdkafka::Bindings.rd_kafka_destroy(@inner)
        @inner = nil
      end
    end
  end
end

module WaterDrop
  # Main WaterDrop messages producer
  class Producer
    extend Forwardable
    include Sync
    include Async
    include Buffer

    def_delegators :config, :middleware

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
      @buffer_mutex = Mutex.new
      @connecting_mutex = Mutex.new
      @closing_mutex = Mutex.new

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
      return @client if @client && @pid == Process.pid

      # Don't allow to obtain a client reference for a producer that was not configured
      raise Errors::ProducerNotConfiguredError, id if @status.initial?

      @connecting_mutex.synchronize do
        return @client if @client && @pid == Process.pid

        # We should raise an error when trying to use a producer from a fork, that is already
        # connected to Kafka. We allow forking producers only before they are used
        raise Errors::ProducerUsedInParentProcess, Process.pid if @status.connected?

        # We undefine all the finalizers, in case it was a fork, so the finalizers from the parent
        # process don't leak
        ObjectSpace.undefine_finalizer(id)
        # Finalizer tracking is needed for handling shutdowns gracefully.
        # I don't expect everyone to remember about closing all the producers all the time, thus
        # this approach is better. Although it is still worth keeping in mind, that this will
        # block GC from removing a no longer used producer unless closed properly but at least
        # won't crash the VM upon closing the process
        ObjectSpace.define_finalizer(id, proc { close })

        @pid = Process.pid
        @client = Builder.new.call(self, @config)

        # Register statistics runner for this particular type of callbacks
        ::Karafka::Core::Instrumentation.statistics_callbacks.add(
          @id,
          Instrumentation::Callbacks::Statistics.new(@id, @client.name, @config.monitor)
        )

        # Register error tracking callback
        ::Karafka::Core::Instrumentation.error_callbacks.add(
          @id,
          Instrumentation::Callbacks::Error.new(@id, @client.name, @config.monitor)
        )

        @status.connected!
      end

      @client
    end

    # Flushes the buffers in a sync way and closes the producer
    def close
      @closing_mutex.synchronize do
        return unless @status.active?

        @monitor.instrument(
          'producer.closed',
          producer_id: id
        ) do
          @status.closing!

          # No need for auto-gc if everything got closed by us
          # This should be used only in case a producer was not closed properly and forgotten
          ObjectSpace.undefine_finalizer(id)

          # Flush has its own buffer mutex but even if it is blocked, flushing can still happen
          # as we close the client after the flushing (even if blocked by the mutex)
          flush(true)

          # Flush the internal buffers in librdkafka
          if @client && !@client.closed?
            client.flush
            # We should not close the client in several threads the same time
            # It is safe to run it several times but not exactly the same moment
            # We also mark it as closed only if it was connected, if not, it would trigger a new
            # connection that anyhow would be immediately closed
            client.close
          end

          # Remove callbacks runners that were registered
          ::Karafka::Core::Instrumentation.statistics_callbacks.delete(@id)
          ::Karafka::Core::Instrumentation.error_callbacks.delete(@id)

          @status.closed!
        end
      end
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
      @contract.validate!(message, Errors::MessageInvalidError)
    end
  end
end
