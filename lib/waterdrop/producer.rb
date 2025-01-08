# frozen_string_literal: true

module WaterDrop
  # Main WaterDrop messages producer
  class Producer
    extend Forwardable
    include Sync
    include Async
    include Buffer
    include Transactions
    include ::Karafka::Core::Helpers::Time
    include ::Karafka::Core::Taggable

    # Local storage for given thread waterdrop client references for variants
    ::Fiber.send(:attr_accessor, :waterdrop_clients)

    # Which of the inline flow errors do we want to intercept and re-bind
    SUPPORTED_FLOW_ERRORS = [
      Rdkafka::RdkafkaError,
      Rdkafka::Producer::DeliveryHandle::WaitTimeoutError
    ].freeze

    # Empty hash to save on memory allocations
    EMPTY_HASH = {}.freeze

    # Empty array to save on memory allocations
    EMPTY_ARRAY = [].freeze

    private_constant :SUPPORTED_FLOW_ERRORS, :EMPTY_HASH, :EMPTY_ARRAY

    def_delegators :config

    # @return [String] uuid of the current producer
    attr_reader :id
    # @return [Status] producer status object
    attr_reader :status
    # @return [Array] internal messages buffer
    attr_reader :messages
    # @return [Object] monitor we want to use
    attr_reader :monitor
    # @return [Object] dry-configurable config object
    attr_reader :config

    # Creates a not-yet-configured instance of the producer
    # @param block [Proc] configuration block
    # @return [Producer] producer instance
    def initialize(&block)
      @operations_in_progress = Helpers::Counter.new
      @buffer_mutex = Mutex.new
      @connecting_mutex = Mutex.new
      @operating_mutex = Mutex.new
      @transaction_mutex = Mutex.new

      @status = Status.new
      @messages = []

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
      @default_variant = Variant.new(self, default: true)
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
      raise Errors::ProducerClosedError, id if @status.closed?

      @connecting_mutex.synchronize do
        return @client if @client && @pid == Process.pid

        # We undefine all the finalizers, in case it was a fork, so the finalizers from the parent
        # process don't leak
        ObjectSpace.undefine_finalizer(id)

        # We should raise an error when trying to use a producer with client from a fork. Always.
        if @client
          # We need to reset the client, otherwise there might be attempt to close the parent
          # client
          @client = nil
          raise Errors::ProducerUsedInParentProcess, Process.pid
        end

        # Finalizer tracking is needed for handling shutdowns gracefully.
        # I don't expect everyone to remember about closing all the producers all the time, thus
        # this approach is better. Although it is still worth keeping in mind, that this will
        # block GC from removing a no longer used producer unless closed properly but at least
        # won't crash the VM upon closing the process
        ObjectSpace.define_finalizer(id, proc { close })

        @pid = Process.pid
        @client = Builder.new.call(self, @config)

        @status.connected!
        @monitor.instrument('producer.connected', producer_id: id)
      end

      @client
    end

    # Fetches and caches the partition count of a topic
    #
    # @param topic [String] topic for which we want to get the number of partitions
    # @return [Integer] number of partitions of the requested topic or -1 if number could not be
    #   retrieved.
    #
    # @note It uses the underlying `rdkafka-ruby` partition count fetch and cache.
    def partition_count(topic)
      client.partition_count(topic.to_s)
    end

    # Purges data from both the buffer queue as well as the librdkafka queue.
    #
    # @note This is an operation that can cause data loss. Keep that in mind. It will not only
    #   purge the internal WaterDrop buffer but will also purge the librdkafka queue as well as
    #   will cancel any outgoing messages dispatches.
    def purge
      @monitor.instrument('buffer.purged', producer_id: id) do
        @buffer_mutex.synchronize do
          @messages = []
        end

        # We should not purge if there is no client initialized
        # It may not be initialized if we created a new producer that never connected to kafka,
        # we used buffer and purged. In cases like this client won't exist
        @connecting_mutex.synchronize do
          @client&.purge
        end
      end
    end

    # Builds the variant alteration and returns it.
    #
    # @param args [Object] anything `Producer::Variant` initializer accepts
    # @return [WaterDrop::Producer::Variant] variant proxy to use with alterations
    def with(**args)
      ensure_active!

      Variant.new(self, **args)
    end

    alias variant with

    # @return [Boolean] true if current producer is idempotent
    def idempotent?
      # Every transactional producer is idempotent by default always
      return true if transactional?
      return @idempotent if instance_variable_defined?(:'@idempotent')

      @idempotent = config.kafka.to_h.key?(:'enable.idempotence')
    end

    # Returns and caches the middleware object that may be used
    # @return [WaterDrop::Producer::Middleware]
    def middleware
      @middleware ||= config.middleware
    end

    # Flushes the buffers in a sync way and closes the producer
    # @param force [Boolean] should we force closing even with outstanding messages after the
    #   max wait timeout
    def close(force: false)
      # If we already own the transactional mutex, it means we are inside of a transaction and
      # it should not we allowed to close the producer in such a case.
      if @transaction_mutex.locked? && @transaction_mutex.owned?
        raise Errors::ProducerTransactionalCloseAttemptError, id
      end

      # The transactional mutex here can be used even when no transactions are in use
      # It prevents us from closing a mutex during transactions and is irrelevant in other cases
      @transaction_mutex.synchronize do
        @operating_mutex.synchronize do
          return unless @status.active?

          @monitor.instrument(
            'producer.closed',
            producer_id: id
          ) do
            @status.closing!
            @monitor.instrument('producer.closing', producer_id: id)

            # No need for auto-gc if everything got closed by us
            # This should be used only in case a producer was not closed properly and forgotten
            ObjectSpace.undefine_finalizer(id)

            # We save this thread id because we need to bypass the activity verification on the
            # producer for final flush of buffers.
            @closing_thread_id = Thread.current.object_id

            # Wait until all the outgoing operations are done. Only when no one is using the
            # underlying client running operations we can close
            sleep(0.001) until @operations_in_progress.value.zero?

            # Flush has its own buffer mutex but even if it is blocked, flushing can still happen
            # as we close the client after the flushing (even if blocked by the mutex)
            flush(true)

            # We should not close the client in several threads the same time
            # It is safe to run it several times but not exactly the same moment
            # We also mark it as closed only if it was connected, if not, it would trigger a new
            # connection that anyhow would be immediately closed
            if @client
              # Why do we trigger it early instead of just having `#close` do it?
              # The linger.ms time will be ignored for the duration of the call,
              # queued messages will be sent to the broker as soon as possible.
              begin
                @client.flush(current_variant.max_wait_timeout) unless @client.closed?
              # We can safely ignore timeouts here because any left outstanding requests
              # will anyhow force wait on close if not forced.
              # If forced, we will purge the queue and just close
              rescue ::Rdkafka::RdkafkaError, Rdkafka::AbstractHandle::WaitTimeoutError
                nil
              ensure
                # Purge fully the local queue in case of a forceful shutdown just to be sure, that
                # there are no dangling messages. In case flush was successful, there should be
                # none but we do it just in case it timed out
                purge if force
              end

              @client.close

              @client = nil
            end

            # Remove callbacks runners that were registered
            ::Karafka::Core::Instrumentation.statistics_callbacks.delete(@id)
            ::Karafka::Core::Instrumentation.error_callbacks.delete(@id)
            ::Karafka::Core::Instrumentation.oauthbearer_token_refresh_callbacks.delete(@id)

            @status.closed!
          end
        end
      end
    end

    # Closes the producer with forced close after timeout, purging any outgoing data
    def close!
      close(force: true)
    end

    private

    # Ensures that we don't run any operations when the producer is not configured or when it
    # was already closed
    def ensure_active!
      return if @status.active?
      return if @status.closing? && @operating_mutex.owned?

      raise Errors::ProducerNotConfiguredError, id if @status.initial?
      raise Errors::ProducerClosedError, id if @status.closing?
      raise Errors::ProducerClosedError, id if @status.closed?

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

    # Waits on a given handler
    #
    # @param handler [Rdkafka::Producer::DeliveryHandle]
    # @param raise_response_error [Boolean] should we raise the response error after we receive the
    #   final result and it is an error.
    def wait(handler, raise_response_error: true)
      handler.wait(
        # rdkafka max_wait_timeout is in seconds and we use ms
        max_wait_timeout: current_variant.max_wait_timeout / 1_000.0,
        raise_response_error: raise_response_error
      )
    end

    # @return [Producer::Variant] the variant config. Either custom if built using `#with` or
    #   a default one.
    def current_variant
      Fiber.current.waterdrop_clients ||= {}
      Fiber.current.waterdrop_clients[id] || @default_variant
    end

    # Runs the client produce method with a given message
    #
    # @param message [Hash] message we want to send
    def produce(message)
      produce_time ||= monotonic_now

      # This can happen only during flushing on closing, in case like this we don't have to
      # synchronize because we already own the lock
      if @operating_mutex.owned?
        @operations_in_progress.increment
      else
        @operating_mutex.synchronize { @operations_in_progress.increment }
        ensure_active!
      end

      # We basically only duplicate the message hash only if it is needed.
      # It is needed when user is using a custom settings variant or when symbol is provided as
      # the topic name. We should never mutate user input message as it may be a hash that the
      # user is using for some other operations
      if message[:topic].is_a?(Symbol) || !current_variant.default?
        message = message.dup
        # In case someone defines topic as a symbol, we need to convert it into a string as
        # librdkafka does not accept symbols
        message[:topic] = message[:topic].to_s
        message[:topic_config] = current_variant.topic_config
      end

      if transactional?
        transaction { client.produce(**message) }
      else
        client.produce(**message)
      end
    rescue SUPPORTED_FLOW_ERRORS.first => e
      # Unless we want to wait and retry and it's a full queue, we raise normally
      raise unless @config.wait_on_queue_full
      raise unless e.code == :queue_full
      # If we're running for longer than the timeout, we need to re-raise the queue full.
      # This will prevent from situation where cluster is down forever and we just retry and retry
      # in an infinite loop, effectively hanging the processing
      raise unless monotonic_now - produce_time < @config.wait_timeout_on_queue_full

      label = caller_locations(2, 1)[0].label.split(' ').last.split('#').last

      # We use this syntax here because we want to preserve the original `#cause` when we
      # instrument the error and there is no way to manually assign `#cause` value. We want to keep
      # the original cause to maintain the same API across all the errors dispatched to the
      # notifications pipeline.
      begin
        raise Errors::ProduceError, e.inspect
      rescue Errors::ProduceError => e
        # Users can configure this because in pipe-like flows with high throughput, queue full with
        # retry may be used as a throttling system that will backoff and wait.
        # In such scenarios this error notification can be removed and until queue full is
        # retryable, it will not be raised as an error.
        if @config.instrument_on_wait_queue_full
          # We want to instrument on this event even when we restart it.
          # The reason is simple: instrumentation and visibility.
          # We can recover from this, but despite that we should be able to instrument this.
          # If this type of event happens too often, it may indicate that the buffer settings are
          # not well configured.
          @monitor.instrument(
            'error.occurred',
            producer_id: id,
            message: message,
            error: e,
            type: "message.#{label}"
          )
        end

        # We do not poll the producer because polling happens in a background thread
        # It also should not be a frequent case (queue full), hence it's ok to just throttle.
        sleep @config.wait_backoff_on_queue_full / 1_000.0
      end

      @operations_in_progress.decrement
      retry
    ensure
      @operations_in_progress.decrement
    end
  end
end
