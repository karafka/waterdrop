# frozen_string_literal: true

module WaterDrop
  # Main WaterDrop messages producer
  class Producer
    extend Forwardable
    include Sync
    include Async
    include Buffer
    include Tombstone
    include Transactions
    include Idempotence
    include ClassMonitor
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

    private_constant(
      :SUPPORTED_FLOW_ERRORS, :EMPTY_HASH, :EMPTY_ARRAY
    )

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
      @id = nil
      @monitor = nil
      @contract = nil
      @default_variant = nil
      @client = nil
      @closing_thread_id = nil
      @idempotent = nil
      @transactional = nil
      @fd_polling = nil
      @poller = nil
      @idempotent_fatal_error_attempts = 0
      @transaction_fatal_error_attempts = 0
      @transaction_first_handle = nil

      @status = Status.new
      @messages = []

      # Instrument producer creation for global listeners
      class_monitor.instrument(
        "producer.created",
        producer: self,
        producer_id: @id
      )

      return unless block

      setup(&block)
    end

    # Sets up the whole configuration and initializes all that is needed
    def setup(...)
      raise Errors::ProducerAlreadyConfiguredError, id unless @status.initial?

      @config = Config
        .new
        .setup(...)
        .config

      @id = @config.id
      @monitor = @config.monitor
      @contract = Contracts::Message.new(max_payload_size: @config.max_payload_size)
      @default_variant = Variant.new(self, default: true)

      if @config.idle_disconnect_timeout.zero?
        @status.configured!

        # Instrument producer configuration for global listeners
        class_monitor.instrument(
          "producer.configured",
          producer: self,
          producer_id: @id,
          config: @config
        )

        return
      end

      # Setup idle disconnect listener if configured so we preserve tcp connections on rarely
      # used producers
      disconnector = Instrumentation::IdleDisconnectorListener.new(
        self,
        disconnect_timeout: @config.idle_disconnect_timeout
      )

      @monitor.subscribe(disconnector)

      @status.configured!

      # Instrument producer configuration for global listeners
      class_monitor.instrument(
        "producer.configured",
        producer: self,
        producer_id: @id,
        config: @config
      )
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
          # We need to reset the client, otherwise there might be attempt to close the parent client
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
        @monitor.instrument("producer.connected", producer_id: id)
      end

      @client
    end

    # Returns the number of messages in the librdkafka producer queue.
    #
    # This count includes:
    # - Messages waiting to be sent to the broker
    # - Messages currently in-flight (being transmitted)
    # - Delivery reports waiting to be processed
    #
    # @return [Integer] the number of pending messages in the rdkafka queue, or 0 if the
    #   producer is not connected
    #
    # @note This only counts messages in the rdkafka queue, not the internal WaterDrop buffer.
    #   To get the internal buffer count, use `messages.size`.
    #
    # @note Returns 0 when the producer is not connected as there cannot be any pending
    #   messages if we haven't connected.
    #
    # @example Check pending messages
    #   producer.queue_size #=> 42
    #
    # @example Check total pending work (buffer + rdkafka queue)
    #   internal_buffer = producer.messages.size
    #   rdkafka_queue = producer.queue_size
    #   total_pending = internal_buffer + rdkafka_queue
    def queue_size
      return 0 unless @status.connected?

      @connecting_mutex.synchronize do
        return 0 unless @client

        @client.queue_size
      end
    end

    alias_method :queue_length, :queue_size

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
      @monitor.instrument("buffer.purged", producer_id: id) do
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
    # @param args [Hash] variant configuration options
    # @option args [Integer, nil] :max_wait_timeout alteration to max wait timeout or nil to use
    #   default
    # @option args [Hash] :topic_config extra topic configuration that can be altered
    # @option args [Boolean] :default is this a default variant or an altered one
    # @return [WaterDrop::Producer::Variant] variant proxy to use with alterations
    # @see https://karafka.io/docs/Librdkafka-Configuration/#topic-configuration-properties
    def with(**args)
      ensure_active!

      Variant.new(self, **args)
    end

    alias_method :variant, :with

    # Returns and caches the middleware object that may be used
    # @return [WaterDrop::Producer::Middleware]
    def middleware
      @middleware ||= config.middleware
    end

    # Returns the variant currently in effect for dispatches on the current fiber.
    #
    # While executing inside a variant-wrapped call (any method invoked on the object returned by
    # {#with} / {#variant}), this returns that variant; otherwise it returns the producer's default
    # variant. It is primarily useful to middleware and instrumentation listeners that run
    # synchronously within a dispatch and want to read the effective per-dispatch settings, such as
    # `#topic_config`, `#max_wait_timeout` or `#default?`.
    #
    # @return [WaterDrop::Producer::Variant] the variant active for the current dispatch on this
    #   fiber, or the producer's default variant when not inside a variant-wrapped call
    #
    # @note The lookup is fiber-local and scoped to a single dispatch; it does not represent a
    #   producer-wide setting. Called from arbitrary code outside a variant-wrapped call it always
    #   returns the default variant. It is likewise not meaningful from asynchronous delivery
    #   callbacks (which run on the poller thread, a different fiber) - there it also returns the
    #   default variant, not the variant the acknowledged message was dispatched with.
    def current_variant
      # Read-only: the fiber-local hash is created by the variant wrapper methods only when needed,
      # so we must not allocate it here just to look up a variant that may not exist.
      clients = Fiber.current.waterdrop_clients
      (clients && clients[id]) || @default_variant
    end

    # Disconnects the producer from Kafka while keeping it configured for potential reconnection
    #
    # This method safely disconnects the underlying Kafka client while preserving the producer's
    # configuration. Unlike `#close`, this allows the producer to be reconnected later by calling
    # methods that require the client. The disconnection will only proceed if certain safety
    # conditions are met.
    #
    # This API can be used to preserve connections on low-intensity producer instances, etc.
    #
    # @return [Boolean] true if disconnection was successful, false if disconnection was not
    #   possible due to safety conditions (active transactions, ongoing operations, pending
    #   messages in buffer, or if already disconnected)
    #
    # @note This method will refuse to disconnect if:
    #   - There are pending messages in the internal buffer
    #   - There are operations currently in progress
    #   - A transaction is currently active
    #   - The client is not currently connected
    #   - Required mutexes are locked by other operations
    #
    # @note After successful disconnection, the producer status changes to disconnected but
    #   remains configured, allowing for future reconnection when client access is needed.
    def disconnect
      return false unless disconnectable?

      # Use the same mutex pattern as the regular close method to prevent race conditions
      @transaction_mutex.synchronize do
        @operating_mutex.synchronize do
          @buffer_mutex.synchronize do
            return false unless @client
            return false unless @status.connected?
            return false unless @messages.empty?
            return false unless @operations_in_progress.value.zero?

            @status.disconnecting!
            @monitor.instrument("producer.disconnecting", producer_id: id)

            @monitor.instrument("producer.disconnected", producer_id: id) do
              # Unregister from poller before closing if fiber polling is enabled
              unregister_from_poller

              # Close the client
              @client.close
              @client = nil

              # Reset connection status but keep producer configured
              @status.disconnected!
            end

            true
          end
        end
      end
    end

    # Is the producer in a state from which we can disconnect
    #
    # @return [Boolean] is producer in a state that potentially allows for a disconnect
    #
    # @note This is a best effort method. The proper checks happen also when disconnecting behind
    #   all the needed mutexes
    def disconnectable?
      return false unless @client
      return false unless @status.connected?
      return false unless @messages.empty?
      return false if @transaction_mutex.locked?
      return false if @operating_mutex.locked?

      true
    end

    # Flushes the buffers in a sync way and closes the producer
    # @param force [Boolean] should we force closing even with outstanding messages after the
    #   max wait timeout
    def close(force: false)
      # If the client was built in a different process, we have been forked. The client and its
      # native resources belong to the parent, so we must never flush or close them here: with the
      # real rdkafka client that is rd_kafka_destroy on a fork-inherited handle (undefined behavior),
      # and it would also tear down a client the parent still uses. We just drop our references and
      # the inherited finalizer and return. This matters most for the GC finalizer, which is
      # inherited across fork and would otherwise run #close in the child at exit.
      if @client && @pid != Process.pid
        @client = nil
        ObjectSpace.undefine_finalizer(id)

        return
      end

      # When closing from within the FD poller thread (e.g., from a callback like
      # message.acknowledged or error.occurred), we must delegate to a background thread.
      # Close performs flush which waits for delivery reports, but delivery reports require
      # the poller to poll. Since we're ON the poller thread inside a callback, this would
      # deadlock. Spawning a thread allows the callback to return, letting the poller continue.
      if fd_polling? && poller.in_poller_thread?
        Thread.new { close(force: force) }
        return
      end

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
            "producer.closed",
            producer_id: id
          ) do
            @status.closing!
            @monitor.instrument("producer.closing", producer_id: id)

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
            #
            # This is best-effort: if a buffered message surfaces a terminal error here (for example
            # a fatal error on an idempotent producer), we must still proceed to close the underlying
            # client. Otherwise the native client and its resources would leak and the producer would
            # stay stuck in the `:closing` state. The failure is already surfaced via the
            # `error.occurred` instrumentation emitted by the dispatch itself, so swallowing the
            # re-raised wrapper here does not hide it.
            begin
              flush(true)
            rescue Errors::ProduceError
              nil
            end

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

              # Unregister from poller before closing if fiber polling is enabled
              unregister_from_poller

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
    rescue ThreadError => e
      # Ruby raises ThreadError with this specific message when Mutex#synchronize (or #lock) is
      # called from a signal trap context. There is no public Ruby API to detect trap context
      # proactively - Thread.current is the same object as the main thread, its status is "run",
      # and caller_locations contains no "trap" frame. The only observable difference is that
      # blocking mutex operations raise this error. We re-raise anything else (e.g.
      # "deadlock; recursive locking") so those are not silently swallowed.
      #
      # Puma's `after_stopped` DSL hook in single mode is one example that fires in trap context.
      # We escape by delegating to a background thread and joining so the caller blocks until the
      # producer is fully closed.
      raise unless e.message == "can't be called from trap context"

      Thread.new { close(force: force) }.value
    end

    # Closes the producer with forced close after timeout, purging any outgoing data
    def close!
      close(force: true)
    end

    # @return [String] mutex-safe inspect details
    def inspect
      # Basic info that's always safe to access
      parts = []
      parts << "id=#{@id.inspect}"
      parts << "status=#{@status}" if @status

      # Try to get buffer info safely
      if @buffer_mutex.try_lock
        begin
          parts << "buffer_size=#{@messages.size}"
        ensure
          @buffer_mutex.unlock
        end
      else
        parts << "buffer_size=busy"
      end

      # Check if client is connected without triggering connection
      parts << if @status.connected?
        "connected=true"
      else
        "connected=false"
      end

      parts << "operations=#{@operations_in_progress.value}"
      parts << "in_transaction=true" if @transaction_mutex.locked?

      "#<#{self.class.name}:#{format("%#x", object_id)} #{parts.join(" ")}>"
    end

    # @return [Boolean] true if FD-based polling mode is enabled
    def fd_polling?
      return @fd_polling unless @fd_polling.nil?
      return false unless config

      @fd_polling = config.polling.mode == :fd
    end

    # Returns the poller instance for this producer
    # @return [WaterDrop::Polling::Poller] custom poller if configured, otherwise the global
    #   singleton poller
    def poller
      return @poller unless @poller.nil?
      return nil unless config

      @poller = config.polling.poller || Polling::Poller.instance
    end

    private

    # Ensures that we don't run any operations when the producer is not configured or when it
    # was already closed
    def ensure_active!
      # Capture the lifecycle state once. Another thread may be transitioning the producer between
      # states (for example configured -> connected while reloading the client after a fatal error),
      # and issuing several @status predicate calls here could otherwise observe an inconsistent mix
      # of states and raise StatusInvalidError for what is in fact a valid, active producer.
      state = @status.to_sym

      return if Status::ACTIVE_STATES.include?(state)
      return if state == :closing && @operating_mutex.owned?

      raise Errors::ProducerNotConfiguredError, id if state == :initial
      raise Errors::ProducerClosedError, id if state == :closing
      raise Errors::ProducerClosedError, id if state == :closed

      # This should never happen
      raise Errors::StatusInvalidError, [id, state.to_s]
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
    # @param max_wait_timeout [Integer] max wait timeout in ms. Resolved from the current variant
    #   by default but can be passed in by batch operations that wait on many handlers, so the
    #   variant is not re-resolved for each of them.
    # @param raise_response_error [Boolean] should we raise the response error after we receive the
    #   final result and it is an error.
    def wait(handler, max_wait_timeout: current_variant.max_wait_timeout, raise_response_error: true)
      handler.wait(
        max_wait_timeout_ms: max_wait_timeout,
        raise_response_error: raise_response_error
      )
    end

    # Dispatches a message, ensuring transactional producers take the transaction lock before the
    # operation is counted.
    #
    # For a transactional producer we wrap the whole dispatch (including the operations-counter
    # bookkeeping) in `transaction`, so `@transaction_mutex` is acquired BEFORE
    # `@operations_in_progress` is incremented. This makes `#produce` acquire locks in the same
    # order as `#close` (`@transaction_mutex` -> `@operating_mutex` -> operations counter) and
    # removes a lock-order inversion: without it, a dispatch that had already counted itself could
    # block forever on `@transaction_mutex` held by a concurrent `#close` that was itself waiting
    # for the operations counter to drain. When we already own the transaction lock (inside an
    # explicit transaction block or the closing flush) the order is already correct, so we dispatch
    # directly.
    #
    # @param message [Hash] message we want to send
    # @param label [String] short name of the public dispatch method (e.g. `"produce_sync"`) that
    #   we surface in the `message.*` queue-full error type. Passed explicitly by each public entry
    #   point so we never have to walk the call stack to recover it (the number of internal frames
    #   varies because the transactional path wraps the dispatch in a `transaction`).
    def produce(message, label)
      if transactional? && !@transaction_mutex.owned?
        transaction { produce_to_client(message, label) }
      else
        produce_to_client(message, label)
      end
    end

    # Runs the client produce method with a given message
    #
    # @param message [Hash] message we want to send
    # @param label [String] public dispatch method name used in the queue-full error type
    def produce_to_client(message, label)
      produce_time ||= monotonic_now

      # This can happen only during flushing on closing, in case like this we don't have to
      # synchronize because we already own the lock
      if @operating_mutex.owned?
        @operations_in_progress.increment
      else
        @operating_mutex.synchronize { @operations_in_progress.increment }
        ensure_active!
      end

      # The variant is fiber-local and cannot change mid-call, so we resolve it once instead of
      # paying the fiber-local lookup for each usage
      variant = current_variant

      # We basically only duplicate the message hash only if it is needed.
      # It is needed when user is using a custom settings variant or when symbol is provided as
      # the topic name. We should never mutate user input message as it may be a hash that the
      # user is using for some other operations
      if message[:topic].is_a?(Symbol) || !variant.default?
        message = message.dup
        # In case someone defines topic as a symbol, we need to convert it into a string as
        # librdkafka does not accept symbols
        message[:topic] = message[:topic].to_s
        message[:topic_config] = variant.topic_config
      end

      result = if transactional?
        transaction { client.produce(**message) }
      else
        client.produce(**message)
      end

      # Remember the first delivery handle of the current transaction. Aborting while the very first
      # produce is still in flight is what triggers librdkafka#4849, so the abort path waits on this
      # handle to confirm the transaction is registered at the coordinator. See
      # `#transactional_await_first_delivery`.
      @transaction_first_handle ||= result if transactional? && @transaction_mutex.owned?

      # Reset attempts counter on successful produce
      @idempotent_fatal_error_attempts = 0

      result
    rescue Rdkafka::ClosedProducerError, Rdkafka::ClosedInnerError
      # A concurrent idempotent fatal-error reload closed the underlying client while this produce
      # was already in flight. Unlike `#close`/`#disconnect`, which drain `@operations_in_progress`
      # before closing the client, the idempotent reload swaps `@client` out from under sibling
      # threads that have already passed the `@operating_mutex` gate and are inside `client.produce`.
      # Racing with `@client.close`, such a thread sees either a producer already flagged closed
      # (`ClosedProducerError`) or a nil inner librdkafka handle (`ClosedInnerError`) - which one
      # depends purely on how far `close` has progressed.
      #
      # This is a benign, recoverable transient rather than a produce failure: the client has just
      # been (or is being) rebuilt, so we retry the dispatch against the fresh client instead of
      # surfacing a raw "closed producer" error to the caller - which would defeat the whole point
      # of the transparent reload. Only the idempotent reload path closes the client with produces
      # in flight, so we scope the retry to that configuration; anywhere else a closed client is a
      # genuine error and must propagate. The retried pass re-runs `ensure_active!`, which raises
      # `ProducerClosedError` once the producer is genuinely closing/closed, so this cannot spin
      # forever.
      raise unless config.reload_on_idempotent_fatal_error
      raise if transactional?

      @operations_in_progress.decrement

      retry
    rescue SUPPORTED_FLOW_ERRORS.first => e
      # Check if this is a fatal error on an idempotent producer and we should reload.
      #
      # We must never reload while closing. During `#close` the final `flush` runs while this
      # thread already owns `@operating_mutex`; the idempotent reload re-acquires that same mutex,
      # which Ruby rejects with `ThreadError: deadlock; recursive locking`, and it would also try to
      # rebuild the very client we are tearing down. In that case we let the error propagate so
      # `#close` can finish and release the underlying client.
      if idempotent_reloadable?(e) && !@operating_mutex.owned?
        # Check if we've exceeded max reload attempts
        raise unless idempotent_retryable?

        # Increment attempts before reload
        @idempotent_fatal_error_attempts += 1

        # Instrument error.occurred before attempting reload for visibility
        @monitor.instrument(
          "error.occurred",
          producer_id: id,
          error: e,
          type: "librdkafka.idempotent_fatal_error",
          attempt: @idempotent_fatal_error_attempts
        )

        # Attempt to reload the producer
        idempotent_reload_client_on_fatal_error(@idempotent_fatal_error_attempts, e)

        # Wait before retrying to avoid rapid reload loops
        sleep(@config.wait_backoff_on_idempotent_fatal_error / 1_000.0)

        # After reload, retry the produce operation
        @operations_in_progress.decrement

        retry
      end

      # Unless we want to wait and retry and it's a full queue, we raise normally
      raise unless @config.wait_on_queue_full
      raise unless e.code == :queue_full
      # If we're running for longer than the timeout, we need to re-raise the queue full.
      # This will prevent from situation where cluster is down forever and we just retry and retry
      # in an infinite loop, effectively hanging the processing
      raise unless monotonic_now - produce_time < @config.wait_timeout_on_queue_full

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
            "error.occurred",
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

    # Reloads the client
    # @note This should be used only within proper mutexes internally
    def reload!
      @client.flush(current_variant.max_wait_timeout)
      purge
      # Unregister from poller before closing if fiber polling is enabled
      unregister_from_poller
      @client.close
      @client = nil
      @status.configured!
    end

    # Unregisters this producer from its poller
    #
    # @note We only unregister when fd_polling? is true because thread-mode producers never
    #   register with the Poller. The Poller.unregister method handles unregistered producers
    #   gracefully, but this guard avoids making unnecessary unregister calls in thread mode.
    def unregister_from_poller
      return unless fd_polling?

      poller.unregister(self)
    end
  end
end
