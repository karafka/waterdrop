# frozen_string_literal: true

module WaterDrop
  # Namespace for FD-based polling components
  # Contains the global Poller singleton and State class for managing producer polling
  module Polling
    # Global poller singleton that manages a single polling thread for all FD-mode producers
    # This replaces librdkafka's native background polling threads with a single Ruby thread
    # that uses IO.select for efficient multiplexing
    #
    # Spawning one thread per producer is acceptable for 1-2 producers but in case of a system
    # with several (transactional for example) the cost becomes bigger and bigger.
    #
    # This implementation handles things by being event-driven instead of GVL releasing blocking.
    #
    # @note Newly registered producers may experience up to 1 second delay before their first
    #   poll cycle, as the poller thread only rebuilds its IO list when IO.select times out.
    #   This is acceptable because producers are expected to be long-lived and the initial
    #   connection overhead to Kafka typically exceeds this delay anyway.
    class Poller
      include Singleton
      include ::Karafka::Core::Helpers::Time

      # Timeout for IO.select in seconds
      # Normally we wake immediately when librdkafka signals events via queue pipe
      # The timeout ensures OAuth/stats callbacks fire even when idle
      POLL_TIMEOUT_S = 1.0

      # Initial backoff in ms after an error
      BACKOFF_MIN_MS = 100

      # Maximum backoff in ms (30 seconds)
      BACKOFF_MAX_MS = 30_000

      private_constant :POLL_TIMEOUT_S, :BACKOFF_MIN_MS, :BACKOFF_MAX_MS

      def initialize
        @mutex = Mutex.new
        @producers = {}
        @thread = nil
        @shutdown = false
        @pid = Process.pid

        # Cached collections - rebuilt only when producers change
        @cached_ios = []
        @cached_io_to_state = {}
        @cached_states = []
        @cached_result = nil
        @ios_dirty = true
      end

      # Registers a producer with the poller
      # @param producer [WaterDrop::Producer] the producer instance
      # @param client [Rdkafka::Producer] the rdkafka client
      def register(producer, client)
        ensure_same_process!

        state = State.new(
          producer.id,
          client,
          producer.monitor,
          producer.config.polling.fd.max_time,
          producer.config.polling.fd.periodic_poll_interval
        )

        @mutex.synchronize do
          @producers[producer.id] = state
          @ios_dirty = true
          # Reset shutdown flag in case thread is exiting but hasn't yet
          # This prevents race where new producer is closed by exiting thread
          @shutdown = false
          ensure_thread_running!
        end

        producer.monitor.instrument(
          "poller.producer_registered",
          producer_id: producer.id
        )
      end

      # Unregisters a producer from the poller
      # This method blocks until the producer is fully removed from the poller
      # to prevent race conditions when disconnect/reconnect happens in quick succession
      # This matches the threaded polling behavior which drains without timeout
      # @param producer [WaterDrop::Producer] the producer instance
      def unregister(producer)
        state = @mutex.synchronize { @producers[producer.id] }

        return unless state

        # Signal the poller thread to handle removal
        state.signal_close

        # Wait for the state to be fully closed by the poller thread
        # This prevents race conditions where a new registration with the same
        # producer_id could be deleted by a pending close signal
        state.wait_for_close

        producer.monitor.instrument(
          "poller.producer_unregistered",
          producer_id: producer.id
        )
      end

      private

      # Ensures we're in the same process (for fork safety)
      def ensure_same_process!
        return if @pid == Process.pid

        # Reset state after fork - parent's thread and producers are not valid in child
        @mutex = Mutex.new
        @producers = {}
        @thread = nil
        @shutdown = false
        @pid = Process.pid
        @cached_ios = []
        @cached_io_to_state = {}
        @cached_states = []
        @cached_result = nil
        @ios_dirty = true
      end

      # Ensures the polling thread is running
      # Must be called within @mutex.synchronize
      def ensure_thread_running!
        return if @thread&.alive?

        @shutdown = false
        @thread = Thread.new { polling_loop }
        @thread.name = "waterdrop.poller"
      end

      # Main polling loop that runs in a dedicated thread
      def polling_loop
        backoff_ms = 0

        loop do
          break if @shutdown

          # Apply backoff from previous error
          if backoff_ms > 0
            sleep(backoff_ms / 1_000.0)
            backoff_ms = 0
          end

          # Collect readable IOs (queue FDs)
          readable_ios, io_to_state = collect_readable_ios

          # Exit when no producers registered
          # New registrations will start a fresh thread via ensure_thread_running!
          break if readable_ios.empty?

          poll_with_select(readable_ios, io_to_state)
        rescue => e
          # Report error and apply exponential backoff to prevent spam
          broadcast_error("poller.polling_loop", e)
          backoff_ms = backoff_ms.zero? ? BACKOFF_MIN_MS : [backoff_ms * 2, BACKOFF_MAX_MS].min
        end
      ensure
        # When the poller thread exits (error or clean shutdown), close all remaining states
        # This releases any latches that might be waiting in unregister calls
        close_all_states
      end

      # Broadcasts an error to all registered producers' monitors
      # @param type [String] error type identifier
      # @param error [Exception] the error to report
      def broadcast_error(type, error)
        @cached_states.each do |state|
          state.monitor.instrument(
            "error.occurred",
            type: type,
            error: error
          )
        end
      end

      # Collects all IOs to monitor and builds a mapping from IO to State
      # Uses cached arrays when possible to avoid allocations in the hot path
      # @return [Array<Array<IO>, Hash{IO => State}, Array<State>>] tuple of ios, io-to-state map, states
      def collect_readable_ios
        # Fast path: return cached result if not dirty (no mutex needed)
        # Safe because @cached_result is frozen and assigned atomically
        return @cached_result unless @ios_dirty

        @mutex.synchronize do
          @cached_ios = []
          @cached_io_to_state = {}
          @cached_states = []

          @producers.each_value do |state|
            io = state.io
            @cached_ios << io
            @cached_io_to_state[io] = state
            @cached_states << state
          end

          @cached_result = [@cached_ios, @cached_io_to_state, @cached_states].freeze
          @ios_dirty = false
        end

        @cached_result
      end

      # Poll producers using IO.select for efficient multiplexing
      # @param readable_ios [Array<IO>] IOs to monitor
      # @param io_to_state [Hash{IO => State}] mapping from IO to state
      def poll_with_select(readable_ios, io_to_state)
        begin
          ready = IO.select(readable_ios, nil, nil, POLL_TIMEOUT_S)
        rescue IOError, Errno::EBADF
          # An IO was closed - mark dirty to rebuild on next iteration
          @ios_dirty = true
          return
        end

        if ready.nil?
          # Timeout: poll ALL producers to ensure OAuth/stats fire
          poll_all_producers
        else
          # FDs ready: handle close signals and poll active producers
          any_polled = false

          ready[0].each do |io|
            state = io_to_state[io]
            next unless state

            # Drain the pipe first (clears librdkafka signals + our signals)
            state.drain

            # Check if this producer is closing (flag set before signal)
            if state.closing?
              handle_close_signal(state)
            else
              poll_producer(state)
              any_polled = true
            end
          end

          # Check for stale producers when actively polling
          # Skip when single producer (most common case) - no other producers to become stale
          # (ensures OAuth/stats fire for idle producers when others are busy)
          poll_stale_producers if any_polled && @cached_states.size > 1
        end
      end

      # Polls all registered producers
      # Called when IO.select times out to ensure periodic polling happens
      # This ensures OAuth token refresh and statistics callbacks fire for all producers
      def poll_all_producers
        @cached_states.each { |state| poll_producer(state) }
      end

      # Polls producers that haven't been polled recently
      # Called when processing continue signals to prevent starvation of idle producers
      # when one producer is very busy
      # Each State internally throttles the check to avoid excessive overhead
      def poll_stale_producers
        @cached_states.each do |state|
          poll_producer(state) if state.needs_periodic_poll?
        end
      end

      # Drains the producer's event queue by polling until empty or time quanta exceeded
      # @param state [State] the producer state
      def poll_producer(state)
        # poll_drain_nb returns:
        # - true when queue is empty (fully drained)
        # - false when timeout hit (more events may remain)
        drained = state.poll
        state.mark_polled!

        # Hit time limit but still have events - signal to continue polling
        state.signal_continue unless drained
      rescue Rdkafka::ClosedProducerError
        # Producer was closed, will be cleaned up
      end

      # Handles a close signal from a producer
      # @param state [State] the producer state
      def handle_close_signal(state)
        # Drain remaining events before closing
        # This matches rdkafka's native polling thread behavior: keep polling until outq_len is zero
        drain_producer_queue(state)

        # Remove producer from registry and clean up
        # If this was the last producer, signal shutdown to stop the thread immediately
        @mutex.synchronize do
          @producers.delete(state.producer_id)
          @ios_dirty = true

          # Stop thread immediately when last producer unregisters to prevent resource leakage
          @shutdown = true if @producers.empty?
        end

        state.close
      end

      # Closes all remaining producer states
      # Called when the poller thread exits to release any pending latches
      # This prevents deadlocks if producers are waiting in unregister
      def close_all_states
        states = @mutex.synchronize do
          to_close = @producers.values.dup
          @producers.clear
          @ios_dirty = true
          to_close
        end

        states.each do |state|
          state.close unless state.closed?
        rescue
          # Ignore errors during cleanup
        end
      end

      # Drains the producer's event queue completely before closing
      # Matches rdkafka's native polling thread behavior: keep polling until queue is empty
      # @param state [State] the producer state
      def drain_producer_queue(state)
        loop do
          break if state.queue_empty?

          state.poll
        end
      rescue Rdkafka::ClosedProducerError
        # Producer was already closed, nothing more to drain
      end
    end
  end
end
