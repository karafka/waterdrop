# frozen_string_literal: true

require "singleton"

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
    class Poller
      include Singleton
      include ::Karafka::Core::Helpers::Time

      # Timeout for IO.select in seconds
      # This is a fallback - normally we wake via:
      # - queue_read: librdkafka signals when delivery reports or statistics arrive
      # - continue_read: self-signal when we hit time limit but have more work
      # - control_read: signal on producer close/shutdown
      # The timeout ensures OAuth/stats callbacks fire even when idle
      POLL_TIMEOUT = 1.0

      def initialize
        @mutex = Mutex.new
        @producers = {}
        @monitors = {}
        @thread = nil
        @shutdown = false
        @pid = Process.pid

        # Cached IO list for select - rebuilt only when producers change
        @cached_ios = []
        @cached_io_to_state = {}
        @ios_dirty = true

        # Wakeup pipe to signal the poller when new producers are registered
        # This allows the thread to wake up from IO.select when a new producer arrives
        @wakeup_read, @wakeup_write = IO.pipe
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
          producer.config.polling.fd.max_time
        )

        @mutex.synchronize do
          @producers[producer.id] = state
          @monitors[producer.id] = producer.monitor
          @ios_dirty = true
          ensure_thread_running!
          signal_wakeup
        end

        producer.monitor.instrument(
          "poller.producer_registered",
          producer_id: producer.id
        )
      end

      # Unregisters a producer from the poller
      # This method blocks until the producer is fully removed from the poller
      # to prevent race conditions when disconnect/reconnect happens in quick succession
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

        # Clean up monitor reference
        @mutex.synchronize { @monitors.delete(producer.id) }

        producer.monitor.instrument(
          "poller.producer_unregistered",
          producer_id: producer.id
        )
      end

      # Gracefully shuts down the poller thread
      def shutdown
        @mutex.synchronize do
          @shutdown = true
          # Signal all producers to close
          @producers.each_value(&:signal_close)
        end

        @thread&.join(5)
        @thread = nil
      end

      private

      # Ensures we're in the same process (for fork safety)
      def ensure_same_process!
        return if @pid == Process.pid

        # Reset state after fork
        @mutex = Mutex.new
        @producers = {}
        @monitors = {}
        @thread = nil
        @shutdown = false
        @pid = Process.pid
        @cached_ios = []
        @cached_io_to_state = {}
        @ios_dirty = true
        @wakeup_read, @wakeup_write = IO.pipe
      end

      # Signals the poller thread to wake up (e.g., when a new producer is registered)
      # Must be called within @mutex.synchronize
      def signal_wakeup
        @wakeup_write.write_nonblock("W")
      rescue IOError, Errno::EPIPE, Errno::EAGAIN
        # Pipe closed or full, ignore
      end

      # Drains the wakeup pipe
      def drain_wakeup_pipe
        loop do
          @wakeup_read.read_nonblock(1024)
        end
      rescue IO::WaitReadable, IOError
        # No more data or pipe closed
      end

      # Ensures the polling thread is running
      # Must be called within @mutex.synchronize
      def ensure_thread_running!
        return if @thread&.alive?

        @shutdown = false
        @thread = Thread.new { polling_loop }
        @thread.name = "waterdrop_poller"
        @thread.abort_on_exception = false
        @thread.report_on_exception = true
      end

      # Main polling loop that runs in a dedicated thread
      def polling_loop
        loop do
          break if @shutdown

          # Collect readable IOs (queue FDs + control pipes)
          readable_ios, io_to_state = collect_readable_ios

          # When no producers registered, wait on wakeup pipe for new registrations
          # This allows quick response when a new producer registers
          # Exit if still empty after timeout (no new registrations)
          if readable_ios.empty?
            begin
              ready = IO.select([@wakeup_read], nil, nil, POLL_TIMEOUT)
              drain_wakeup_pipe if ready
            rescue IOError
              # Pipe closed, will be handled on next iteration
            end
            # Re-collect after wakeup to see if new producers registered
            readable_ios, io_to_state = collect_readable_ios
            break if readable_ios.empty?
          end

          # Use IO.select for all cases - yields to fiber scheduler
          poll_with_select(readable_ios, io_to_state)
        end
      rescue => e
        # Report error to all registered producers' monitors
        broadcast_error("poller.polling_loop", e)
      end

      # Broadcasts an error to all registered producers' monitors
      # @param type [String] error type identifier
      # @param error [Exception] the error to report
      def broadcast_error(type, error)
        monitors = @mutex.synchronize { @monitors.values.dup }

        monitors.each do |monitor|
          monitor.instrument(
            "error.occurred",
            type: type,
            error: error
          )
        rescue
          # Ignore errors in error reporting to avoid cascading failures
        end
      end

      # Collects all IOs to monitor and builds a mapping from IO to State
      # Uses cached arrays when possible to avoid allocations in the hot path
      # @return [Array<Array<IO>, Hash{IO => State}>] tuple of ios list and io-to-state mapping
      def collect_readable_ios
        @mutex.synchronize do
          return [@cached_ios, @cached_io_to_state] unless @ios_dirty

          @cached_ios = []
          @cached_io_to_state = {}

          @producers.each_value do |state|
            next if state.closed?

            state.ios.each do |io|
              @cached_ios << io
              @cached_io_to_state[io] = state
            end
          end

          @ios_dirty = false
        end

        [@cached_ios, @cached_io_to_state]
      end

      # Poll producers using IO.select for efficient multiplexing
      # @param readable_ios [Array<IO>] IOs to monitor
      # @param io_to_state [Hash{IO => State}] mapping from IO to state
      def poll_with_select(readable_ios, io_to_state)
        begin
          ready = IO.select(readable_ios, nil, nil, POLL_TIMEOUT)
        rescue IOError, Errno::EBADF
          # An IO was closed, will be handled on next iteration
          return
        end

        if ready.nil?
          # Timeout: poll ALL producers to ensure OAuth/stats fire
          poll_all_producers
        else
          # FDs ready: handle close signals and poll active producers
          ready[0].each do |io|
            state = io_to_state[io]
            next unless state

            if io == state.control_read
              handle_close_signal(state)
            elsif io == state.queue_read
              # Drain the signaling pipe then poll
              state.drain_queue_pipe
              poll_producer(state)
            elsif io == state.continue_read
              # Continue signal - we hit time limit but have more work
              state.drain_continue_pipe
              poll_producer(state)
            end
          end
        end
      end

      # Polls all registered producers
      # Called when IO.select times out to ensure periodic polling happens
      def poll_all_producers
        states = @mutex.synchronize { @producers.values.dup }

        states.each do |state|
          next if state.closed?

          poll_producer(state)
        end
      end

      # Drains the producer's event queue by polling until empty or time quanta exceeded
      # Uses poll_drain_nb which acquires mutex once for all polls (more efficient)
      # @param state [State] the producer state
      # @return [Boolean] true if there may be more events (hit time limit), false if fully drained
      def poll_producer(state)
        return false if state.closed?

        # poll_drain_nb returns true if time limit was hit (more events may remain)
        hit_limit = state.client.poll_drain_nb(state.max_poll_time)

        if hit_limit
          # Hit time limit but still have events - signal to continue
          state.signal_continue
          return true
        end

        false
      rescue Rdkafka::ClosedProducerError
        # Producer was closed, will be cleaned up
        false
      end

      # Handles a close signal from a producer's control pipe
      # @param state [State] the producer state
      def handle_close_signal(state)
        # Read and discard the signal byte
        state.control_read.read_nonblock(1)
      rescue IOError, Errno::EAGAIN
        # Ignore read errors (EOFError is a subclass of IOError)
      ensure
        # Drain remaining events before closing
        # This matches rdkafka's native polling thread behavior: keep polling until outq_len is zero
        drain_producer_queue(state)

        # Remove producer from registry and clean up
        @mutex.synchronize do
          @producers.delete(state.producer_id)
          @ios_dirty = true
        end

        state.close
      end

      # Drains the producer's event queue completely before closing
      # Matches rdkafka's native polling thread behavior: keep polling until queue_size is zero
      # @param state [State] the producer state
      def drain_producer_queue(state)
        loop do
          break if state.client.queue_size.zero?

          state.client.poll_drain_nb(state.max_poll_time)
        end
      rescue Rdkafka::ClosedProducerError
        # Producer was already closed, nothing more to drain
      end
    end
  end
end
