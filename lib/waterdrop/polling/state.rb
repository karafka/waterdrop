# frozen_string_literal: true

module WaterDrop
  # Namespace for FD-based polling components
  # Contains the global Poller singleton and State class for managing producer polling
  module Polling
    # Holds the state for a registered producer in the poller
    # Each producer has its own State instance that tracks:
    # - The producer ID and client reference
    # - Queue pipe for IO.select monitoring (librdkafka writes to it when events ready)
    # - Control pipe for close signaling from any thread
    # - Configuration (max poll time)
    class State
      # @return [String] producer ID
      attr_reader :producer_id

      # @return [Rdkafka::Producer] the rdkafka client
      attr_reader :client

      # @return [IO] readable end of the control pipe
      attr_reader :control_read

      # @return [IO] writable end of the control pipe
      attr_reader :control_write

      # @return [IO, nil] readable end of the queue event pipe (nil if not available)
      attr_reader :queue_read

      # @return [Array<IO>] cached list of IOs to monitor (avoids allocation on each call)
      attr_reader :ios

      # @return [IO, nil] readable end of the continue pipe (for self-signaling)
      attr_reader :continue_read

      # @return [Integer] max milliseconds to poll this producer per cycle
      attr_reader :max_poll_time

      # Creates a new state for a producer
      # @param producer_id [String] unique producer ID
      # @param client [Rdkafka::Producer] the rdkafka producer client
      # @param monitor [Object] the producer's monitor for error reporting
      # @param max_poll_time [Integer] max time in ms to poll per cycle
      def initialize(producer_id, client, monitor, max_poll_time)
        @producer_id = producer_id
        @client = client
        @monitor = monitor
        @max_poll_time = max_poll_time
        @closed = false

        # Synchronization for waiting on close completion
        @close_mutex = Mutex.new
        @close_cv = ConditionVariable.new

        # Create control pipe for signaling close from any thread
        @control_read, @control_write = IO.pipe

        # Create continue pipe for self-signaling when we hit time limit but have more data
        # This avoids waiting for IO.select timeout when there's still work to do
        @continue_read, @continue_write = IO.pipe

        # Setup queue event pipe - librdkafka will write to this when events are ready
        setup_queue_pipe

        # Cache the IOs array to avoid allocation on each select loop
        @ios = build_ios_array
      end

      # Signals the poller to remove this producer
      # Called from any thread when the producer is being closed
      def signal_close
        return if @closed

        @control_write.write_nonblock("X")
      rescue IOError, Errno::EPIPE
        # Pipe already closed, ignore
      end

      # Signals that there's more work to do (hit time limit but queue not empty)
      # This wakes up IO.select immediately instead of waiting for timeout
      def signal_continue
        return if @closed

        @continue_write.write_nonblock("C")
      rescue IOError, Errno::EPIPE, Errno::EAGAIN
        # Pipe closed or full, ignore
      end

      # Drains the continue pipe
      def drain_continue_pipe
        return unless @continue_read

        loop do
          @continue_read.read_nonblock(1024)
        end
      rescue IO::WaitReadable, IOError
        # No more data or pipe closed
      end

      # Closes all IOs associated with this state and signals any waiters
      def close
        @close_mutex.synchronize do
          return if @closed

          @closed = true

          close_io(@control_read)
          close_io(@control_write)
          close_io(@queue_read)
          close_io(@queue_write)
          close_io(@continue_read)
          close_io(@continue_write)

          # Signal any threads waiting for close completion
          @close_cv.broadcast
        end
      end

      # Waits for this state to be closed
      # Used by unregister to ensure synchronous cleanup before returning
      # @param timeout [Float] maximum time to wait in seconds (default: 5)
      def wait_for_close(timeout = 5)
        @close_mutex.synchronize do
          return if @closed

          @close_cv.wait(@close_mutex, timeout)
        end
      end

      # @return [Boolean] whether this state has been closed
      def closed?
        @closed
      end

      # Drains any pending bytes from the queue pipe
      # Called before polling to clear the signaling pipe
      def drain_queue_pipe
        return unless @queue_read

        loop do
          @queue_read.read_nonblock(1024)
        end
      rescue IO::WaitReadable, IOError
        # No more data or pipe closed
      end

      private

      # Builds the cached IOs array for IO.select
      # @return [Array<IO>] array of IOs to monitor
      def build_ios_array
        ios = [@control_read, @continue_read]
        ios << @queue_read if @queue_read
        ios.freeze
      end

      # Safely closes an IO object
      # @param io [IO, nil] the IO to close
      def close_io(io)
        io&.close
      rescue IOError
        # Already closed, ignore
      end

      # Sets up a pipe for queue event notification
      # We give librdkafka the write end, and monitor the read end with IO.select
      # For producers, delivery reports go to the MAIN queue (not background queue)
      def setup_queue_pipe
        # Create pipe for queue events
        @queue_read, @queue_write = IO.pipe

        # Tell librdkafka to write to our pipe when events arrive on the main queue
        @client.enable_queue_io_events(@queue_write.fileno)
      rescue => e
        # If we can't setup the queue pipe, we'll rely on periodic polling
        # Report the error via the producer's monitor
        @monitor.instrument(
          "error.occurred",
          producer_id: @producer_id,
          type: "poller.queue_pipe_setup",
          error: e
        )

        close_io(@queue_read)
        close_io(@queue_write)
        @queue_read = nil
        @queue_write = nil
      end
    end
  end
end
