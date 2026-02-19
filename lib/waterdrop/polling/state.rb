# frozen_string_literal: true

module WaterDrop
  module Polling
    # Holds the state for a registered producer in the poller
    # Each producer has its own State instance that tracks:
    # - The producer ID and client reference
    # - Queue pipe for IO.select monitoring (shared with librdkafka for efficiency)
    # - Configuration (max poll time)
    # - Last poll time for staleness detection
    # - Closing flag for shutdown signaling
    class State
      include ::Karafka::Core::Helpers::Time

      # @return [String] producer ID
      attr_reader :producer_id

      # @return [IO] the queue pipe reader for IO.select monitoring
      attr_reader :io

      # @return [Object] the producer's monitor for instrumentation
      attr_reader :monitor

      # Creates a new state for a producer
      # @param producer_id [String] unique producer ID
      # @param client [Rdkafka::Producer] the rdkafka producer client
      # @param monitor [Object] the producer's monitor for error reporting
      # @param max_poll_time [Integer] max time in ms to poll per cycle
      # @param periodic_poll_interval [Integer] max time in ms before this producer needs periodic poll
      # @raise [StandardError] if queue pipe setup fails (FD mode requires this to work)
      def initialize(producer_id, client, monitor, max_poll_time, periodic_poll_interval)
        @producer_id = producer_id
        @client = client
        @monitor = monitor
        @max_poll_time = max_poll_time
        @periodic_poll_interval = periodic_poll_interval
        # Initialize to 0 so first check always triggers (no nil handling needed)
        @last_poll_time = 0
        @last_stale_check = 0
        @last_stale_result = false

        # Closing flag - set by signal_close, checked by poller
        @closing = false

        # Latch for synchronizing close operations
        @close_latch = Latch.new

        # Queue pipe for all signaling (librdkafka events + continue + close)
        # Reusing one pipe reduces FDs and IO.select overhead
        @queue_pipe = QueuePipe.new(@client)

        # Cache reader reference for hot path performance
        @io = @queue_pipe.reader
      end

      # Drains the queue pipe
      # Called before polling to clear any pending signals
      def drain
        @queue_pipe.drain
      end

      # Polls the producer's event queue
      # @return [Boolean] true if no more events to process, false if stopped due to time limit
      def poll
        drained = true
        deadline = monotonic_now + @max_poll_time

        @client.events_poll_nb_each do |count|
          if count.zero?
            :stop
          elsif monotonic_now >= deadline
            drained = false
            :stop
          end
        end

        drained
      end

      # Checks if the producer's event queue is empty
      # @return [Boolean] true if queue is empty
      def queue_empty?
        @client.queue_size.zero?
      end

      # Minimum interval between stale checks to avoid excessive overhead
      STALE_CHECK_THROTTLE_MS = 100

      private_constant :STALE_CHECK_THROTTLE_MS

      # Marks this producer as having been polled
      # Called after polling to track staleness
      def mark_polled!
        @last_poll_time = monotonic_now
      end

      # Checks if this producer needs a periodic poll
      # Used to ensure OAuth/stats callbacks fire even when another producer is busy
      # Includes internal throttling to avoid excessive checks
      # @return [Boolean] true if the producer needs a periodic poll
      def needs_periodic_poll?
        now = monotonic_now

        # Throttle: return cached result if checked recently
        return @last_stale_result if (now - @last_stale_check) < STALE_CHECK_THROTTLE_MS

        @last_stale_check = now
        @last_stale_result = (now - @last_poll_time) >= @periodic_poll_interval
      end

      # Signals the poller to remove this producer
      # Called from any thread when the producer is being closed
      # Sets closing flag BEFORE signaling to ensure poller sees it
      def signal_close
        @closing = true
        @queue_pipe.signal
      end

      # Signals that there's more work to do (hit time limit but queue not empty)
      # This wakes up IO.select immediately instead of waiting for timeout
      def signal_continue
        @queue_pipe.signal
      end

      # @return [Boolean] whether this producer is being closed
      def closing?
        @closing
      end

      # Closes all resources and signals any waiters
      def close
        return if closed?

        @queue_pipe.close
        @close_latch.release!
      end

      # Waits for this state to be closed
      # Used by unregister to ensure synchronous cleanup before returning
      # This matches the threaded polling behavior which drains without timeout
      def wait_for_close
        @close_latch.wait
      end

      # @return [Boolean] whether this state has been closed
      def closed?
        @close_latch.released?
      end
    end
  end
end
