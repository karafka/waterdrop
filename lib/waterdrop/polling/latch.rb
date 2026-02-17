# frozen_string_literal: true

module WaterDrop
  module Polling
    # A thread-safe latch for synchronizing producer close operations
    #
    # When a producer is closed, two threads are involved:
    # 1. The caller thread (user code calling producer.close)
    # 2. The poller thread (background thread running IO.select)
    #
    # The close sequence:
    # 1. Caller calls producer.close -> unregister_from_poller -> Poller#unregister
    # 2. Poller#unregister signals via control pipe and calls state.wait_for_close (blocks on latch)
    # 3. Poller thread receives control signal, drains queue, calls state.close
    # 4. state.close releases the latch via release!
    # 5. Caller's wait_for_close returns, unregister completes
    #
    # This ensures the producer is fully drained and removed from the poller
    # before returning control to the caller, preventing race conditions.
    class Latch
      def initialize
        @mutex = Mutex.new
        @cv = ConditionVariable.new
        @released = false
      end

      # Releases the latch and wakes any waiting threads
      def release!
        @mutex.synchronize do
          @released = true
          @cv.broadcast
        end
      end

      # Waits until the latch is released
      # Returns immediately if already released
      def wait
        @mutex.synchronize do
          return if @released

          @cv.wait(@mutex)
        end
      end

      # @return [Boolean] whether the latch has been released
      def released?
        @released
      end
    end
  end
end
