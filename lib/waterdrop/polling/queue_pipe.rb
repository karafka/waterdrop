# frozen_string_literal: true

module WaterDrop
  module Polling
    # A pipe connected to librdkafka's queue event notification system
    # When events (delivery reports, statistics) arrive, librdkafka writes to the pipe
    # allowing IO.select to wake up immediately
    #
    # This pipe is also used by WaterDrop to signal:
    # - Continue: when poll hits time limit but more events remain
    # - Close: when producer is being closed (combined with @closing flag in State)
    #
    # Reusing the same pipe reduces file descriptors and IO.select monitoring overhead
    class QueuePipe
      # @return [IO] the readable end of the pipe for use with IO.select
      attr_reader :reader

      # Creates a new queue pipe and connects it to the client's event queue
      # @param client [Rdkafka::Producer] the rdkafka client
      # @raise [StandardError] if enable_queue_io_events fails
      def initialize(client)
        @reader, @writer = IO.pipe

        # Tell librdkafka to write to our pipe when events arrive on the main queue
        client.enable_queue_io_events(@writer.fileno)
      end

      # Signals by writing a byte to the pipe
      # Used to wake IO.select for continue/close signals
      # Thread-safe and non-blocking; silently ignores errors
      def signal
        @writer.write_nonblock("W", exception: false)
      rescue IOError, Errno::EBADF
        # Pipe closed
      end

      # Drains all pending bytes from the pipe
      # Called after IO.select returns to clear the notification
      # Uses a single large read to drain in one syscall (pipe buffers are typically 64KB)
      def drain
        @reader.read_nonblock(1_048_576, exception: false)
      rescue IOError, Errno::EBADF
        # Pipe closed during drain
      end

      # Closes both ends of the pipe
      def close
        close_io(@reader)
        close_io(@writer)
      end

      private

      # Safely closes an IO object
      # @param io [IO] the IO to close
      def close_io(io)
        io.close
      rescue IOError, Errno::EBADF
        # Already closed, ignore
      end
    end
  end
end
