# frozen_string_literal: true

module WaterDrop
  class Producer
    module Buffer
      def buffer(message)
        ensure_active!
        @validator.call(message)

        @monitor.instrument(
          'message.buffered',
          producer: self,
          message: message
        ) { @buffer << message }
      end

      def buffer_many(messages)
        ensure_active!
        messages.each(&@validator)

        @monitor.instrument(
          'messages.buffered',
          producer: self,
          messages: messages
        ) do
          messages.each { |message| @buffer << message }
          messages
        end
      end

      def flush_async
        ensure_active!

        @monitor.instrument(
          'buffer.flushed_async',
          producer: self,
          buffer: @buffer
        ) { flush(false) }
      end

      def flush_sync
        ensure_active!

        @monitor.instrument(
          'buffer.flushed_sync',
          producer: self,
          buffer: @buffer
        ) { flush(true) }
      end

      def flush(sync)
        data_for_dispatch = nil
        dispatched = []

        @mutex.synchronize do
          data_for_dispatch = @buffer
          @buffer = Concurrent::Array.new
        end

        dispatched = data_for_dispatch.map { |message| @client.produce(message) }

        return dispatched unless sync

        dispatched.map { |handler| handler.wait(@config.wait_timeout) }
      rescue Rdkafka::RdkafkaError, Rdkafka::Producer::DeliveryHandle::WaitTimeoutError
        raise Errors::FlushFailureError.new(dispatched)
      end
    end
  end
end
