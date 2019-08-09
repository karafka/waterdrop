# frozen_string_literal: true

module WaterDrop
  class Producer
    module Buffer
      # Adds given message into the internal producer buffer without flushing it to Kafka
      #
      # @param message [Hash] hash that complies with the `WaterDrop::Contracts::Message` contract
      # @raise [Errors::MessageInvalidError] When provided message details are invalid and the
      #   message could not be sent to Kafka
      def buffer(message)
        ensure_active!
        validate_message!(message)

        @monitor.instrument(
          'message.buffered',
          producer: self,
          message: message
        ) { @buffer << message }
      end

      # Adds given messages into the internal producer buffer without flushing them to Kafka
      #
      # @param messages [Array<Hash>] array with messages that comply with the
      #   `Contracts::Message` contract
      # @raise [Errors::MessageInvalidError] When any of the provided messages details are invalid
      #   and the message could not be sent to Kafka
      def buffer_many(messages)
        ensure_active!
        messages.each { |message| validate_message!(message) }

        @monitor.instrument(
          'messages.buffered',
          producer: self,
          messages: messages
        ) do
          messages.each { |message| @buffer << message }
          messages
        end
      end

      # Flushes the internal buffer to Kafka in an async way
      def flush_async
        ensure_active!

        @monitor.instrument(
          'buffer.flushed_async',
          producer: self,
          buffer: @buffer
        ) { flush(false) }
      end

      # Flushes the internal buffer to Kafka in a sync way
      def flush_sync
        ensure_active!

        @monitor.instrument(
          'buffer.flushed_sync',
          producer: self,
          buffer: @buffer
        ) { flush(true) }
      end

      private

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
