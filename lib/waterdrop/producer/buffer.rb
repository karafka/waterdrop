# frozen_string_literal: true

module WaterDrop
  class Producer
    # Component for buffered operations
    module Buffer
      # Adds given message into the internal producer buffer without flushing it to Kafka
      #
      # @param message [Hash] hash that complies with the {Contracts::Message} contract
      # @raise [Errors::MessageInvalidError] When provided message details are invalid and the
      #   message could not be sent to Kafka
      def buffer(message)
        ensure_active!

        @monitor.instrument(
          'message.buffered',
          producer_id: id,
          message: message,
          buffer: @messages
        ) { @messages << message }
      end

      # Adds given messages into the internal producer buffer without flushing them to Kafka
      #
      # @param messages [Array<Hash>] array with messages that comply with the
      #   {Contracts::Message} contract
      # @raise [Errors::MessageInvalidError] When any of the provided messages details are invalid
      #   and the message could not be sent to Kafka
      def buffer_many(messages)
        ensure_active!

        @monitor.instrument(
          'messages.buffered',
          producer_id: id,
          messages: messages,
          buffer: @messages
        ) do
          messages.each { |message| @messages << message }
          messages
        end
      end

      # Flushes the internal buffer to Kafka in an async way
      # @return [Array<Rdkafka::Producer::DeliveryHandle>] delivery handles for messages that were
      #   flushed
      def flush_async
        @monitor.instrument(
          'buffer.flushed_async',
          producer_id: id,
          messages: @messages
        ) { flush(false) }
      end

      # Flushes the internal buffer to Kafka in a sync way
      # @return [Array<Rdkafka::Producer::DeliveryReport>] delivery reports for messages that were
      #   flushed
      def flush_sync
        @monitor.instrument(
          'buffer.flushed_sync',
          producer_id: id,
          messages: @messages
        ) { flush(true) }
      end

      private

      # Method for triggering the buffer
      # @param sync [Boolean] should it flush in a sync way
      # @return [Array<Rdkafka::Producer::DeliveryHandle, Rdkafka::Producer::DeliveryReport>]
      #   delivery handles for async or delivery reports for sync
      # @raise [Errors::ProduceManyError] when there was a failure in flushing
      # @note We use this method underneath to provide a different instrumentation for sync and
      #   async flushing within the public API
      def flush(sync)
        data_for_dispatch = nil

        @buffer_mutex.synchronize do
          data_for_dispatch = @messages
          @messages = []
        end

        # Do nothing if nothing to flush
        return data_for_dispatch if data_for_dispatch.empty?

        sync ? produce_many_sync(data_for_dispatch) : produce_many_async(data_for_dispatch)
      end
    end
  end
end
