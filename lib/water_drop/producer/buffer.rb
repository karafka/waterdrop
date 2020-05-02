# frozen_string_literal: true

module WaterDrop
  class Producer
    # Component for buffered operations
    module Buffer
      # Exceptions we catch when dispatching messages from a buffer
      RESCUED_ERRORS = [
        Rdkafka::RdkafkaError,
        Rdkafka::Producer::DeliveryHandle::WaitTimeoutError
      ].freeze

      private_constant :RESCUED_ERRORS

      # Adds given message into the internal producer buffer without flushing it to Kafka
      #
      # @param message [Hash] hash that complies with the {Contracts::Message} contract
      # @raise [Errors::MessageInvalidError] When provided message details are invalid and the
      #   message could not be sent to Kafka
      def buffer(message)
        ensure_usable!
        validate_message!(message)

        @monitor.instrument(
          'message.buffered',
          producer: self,
          message: message
        ) { @messages << message }
      end

      # Adds given messages into the internal producer buffer without flushing them to Kafka
      #
      # @param messages [Array<Hash>] array with messages that comply with the
      #   {Contracts::Message} contract
      # @raise [Errors::MessageInvalidError] When any of the provided messages details are invalid
      #   and the message could not be sent to Kafka
      def buffer_many(messages)
        ensure_usable!
        messages.each { |message| validate_message!(message) }

        @monitor.instrument(
          'messages.buffered',
          producer: self,
          messages: messages
        ) do
          messages.each { |message| @messages << message }
          messages
        end
      end

      # Flushes the internal buffer to Kafka in an async way
      # @return [Array<Rdkafka::Producer::DeliveryHandle>] delivery handles for messages that were
      #   flushed
      def flush_async
        ensure_usable!

        @monitor.instrument(
          'buffer.flushed_async',
          producer: self,
          messages: @messages
        ) { flush(false) }
      end

      # Flushes the internal buffer to Kafka in a sync way
      # @return [Array<Rdkafka::Producer::DeliveryReport>] delivery reports for messages that were
      #   flushed
      def flush_sync
        ensure_usable!

        @monitor.instrument(
          'buffer.flushed_sync',
          producer: self,
          messages: @messages
        ) { flush(true) }
      end

      private

      # Method for triggering the buffer
      # @param sync [Boolean] should it flush in a sync way
      # @return [Array<Rdkafka::Producer::DeliveryHandle, Rdkafka::Producer::DeliveryReport>]
      #   delivery handles for async or delivery reports for sync
      # @raise [Errors::FlushFailureError] when there was a failure in flushing
      # @note We use this method underneath to provide a different instrumentation for sync and
      #   async flushing within the public API
      def flush(sync)
        data_for_dispatch = nil
        dispatched = []

        @mutex.synchronize do
          data_for_dispatch = @messages
          @messages = Concurrent::Array.new
        end

        dispatched = data_for_dispatch.map { |message| client.produce(**message) }

        return dispatched unless sync

        dispatched.map do |handler|
          handler.wait(
            max_wait_timeout: @config.max_wait_timeout,
            wait_timeout: @config.wait_timeout
          )
        end
      rescue *RESCUED_ERRORS => e
        key = sync ? 'buffer.flushed_sync.error' : 'buffer.flush_async.error'
        @monitor.instrument(key, producer: self, error: e, dispatched: dispatched)

        raise Errors::FlushFailureError.new(dispatched)
      end
    end
  end
end
