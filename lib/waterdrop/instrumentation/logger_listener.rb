# frozen_string_literal: true

module WaterDrop
  # WaterDrop instrumentation related module
  module Instrumentation
    # Default listener that hooks up to our instrumentation and uses its events for logging
    # It can be removed/replaced or anything without any harm to the Waterdrop flow
    # @note It is a module as we can use it then as a part of the Karafka framework listener
    #   as well as we can use it standalone
    class LoggerListener
      # @param logger [Object] logger we want to use
      # @param log_messages [Boolean] Should we report the messages content (payload and metadata)
      #   with each message operation.
      #
      #   This can be extensive, especially when producing a lot of messages. We provide this
      #   despite the fact that we only report payloads in debug, because Rails by default operates
      #   with debug level. This means, that when working with Rails in development, every single
      #   payload dispatched will go to logs. In majority of the cases this is extensive and simply
      #   floods the end user.
      def initialize(logger, log_messages: true)
        @logger = logger
        @log_messages = log_messages
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_message_produced_async(event)
        message = event[:message]

        info(event, "Message to '#{message[:topic]}' topic was delegated to a dispatch queue")

        return unless log_messages?

        debug(event, message)
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_message_produced_sync(event)
        message = event[:message]

        info(event, "Sync producing of a message to '#{message[:topic]}' topic")

        return unless log_messages?

        debug(event, message)
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_messages_produced_async(event)
        messages = event[:messages]
        topics_count = messages.map { |message| "'#{message[:topic]}'" }.uniq.count

        info(
          event,
          "#{messages.size} messages to #{topics_count} topics were delegated to a dispatch queue"
        )

        return unless log_messages?

        debug(event, messages)
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_messages_produced_sync(event)
        messages = event[:messages]
        topics_count = messages.map { |message| "'#{message[:topic]}'" }.uniq.count

        info(event, "Sync producing of #{messages.size} messages to #{topics_count} topics")

        return unless log_messages?

        debug(event, messages)
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_message_buffered(event)
        message = event[:message]

        info(event, "Buffering of a message to '#{message[:topic]}' topic")

        return unless log_messages?

        debug(event, [message])
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_messages_buffered(event)
        messages = event[:messages]

        info(event, "Buffering of #{messages.size} messages")

        return unless log_messages?

        debug(event, [messages, messages.size])
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_buffer_flushed_async(event)
        messages = event[:messages]

        info(event, "Async flushing of #{messages.size} messages from the buffer")

        return unless log_messages?

        debug(event, messages)
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_buffer_flushed_sync(event)
        messages = event[:messages]

        info(event, "Sync flushing of #{messages.size} messages from the buffer")

        return unless log_messages?

        debug(event, messages)
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_buffer_purged(event)
        info(event, 'Successfully purging buffer')
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_producer_closing(event)
        info(event, 'Closing producer')
      end

      # @param event [Dry::Events::Event] event that happened with the details
      # @note While this says "Closing producer", it produces a nice message with time taken:
      #   "Closing producer took 12 ms" indicating it happened in the past.
      def on_producer_closed(event)
        info(event, 'Closing producer')
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_producer_reloaded(event)
        info(event, 'Producer successfully reloaded')
      end

      # @param event [Dry::Events::Event] event that happened with the error details
      def on_error_occurred(event)
        error = event[:error]
        type = event[:type]

        error(event, "Error occurred: #{error} - #{type}")
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_transaction_started(event)
        info(event, 'Starting transaction')
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_transaction_aborted(event)
        info(event, 'Aborting transaction')
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_transaction_committed(event)
        info(event, 'Committing transaction')
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_transaction_marked_as_consumed(event)
        message = event[:message]
        topic = message.topic
        partition = message.partition
        offset = message.offset
        loc = "#{topic}/#{partition}"

        info(
          event,
          "Marking message with offset #{offset} for topic #{loc} as consumed in a transaction"
        )
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_transaction_finished(event)
        info(event, 'Processing transaction')
      end

      private

      # @return [Boolean] should we report the messages details in the debug mode.
      def log_messages?
        @log_messages
      end

      # @param event [Dry::Events::Event] event that happened with the details
      # @param log_message [String] message we want to publish
      def debug(event, log_message)
        @logger.debug("[#{event[:producer_id]}] #{log_message}")
      end

      # @param event [Dry::Events::Event] event that happened with the details
      # @param log_message [String] message we want to publish
      def info(event, log_message)
        if event.payload.key?(:time)
          @logger.info("[#{event[:producer_id]}] #{log_message} took #{event[:time].round(2)} ms")
        else
          @logger.info("[#{event[:producer_id]}] #{log_message}")
        end
      end

      # @param event [Dry::Events::Event] event that happened with the details
      # @param log_message [String] message we want to publish
      def error(event, log_message)
        @logger.error("[#{event[:producer_id]}] #{log_message}")
      end
    end
  end
end
