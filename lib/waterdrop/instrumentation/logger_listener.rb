# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    # Default listener that hooks up to our instrumentation and uses its events for logging
    # It can be removed/replaced or anything without any harm to the Waterdrop flow
    # @note It is a module as we can use it then as a part of the Karafka framework listener
    #   as well as we can use it standalone
    class LoggerListener
      # @param logger [Object] logger we want to use
      def initialize(logger)
        @logger = logger
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_message_produced_async(event)
        message = event[:message]

        info(event, "Async producing of a message to '#{message[:topic]}' topic")
        debug(event, message)
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_message_produced_sync(event)
        message = event[:message]

        info(event, "Sync producing of a message to '#{message[:topic]}' topic")
        debug(event, message)
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_messages_produced_async(event)
        messages = event[:messages]
        topics_count = messages.map { |message| "'#{message[:topic]}'" }.uniq.count

        info(event, "Async producing of #{messages.size} messages to #{topics_count} topics")
        debug(event, messages)
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_messages_produced_sync(event)
        messages = event[:messages]
        topics_count = messages.map { |message| "'#{message[:topic]}'" }.uniq.count

        info(event, "Sync producing of #{messages.size} messages to #{topics_count} topics")
        debug(event, messages)
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_message_buffered(event)
        message = event[:message]

        info(event, "Buffering of a message to '#{message[:topic]}' topic")
        debug(event, [message])
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_messages_buffered(event)
        messages = event[:messages]

        info(event, "Buffering of #{messages.size} messages")
        debug(event, [messages, messages.size])
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_buffer_flushed_async(event)
        messages = event[:messages]

        info(event, "Async flushing of #{messages.size} messages from the buffer")
        debug(event, messages)
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_buffer_flushed_sync(event)
        messages = event[:messages]

        info(event, "Sync flushing of #{messages.size} messages from the buffer")
        debug(event, messages)
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_producer_closed(event)
        info event, 'Closing producer'
        debug event, ''
      end

      # @param event [Dry::Events::Event] event that happened with the error details
      def on_error_occurred(event)
        error = event[:error]
        type = event[:type]

        error(event, "Error occurred: #{error} - #{type}")
        debug(event, '')
      end

      private

      # @param event [Dry::Events::Event] event that happened with the details
      # @param log_message [String] message we want to publish
      def debug(event, log_message)
        @logger.debug("[#{event[:producer_id]}] #{log_message}")
      end

      # @param event [Dry::Events::Event] event that happened with the details
      # @param log_message [String] message we want to publish
      def info(event, log_message)
        @logger.info("[#{event[:producer_id]}] #{log_message} took #{event[:time]} ms")
      end

      # @param event [Dry::Events::Event] event that happened with the details
      # @param log_message [String] message we want to publish
      def error(event, log_message)
        @logger.error("[#{event[:producer_id]}] #{log_message}")
      end
    end
  end
end
