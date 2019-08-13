# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    # Default listener that hooks up to our instrumentation and uses its events for logging
    # It can be removed/replaced or anything without any harm to the Waterdrop flow
    # @note It is a module as we can use it then as a part of the Karafka framework listener
    #   as well as we can use it standalone
    class StdoutListener
      # @param logger [Object] stdout logger we want to use
      def initialize(logger)
        @logger = logger
      end

      # Log levels that we use in this particular listener
      %i[
        debug
        info
        error
      ].each do |log_level|
        # Creates a custom logging methods that add some extra info when printing to the stdout
        define_method log_level do |event, log_message|
          @logger.public_send(
            log_level,
            "[#{event[:producer].id}] #{log_message} took #{event[:time]} ms"
          )
        end
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_message_produced_async(event)
        message = event[:message]

        info event, "Async producing of a message to '#{message[:topic]}' topic"
        debug event, message
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_message_produced_sync(event)
        message = event[:message]

        info event, "Sync producing of a message to '#{message[:topic]}' topic"
        debug event, message
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_messages_produced_async(event)
        messages = event[:messages]
        topics_count = messages.map { |message| "'#{message[:topic]}'" }.uniq.count

        info event, "Async producing of #{messages.size} messages to #{topics_count} topics"
        debug event, messages
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_messages_produced_sync(event)
        messages = event[:messages]
        topics_count = messages.map { |message| "'#{message[:topic]}'" }.uniq.count

        info event, "Sync producing of #{messages.size} messages to #{topics_count} topics"
        debug event, messages
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_message_buffered(event)
        message = event[:message]

        info event, "Buffering of a message to '#{message[:topic]}' topic."
        debug event, [message, event[:producer].messages.size]
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_messages_buffered(event)
        messages = event[:messages]

        info event, "Buffering of #{messages.size} messages."
        debug event, [message, event[:producer].messages.size]
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_buffer_flushed_async(event)
        messages = event[:messages]

        info event, "Async flushing of #{messages.size} messages from the buffer."
        debug event, messages
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_buffer_flushed_sync(event)
        messages = event[:messages]

        info event, "Sync flushing of #{messages.size} messages from the buffer."
        debug event, messages
      end

      # @param event [Dry::Events::Event] event that happened with the details
      def on_producer_closed(event)
        info event, 'Closing producer'
        debug event, event[:producer].messages.size
      end
    end
  end
end
