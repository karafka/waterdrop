# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    # Default listener that hooks up to our instrumentation and uses its events for logging
    # It can be removed/replaced or anything without any harm to the Waterdrop flow
    # @note It is a module as we can use it then as a part of the Karafka framework listener
    #   as well as we can use it standalone
    class StdoutListener
      def initialize(logger)
        @logger = logger
      end

      # Log levels that we use in this particular listener
      USED_LOG_LEVELS = %i[
        debug
        info
        error
      ].freeze

      def on_message_produced_async(event)
        message = event[:message]

        info "Async producing of a message to '#{message[:topic]}' topic"
        debug [message, event[:time]]
      end

      def on_message_produced_sync(event)
        message = event[:message]

        info "Sync producing of a message to '#{message[:topic]}' topic"
        debug [message, event[:time]]
      end

      def on_messages_produced_async(event)
        messages = event[:messages]
        topics_count = messages.map { |message| "'#{message[:topic]}'" }.uniq.count

        info "Async producing of #{messages.size} messages to #{topics_count} topics"
        debug [messages, event[:time]]
      end

      def on_messages_produced_sync(event)
        messages = event[:messages]
        topics_count = messages.map { |message| "'#{message[:topic]}'" }.uniq.count

        info "Sync producing of #{messages.size} messages to #{topics_count} topics"
        debug [messages, event[:time]]
      end

      def on_message_buffered(event)
        message = event[:message]
        producer = event[:producer]
        buffer_size = producer.messages.size

        info "Buffering of a message to '#{message[:topic]}' topic. Buffer size: #{buffer_size}"
        debug [message, event[:time]]
      end

      def on_messages_buffered(event)
        messages = event[:messages]
        producer = event[:producer]
        buffer_size = producer.messages.size

        info "Buffering of #{messages.size} messages. Buffer size: #{buffer_size}"
        debug [messages, event[:time]]
      end

      def on_buffer_flushed_async(event)
        messages = event[:messages]

        info "Async flushing of #{messages.size} messages from the buffer."
        debug [messages, event[:time]]
      end

      def on_buffer_flushed_sync(event)
        messages = event[:messages]

        info "Sync flushing of #{messages.size} messages from the buffer."
        debug [messages, event[:time]]
      end

      %i[
        buffer.flushed_async.error
        buffer.flushed_sync.error
      ]

      USED_LOG_LEVELS.each do |log_level|
        define_method log_level do |*args|
          @logger.send(log_level, *args)
        end
      end
    end
  end
end
