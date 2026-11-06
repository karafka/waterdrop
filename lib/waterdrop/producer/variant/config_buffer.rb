# frozen_string_literal: true

module WaterDrop
  class Producer
    class Variant < SimpleDelegator
      # Buffers belong to the producer, so another variant or the producer itself may flush them.
      # A message buffered through a variant keeps its topic config, so it is sent with it whoever
      # flushes. This lives here and not in Buffer#buffer because resolving `current_variant` there
      # would cost every default-producer #buffer call (~13%).
      class ConfigBuffer
        # @param producer [WaterDrop::Producer] producer that owns the buffer
        # @param topic_config [Hash] topic config of the variant buffering the messages
        def initialize(producer, topic_config)
          @producer = producer
          @topic_config = topic_config
        end

        # @param message [Hash] message to buffer
        def buffer(message)
          @producer.buffer(with_topic_config(message))
        end

        # @param messages [Array<Hash>] messages to buffer
        def buffer_many(messages)
          @producer.buffer_many(messages.map { |message| with_topic_config(message) })
        end

        private

        # @param message [Hash] user message, never changed in place
        # @return [Hash] copy of the message with the variant topic config
        def with_topic_config(message)
          message = message.dup
          message[:topic_config] = @topic_config
          message
        end
      end
    end
  end
end
