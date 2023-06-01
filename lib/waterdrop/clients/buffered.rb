# frozen_string_literal: true

module WaterDrop
  module Clients
    # Client used to buffer messages that we send out in specs and other places.
    class Buffered < Clients::Dummy
      attr_accessor :messages

      # Sync fake response for the message delivery to Kafka, since we do not dispatch anything
      class SyncResponse
        # @param _args Handler wait arguments (irrelevant as waiting is fake here)
        def wait(*_args)
          false
        end
      end

      # @param args [Object] anything accepted by `Clients::Dummy`
      def initialize(*args)
        super
        @messages = []
        @topics = Hash.new { |k, v| k[v] = [] }
      end

      # "Produces" message to Kafka: it acknowledges it locally, adds it to the internal buffer
      # @param message [Hash] `WaterDrop::Producer#produce_sync` message hash
      def produce(message)
        topic = message.fetch(:topic) { raise ArgumentError, ':topic is missing' }
        @topics[topic] << message
        @messages << message
        SyncResponse.new
      end

      # Returns messages produced to a given topic
      # @param topic [String]
      def messages_for(topic)
        @topics[topic]
      end

      # Clears internal buffer
      # Used in between specs so messages do not leak out
      def reset
        @messages.clear
        @topics.each_value(&:clear)
      end
    end
  end
end
