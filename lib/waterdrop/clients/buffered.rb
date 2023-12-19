# frozen_string_literal: true

module WaterDrop
  module Clients
    # Client used to buffer messages that we send out in specs and other places.
    class Buffered < Clients::Dummy
      attr_accessor :messages

      # @param args [Object] anything accepted by `Clients::Dummy`
      def initialize(*args)
        super
        @messages = []
        @topics = Hash.new { |k, v| k[v] = [] }

        @transaction_active = false
        @transaction_messages = []
        @transaction_topics = Hash.new { |k, v| k[v] = [] }
        @transaction_level = 0
      end

      # "Produces" message to Kafka: it acknowledges it locally, adds it to the internal buffer
      # @param message [Hash] `WaterDrop::Producer#produce_sync` message hash
      # @return [Dummy::Handle] fake delivery handle that can be materialized into a report
      def produce(message)
        if @transaction_active
          @transaction_topics[message.fetch(:topic)] << message
          @transaction_messages << message
        else
          # We pre-validate the message payload, so topic is ensured to be present
          @topics[message.fetch(:topic)] << message
          @messages << message
        end

        super(**message.to_h)
      end

      # Starts the transaction on a given level
      def begin_transaction
        @transaction_level += 1
        @transaction_active = true
      end

      # Finishes given level of transaction
      def commit_transaction
        @transaction_level -= 1

        # Transfer transactional data on success
        @transaction_topics.each do |topic, messages|
          @topics[topic] += messages
        end

        @messages += @transaction_messages

        @transaction_topics.clear
        @transaction_messages.clear
        @transaction_active = false
      end

      # Fakes storing the offset in a transactional fashion
      #
      # @param _consumer [#consumer_group_metadata_pointer] any consumer from which we can obtain
      #   the librdkafka consumer group metadata pointer
      # @param _tpl [Rdkafka::Consumer::TopicPartitionList] consumer tpl for offset storage
      # @param _timeout [Integer] ms timeout
      def send_offsets_to_transaction(_consumer, _tpl, _timeout)
        nil
      end

      # Aborts the transaction
      def abort_transaction
        @transaction_level -= 1
        @transaction_topics.clear
        @transaction_messages.clear
        @transaction_active = false
      end

      # Returns messages produced to a given topic
      # @param topic [String]
      def messages_for(topic)
        @topics[topic]
      end

      # Clears internal buffer
      # Used in between specs so messages do not leak out
      def reset
        @transaction_level = 0
        @transaction_active = false
        @transaction_topics.clear
        @transaction_messages.clear
        @messages.clear
        @topics.each_value(&:clear)
      end
    end
  end
end
