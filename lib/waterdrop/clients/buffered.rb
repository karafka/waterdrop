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

        @transaction_mutex = Mutex.new
        @transaction_active = false
        @transaction_messages = []
        @transaction_topics = Hash.new { |k, v| k[v] = [] }
      end

      # "Produces" message to Kafka: it acknowledges it locally, adds it to the internal buffer
      # @param message [Hash] `WaterDrop::Producer#produce_sync` message hash
      def produce(message)
        if @transaction_active
          @transaction_topics[message.fetch(:topic)] << message
          @transaction_messages << message
        else
          # We pre-validate the message payload, so topic is ensured to be present
          @topics[message.fetch(:topic)] << message
          @messages << message
        end

        SyncResponse.new
      end

      # Yields the code pretending it is in a transaction
      # Supports our aborting transaction flow
      # Moves messages the appropriate buffers only if transaction is successful
      def transaction
        return yield if @transaction_mutex.owned?

        @transaction_mutex.lock
        @transaction_active = true

        result = nil
        commit = false

        catch(:abort) do
          result = yield
          commit = true
        end

        commit || raise(WaterDrop::Errors::AbortTransaction)

        # Transfer transactional data on success
        @transaction_topics.each do |topic, messages|
          @topics[topic] += messages
        end

        @messages += @transaction_messages

        result
      rescue StandardError => e
        return if e.is_a?(WaterDrop::Errors::AbortTransaction)

        raise
      ensure
        if @transaction_mutex.owned?
          @transaction_topics.clear
          @transaction_messages.clear
          @transaction_active = false
          @transaction_mutex.unlock
        end
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
