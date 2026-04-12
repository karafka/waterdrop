# frozen_string_literal: true

module WaterDrop
  class Producer
    # Component for tombstone producer operations
    #
    # Tombstone records are Kafka messages with a nil payload, used to signal deletion of a key
    # in compacted topics. This module provides a dedicated API so users don't have to manually
    # construct `produce_*(topic:, key:, payload: nil, ...)` calls.
    module Tombstone
      # Produces a tombstone message to Kafka and waits for it to be delivered
      #
      # @param message [Hash] hash with at least `:topic`, `:key`, and `:partition` keys.
      #   `:payload` is not accepted — it will be silently removed if present.
      #
      # @return [Rdkafka::Producer::DeliveryReport] delivery report
      #
      # @raise [Errors::MessageInvalidError] When `:key` or `:partition` is missing
      def tombstone_sync(message)
        produce_sync(prepare_tombstone(message))
      end

      # Produces a tombstone message to Kafka and does not wait for results
      #
      # @param message [Hash] hash with at least `:topic`, `:key`, and `:partition` keys.
      #   `:payload` is not accepted — it will be silently removed if present.
      #
      # @return [Rdkafka::Producer::DeliveryHandle] delivery handle
      #
      # @raise [Errors::MessageInvalidError] When `:key` or `:partition` is missing
      def tombstone_async(message)
        produce_async(prepare_tombstone(message))
      end

      # Produces many tombstone messages to Kafka and waits for them to be delivered
      #
      # @param messages [Array<Hash>] array of hashes, each with `:topic`, `:key`, and
      #   `:partition` keys
      #
      # @return [Array<Rdkafka::Producer::DeliveryHandle>] delivery handles
      #
      # @raise [Errors::MessageInvalidError] When any message is missing `:key` or `:partition`
      def tombstone_many_sync(messages)
        produce_many_sync(messages.map { |message| prepare_tombstone(message) })
      end

      # Produces many tombstone messages to Kafka and does not wait for them to be delivered
      #
      # @param messages [Array<Hash>] array of hashes, each with `:topic`, `:key`, and
      #   `:partition` keys
      #
      # @return [Array<Rdkafka::Producer::DeliveryHandle>] delivery handles
      #
      # @raise [Errors::MessageInvalidError] When any message is missing `:key` or `:partition`
      def tombstone_many_async(messages)
        produce_many_async(messages.map { |message| prepare_tombstone(message) })
      end

      private

      # Validates and prepares a tombstone message by ensuring required keys are present
      # and setting payload to nil
      #
      # @param message [Hash] the original message hash
      # @return [Hash] a new message hash with payload set to nil
      # @raise [Errors::MessageInvalidError] when key or partition is missing
      def prepare_tombstone(message)
        message = message.dup
        message.delete(:payload)
        message[:payload] = nil

        Contracts::Tombstone.new.validate!(message, Errors::MessageInvalidError)

        message
      end
    end
  end
end
