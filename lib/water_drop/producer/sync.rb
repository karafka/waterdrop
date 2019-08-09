# frozen_string_literal: true

module WaterDrop
  class Producer
    # Component for synchronous producer operations
    module Sync
      # Produces a message to Kafka and waits for it to be delivered
      #
      # @param message [Hash] hash that complies with the `WaterDrop::Contracts::Message` contract
      #
      # @return [Rdkafka::Producer::DeliveryReport] delivery report
      #
      # @raise [Rdkafka::RdkafkaError] When adding the message to rdkafka's queue failed
      # @raise [Rdkafka::Producer::WaitTimeoutError] When the timeout has been reached and the
      #   handle is still pending
      def produce_sync(message)
        ensure_active!
        @validator.call(message)

        @monitor.instrument(
          'message.produced_sync',
          message: message
        ) do
          @client
            .produce(message)
            .wait(@config.wait_timeout)
        end
      end

      # Produces many messages to Kafka and waits for them to be delivered
      #
      # @param messages [Array<Hash>] array with messages that comply with the
      #   `WaterDrop::Contracts::Message` contract
      #
      # @return [Array<Rdkafka::Producer::DeliveryReport>] delivery reports
      #
      # @raise [Rdkafka::RdkafkaError] When adding the messages to rdkafka's queue failed
      # @raise [Rdkafka::Producer::WaitTimeoutError] When the timeout has been reached and the
      #   some handles are still pending
      def produce_many_sync(messages)
        ensure_active!
        messages.each(&@validator)

        @monitor.instrument(
          'messages.produced_sync',
          messages: messages
        ) do
          messages
            .map { |message| @client.produce(message) }
            .map { |handler| handler.wait(@config.wait_timeout) }
        end
      end
    end
  end
end
