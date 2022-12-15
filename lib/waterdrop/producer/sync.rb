# frozen_string_literal: true

module WaterDrop
  class Producer
    # Component for synchronous producer operations
    module Sync
      # Produces a message to Kafka and waits for it to be delivered
      #
      # @param message [Hash] hash that complies with the {Contracts::Message} contract
      #
      # @return [Rdkafka::Producer::DeliveryReport] delivery report
      #
      # @raise [Rdkafka::RdkafkaError] When adding the message to rdkafka's queue failed
      # @raise [Rdkafka::Producer::WaitTimeoutError] When the timeout has been reached and the
      #   handle is still pending
      # @raise [Errors::MessageInvalidError] When provided message details are invalid and the
      #   message could not be sent to Kafka
      def produce_sync(message)
        ensure_active!

        message = middleware.run(message)
        validate_message!(message)

        @monitor.instrument(
          'message.produced_sync',
          producer_id: id,
          message: message
        ) do
          client
            .produce(**message)
            .wait(
              max_wait_timeout: @config.max_wait_timeout,
              wait_timeout: @config.wait_timeout
            )
        end
      end

      # Produces many messages to Kafka and waits for them to be delivered
      #
      # @param messages [Array<Hash>] array with messages that comply with the
      #   {Contracts::Message} contract
      #
      # @return [Array<Rdkafka::Producer::DeliveryReport>] delivery reports
      #
      # @raise [Rdkafka::RdkafkaError] When adding the messages to rdkafka's queue failed
      # @raise [Rdkafka::Producer::WaitTimeoutError] When the timeout has been reached and the
      #   some handles are still pending
      # @raise [Errors::MessageInvalidError] When any of the provided messages details are invalid
      #   and the message could not be sent to Kafka
      def produce_many_sync(messages)
        ensure_active!

        messages = middleware.run_many(messages)
        messages.each { |message| validate_message!(message) }

        @monitor.instrument('messages.produced_sync', producer_id: id, messages: messages) do
          messages
            .map { |message| client.produce(**message) }
            .map! do |handler|
              handler.wait(
                max_wait_timeout: @config.max_wait_timeout,
                wait_timeout: @config.wait_timeout
              )
            end
        end
      end
    end
  end
end
