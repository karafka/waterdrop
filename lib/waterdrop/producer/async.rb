# frozen_string_literal: true

module WaterDrop
  class Producer
    # Component for asynchronous producer operations
    module Async
      # Produces a message to Kafka and does not wait for results
      #
      # @param message [Hash] hash that complies with the {Contracts::Message} contract
      #
      # @return [Rdkafka::Producer::DeliveryHandle] delivery handle that might return the report
      #
      # @raise [Rdkafka::RdkafkaError] When adding the message to rdkafka's queue failed
      # @raise [Errors::MessageInvalidError] When provided message details are invalid and the
      #   message could not be sent to Kafka
      def produce_async(message)
        message = middleware.run(message)
        validate_message!(message)

        @monitor.instrument(
          'message.produced_async',
          producer_id: id,
          message: message
        ) { produce(message) }
      rescue *SUPPORTED_FLOW_ERRORS => e
        re_raised = Errors::ProduceError.new(e.inspect)

        @monitor.instrument(
          'error.occurred',
          producer_id: id,
          message: message,
          error: re_raised,
          type: 'message.produce_async'
        )

        raise re_raised
      end

      # Produces many messages to Kafka and does not wait for them to be delivered
      #
      # @param messages [Array<Hash>] array with messages that comply with the
      #   {Contracts::Message} contract
      #
      # @return [Array<Rdkafka::Producer::DeliveryHandle>] deliveries handles
      #
      # @raise [Rdkafka::RdkafkaError] When adding the messages to rdkafka's queue failed
      # @raise [Errors::MessageInvalidError] When any of the provided messages details are invalid
      #   and the message could not be sent to Kafka
      def produce_many_async(messages)
        dispatched = []
        messages = middleware.run_many(messages)
        messages.each { |message| validate_message!(message) }

        @monitor.instrument(
          'messages.produced_async',
          producer_id: id,
          messages: messages
        ) do
          messages.each do |message|
            dispatched << produce(message)
          end

          dispatched
        end
      rescue *SUPPORTED_FLOW_ERRORS => e
        re_raised = Errors::ProduceManyError.new(dispatched, e.inspect)

        @monitor.instrument(
          'error.occurred',
          producer_id: id,
          messages: messages,
          dispatched: dispatched,
          error: re_raised,
          type: 'messages.produce_many_async'
        )

        raise re_raised
      end
    end
  end
end
