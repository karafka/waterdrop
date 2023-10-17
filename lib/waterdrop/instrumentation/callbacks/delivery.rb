# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    module Callbacks
      # Creates a callable that we want to run upon each message delivery or failure
      #
      # @note We don't have to provide client_name here as this callback is per client instance
      class Delivery
        # @param producer_id [String] id of the current producer
        # @param monitor [WaterDrop::Instrumentation::Monitor] monitor we are using
        def initialize(producer_id, monitor)
          @producer_id = producer_id
          @monitor = monitor
        end

        # Emits delivery details to the monitor
        # @param delivery_report [Rdkafka::Producer::DeliveryReport] delivery report
        def call(delivery_report)
          if delivery_report.error.to_i.zero?
            instrument_acknowledged(delivery_report)
          else
            instrument_error(delivery_report)
          end
        end

        private

        # @param delivery_report [Rdkafka::Producer::DeliveryReport] delivery report
        def instrument_error(delivery_report)
          @monitor.instrument(
            'error.occurred',
            caller: self,
            error: ::Rdkafka::RdkafkaError.new(delivery_report.error),
            producer_id: @producer_id,
            offset: delivery_report.offset,
            partition: delivery_report.partition,
            topic: delivery_report.topic_name,
            delivery_report: delivery_report,
            type: 'librdkafka.dispatch_error'
          )
        end

        # @param delivery_report [Rdkafka::Producer::DeliveryReport] delivery report
        def instrument_acknowledged(delivery_report)
          @monitor.instrument(
            'message.acknowledged',
            producer_id: @producer_id,
            offset: delivery_report.offset,
            partition: delivery_report.partition,
            topic: delivery_report.topic_name,
            delivery_report: delivery_report
          )
        end
      end
    end
  end
end
