# frozen_string_literal: true

module WaterDrop
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
        @monitor.instrument(
          'message.acknowledged',
          producer_id: @producer_od,
          offset: delivery_report.offset,
          partition: delivery_report.partition
        )
      end
    end
  end
end
