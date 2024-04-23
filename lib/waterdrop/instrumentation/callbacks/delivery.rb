# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    module Callbacks
      # Creates a callable that we want to run upon each message delivery or failure
      #
      # @note We don't have to provide client_name here as this callback is per client instance
      #
      # @note We do not consider `message.purge` as an error for transactional producers, because
      #   this is a standard behaviour for not yet dispatched messages on aborted transactions.
      #   We do however still want to instrument it for traceability.
      class Delivery
        # Error emitted when a message was not yet dispatched and was purged from the queue
        RD_KAFKA_RESP_PURGE_QUEUE = -152

        # Error emitted when a message was purged while it was dispatched
        RD_KAFKA_RESP_PURGE_INFLIGHT = -151

        # Errors related to queue purging that is expected in transactions
        PURGE_ERRORS = [RD_KAFKA_RESP_PURGE_INFLIGHT, RD_KAFKA_RESP_PURGE_QUEUE].freeze

        private_constant :RD_KAFKA_RESP_PURGE_QUEUE, :RD_KAFKA_RESP_PURGE_INFLIGHT, :PURGE_ERRORS

        # @param producer_id [String] id of the current producer
        # @param transactional [Boolean] is this handle for a transactional or regular producer
        # @param monitor [WaterDrop::Instrumentation::Monitor] monitor we are using
        def initialize(producer_id, transactional, monitor)
          @producer_id = producer_id
          @transactional = transactional
          @monitor = monitor
        end

        # Emits delivery details to the monitor
        # @param delivery_report [Rdkafka::Producer::DeliveryReport] delivery report
        def call(delivery_report)
          error_code = delivery_report.error.to_i

          if error_code.zero?
            instrument_acknowledged(delivery_report)

          elsif @transactional && PURGE_ERRORS.include?(error_code)
            instrument_purged(delivery_report)
          else
            instrument_error(delivery_report)
          end
        # This runs from the rdkafka thread, thus we want to safe-guard it and prevent absolute
        # crashes even if the instrumentation code fails. If it would bubble-up, it could crash
        # the rdkafka background thread
        rescue StandardError => e
          @monitor.instrument(
            'error.occurred',
            caller: self,
            error: e,
            producer_id: @producer_id,
            type: 'callbacks.delivery.error'
          )
        end

        private

        # @param delivery_report [Rdkafka::Producer::DeliveryReport] delivery report
        def instrument_acknowledged(delivery_report)
          @monitor.instrument(
            'message.acknowledged',
            caller: self,
            producer_id: @producer_id,
            offset: delivery_report.offset,
            partition: delivery_report.partition,
            topic: delivery_report.topic_name,
            delivery_report: delivery_report,
            label: delivery_report.label
          )
        end

        # @param delivery_report [Rdkafka::Producer::DeliveryReport] delivery report
        def instrument_purged(delivery_report)
          @monitor.instrument(
            'message.purged',
            caller: self,
            error: build_error(delivery_report),
            producer_id: @producer_id,
            offset: delivery_report.offset,
            partition: delivery_report.partition,
            topic: delivery_report.topic_name,
            delivery_report: delivery_report,
            label: delivery_report.label,
            type: 'librdkafka.dispatch_error'
          )
        end

        # @param delivery_report [Rdkafka::Producer::DeliveryReport] delivery report
        def instrument_error(delivery_report)
          @monitor.instrument(
            'error.occurred',
            caller: self,
            error: build_error(delivery_report),
            producer_id: @producer_id,
            offset: delivery_report.offset,
            partition: delivery_report.partition,
            topic: delivery_report.topic_name,
            delivery_report: delivery_report,
            label: delivery_report.label,
            type: 'librdkafka.dispatch_error'
          )
        end

        # Builds appropriate rdkafka error
        # @param delivery_report [Rdkafka::Producer::DeliveryReport] delivery report
        # @return [::Rdkafka::RdkafkaError]
        def build_error(delivery_report)
          ::Rdkafka::RdkafkaError.new(delivery_report.error)
        end
      end
    end
  end
end
