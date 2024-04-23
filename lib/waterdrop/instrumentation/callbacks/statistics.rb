# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    # Namespace for handlers of callbacks emitted by the kafka client lib
    module Callbacks
      # Statistics callback handler
      # @note We decorate the statistics with our own decorator because some of the metrics from
      #   rdkafka are absolute. For example number of sent messages increases not in reference to
      #   previous statistics emit but from the beginning of the process. We decorate it with diff
      #   of all the numeric values against the data from the previous callback emit
      class Statistics
        # @param producer_id [String] id of the current producer
        # @param client_name [String] rdkafka client name
        # @param monitor [WaterDrop::Instrumentation::Monitor] monitor we are using
        def initialize(producer_id, client_name, monitor)
          @producer_id = producer_id
          @client_name = client_name
          @monitor = monitor
          @statistics_decorator = ::Karafka::Core::Monitoring::StatisticsDecorator.new
        end

        # Emits decorated statistics to the monitor
        # @param statistics [Hash] rdkafka statistics
        def call(statistics)
          # Emit only statistics related to our client
          # rdkafka does not have per-instance statistics hook, thus we need to make sure that we
          # emit only stats that are related to current producer. Otherwise we would emit all of
          # all the time.
          return unless @client_name == statistics['name']

          @monitor.instrument(
            'statistics.emitted',
            producer_id: @producer_id,
            statistics: @statistics_decorator.call(statistics)
          )
        # This runs from the rdkafka thread, thus we want to safe-guard it and prevent absolute
        # crashes even if the instrumentation code fails. If it would bubble-up, it could crash
        # the rdkafka background thread
        rescue StandardError => e
          @monitor.instrument(
            'error.occurred',
            caller: self,
            error: e,
            producer_id: @producer_id,
            type: 'callbacks.statistics.error'
          )
        end
      end
    end
  end
end
