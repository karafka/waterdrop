# frozen_string_literal: true

module WaterDrop
  class Producer
    # Class used to construct the rdkafka producer client
    class Builder
      # @param producer [Producer] not yet configured producer for which we want to
      #   build the client
      # @param config [Object] dry-configurable based configuration object
      # @return [Rdkafka::Producer, Producer::DummyClient] raw rdkafka producer or a dummy producer
      #   when we don't want to dispatch any messages
      def call(producer, config)
        return DummyClient.new unless config.deliver

        Rdkafka::Config.logger = config.logger
        Rdkafka::Config.statistics_callback = build_statistics_callback(producer, config.monitor)

        client = Rdkafka::Config.new(config.kafka.to_h).producer
        client.delivery_callback = build_delivery_callback(producer, config.monitor)
        client
      end

      private

      # Creates a proc that we want to run upon each successful message delivery
      #
      # @param producer [Producer]
      # @param monitor [Object] monitor we want to use
      # @return [Proc] delivery callback
      def build_delivery_callback(producer, monitor)
        lambda do |delivery_report|
          monitor.instrument(
            'message.acknowledged',
            producer: producer,
            offset: delivery_report.offset,
            partition: delivery_report.partition
          )
        end
      end

      # Creates a proc that we want to run upon each statistics callback execution
      #
      # @param producer [Producer]
      # @param monitor [Object] monitor we want to use
      # @return [Proc] statistics callback
      # @note We decorate the statistics with our own decorator because some of the metrics from
      #   rdkafka are absolute. For example number of sent messages increases not in reference to
      #   previous statistics emit but from the beginning of the process. We decorate it with diff
      #   of all the numeric values against the data from the previous callback emit
      def build_statistics_callback(producer, monitor)
        statistics_decorator = StatisticsDecorator.new

        lambda do |statistics|
          monitor.instrument(
            'statistics.emitted',
            producer: producer,
            statistics: statistics_decorator.call(statistics)
          )
        end
      end
    end
  end
end
