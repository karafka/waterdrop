# frozen_string_literal: true

module WaterDrop
  class Producer
    # Class used to construct the rdkafka producer client
    class Builder
      # @param config [Object] dry-configurable based configuration object
      # @return [Rdkafka::Producer, Producer::DummyClient] raw rdkafka producer or a dummy producer
      #   when we don't want to dispatch any messages
      def call(producer, config)
        return DummyClient.new unless config.deliver

        Rdkafka::Config.logger = config.logger

        client = Rdkafka::Config.new(config.kafka.to_h).producer
        client.delivery_callback = build_delivery_callback(producer, config.monitor)
        client
      end

      private

      def build_delivery_callback(producer, monitor)
        ->(delivery_report) do
          monitor.instrument(
            'message.acknowledged',
            producer: producer,
            offset: delivery_report.offset,
            partition: delivery_report.partition
          )
        end
      end
    end
  end
end
