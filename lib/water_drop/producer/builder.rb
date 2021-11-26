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

        client = Rdkafka::Config.new(config.kafka.to_h).producer
        client.delivery_callback = Callbacks::Delivery.new(producer.id, config.monitor)
        client
      end
    end
  end
end
