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
        klass = config.client_class
        # This allows us to have backwards compatibility.
        # If it is the default client and delivery is set to false, we use dummy as we used to
        # before `client_class` was introduced
        klass = Clients::Dummy if klass == Clients::Rdkafka && !config.deliver

        klass.new(producer)
      end
    end
  end
end
