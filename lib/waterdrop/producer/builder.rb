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

        # We use our own finalizers, this finalizer is not stable enough and there seems to be a
        # race condition in between finalizers
        # Removing this allows us to use our own finalizer
        ObjectSpace.undefine_finalizer(client)

        # This callback is not global and is per client, thus we do not have to wrap it with a
        # callbacks manager to make it work
        client.delivery_callback = Instrumentation::Callbacks::Delivery.new(
          producer.id,
          config.monitor
        )

        # Initialize the name so it is cached
        # This will prevent us from building the name on a producer client that is already closed
        # and does not exist
        client.name

        client
      end
    end
  end
end
