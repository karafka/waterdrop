# frozen_string_literal: true

module WaterDrop
  # Namespace for all the clients that WaterDrop may use under the hood
  module Clients
    # Default Rdkafka client.
    # Since we use the ::Rdkafka::Producer under the hood, this is just a module that aligns with
    # client building API for the convenience.
    module Rdkafka
      class << self
        # @param producer [WaterDrop::Producer] producer instance with its config, etc
        # @note We overwrite this that way, because we do not care
        def new(producer)
          config = producer.config.kafka.to_h

          client = ::Rdkafka::Config.new(config).producer

          # This callback is not global and is per client, thus we do not have to wrap it with a
          # callbacks manager to make it work
          client.delivery_callback = Instrumentation::Callbacks::Delivery.new(
            producer.id,
            producer.transactional?,
            producer.config.monitor
          )

          # Switch to the transactional mode if user provided the transactional id
          client.init_transactions if config.key?(:'transactional.id')

          client
        end
      end
    end
  end
end
