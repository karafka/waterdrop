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
          kafka_config = producer.config.kafka.to_h
          monitor = producer.config.monitor

          client = ::Rdkafka::Config.new(kafka_config).producer(native_kafka_auto_start: false)

          # Register statistics runner for this particular type of callbacks
          ::Karafka::Core::Instrumentation.statistics_callbacks.add(
            producer.id,
            Instrumentation::Callbacks::Statistics.new(producer.id, client.name, monitor)
          )

          # Register error tracking callback
          ::Karafka::Core::Instrumentation.error_callbacks.add(
            producer.id,
            Instrumentation::Callbacks::Error.new(producer.id, client.name, monitor)
          )

          # Register oauth bearer refresh for this particular type of callbacks
          ::Karafka::Core::Instrumentation.oauthbearer_token_refresh_callbacks.add(
            producer.id,
            Instrumentation::Callbacks::OauthbearerTokenRefresh.new(client, monitor)
          )

          # This callback is not global and is per client, thus we do not have to wrap it with a
          # callbacks manager to make it work
          client.delivery_callback = Instrumentation::Callbacks::Delivery.new(
            producer.id,
            producer.transactional?,
            monitor
          )

          oauth_listener = producer.config.oauth.token_provider_listener
          # We need to subscribe the oauth listener here because we want it to be ready before
          # any producer callbacks run. In theory because WaterDrop rdkafka producer is lazy loaded
          # we would have enough time to make user subscribe it himself, but then it would not
          # coop with auto-configuration coming from Karafka. The way it is done below, if it is
          # configured it will be subscribed and if not, user always can subscribe it himself as
          # long as it is done prior to first usage
          monitor.subscribe(oauth_listener) if oauth_listener

          client.start

          # Switch to the transactional mode if user provided the transactional id
          client.init_transactions if kafka_config.key?(:'transactional.id')

          client
        end
      end
    end
  end
end
