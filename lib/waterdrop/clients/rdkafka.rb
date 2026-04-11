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
          monitor = producer.config.monitor
          kafka_config, statistics_enabled = prepare_statistics(
            producer.config.kafka.to_h,
            monitor
          )

          client = build_rdkafka_client(producer, kafka_config)

          register_instrumentation_callbacks(
            producer,
            client,
            monitor,
            statistics_enabled: statistics_enabled
          )

          # This callback is not global and is per client, thus we do not have to wrap it with a
          # callbacks manager to make it work
          client.delivery_callback = Instrumentation::Callbacks::Delivery.new(
            producer.id,
            producer.transactional?,
            monitor
          )

          subscribe_oauth_listener(producer, monitor)
          activate_client(producer, client, kafka_config)

          client
        end

        private

        # Decides whether librdkafka statistics should be enabled for this client and returns
        # the (possibly mutated) kafka config together with the decision.
        #
        # When no one is subscribed to `statistics.emitted` at the time the underlying rdkafka
        # client is being built, we force `statistics.interval.ms` to 0 regardless of user
        # configuration. This prevents librdkafka from computing statistics periodically and
        # saves a significant number of allocations on the Ruby side (no JSON parsing, no
        # statistics hash materialization, no decorator work). Any listener subscribed after
        # the client has been built will not receive `statistics.emitted` events because
        # librdkafka never emits them in the first place — to use statistics, subscribe a
        # listener BEFORE the first producer use.
        #
        # When statistics end up disabled (either because the user explicitly set the interval
        # to 0, or because we forced it to 0 here), we freeze the statistics listener slot on
        # the monitor. Any later subscription attempt raises instead of silently being a no-op,
        # surfacing the timing mistake to the user immediately.
        #
        # @param kafka_config [Hash] kafka config hash taken from the producer config
        # @param monitor [WaterDrop::Instrumentation::Monitor] per-producer monitor
        # @return [Array] two-element array `[kafka_config, statistics_enabled]`. The returned
        #   hash is a duped copy when we need to mutate the interval, so the producer's own
        #   config hash is never touched.
        def prepare_statistics(kafka_config, monitor)
          statistics_enabled = kafka_config[:"statistics.interval.ms"].to_i.positive?

          if statistics_enabled && !statistics_listener?(monitor)
            kafka_config = kafka_config.dup
            kafka_config[:"statistics.interval.ms"] = 0
            statistics_enabled = false
          end

          monitor.freeze_statistics_listeners! unless statistics_enabled

          [kafka_config, statistics_enabled]
        end

        # Instantiates the underlying rdkafka producer with the correct polling options. When
        # FD polling is enabled, we disable librdkafka's native background polling thread and
        # use our own Ruby-based poller instead.
        #
        # @param producer [WaterDrop::Producer]
        # @param kafka_config [Hash]
        # @return [::Rdkafka::Producer]
        def build_rdkafka_client(producer, kafka_config)
          producer_options = { native_kafka_auto_start: false }
          producer_options[:run_polling_thread] = false if producer.fd_polling?

          ::Rdkafka::Config.new(kafka_config).producer(**producer_options)
        end

        # Registers the global callbacks (statistics, error, oauth refresh) for this producer
        # on the shared `Karafka::Core::Instrumentation` managers. The statistics callback is
        # only registered when librdkafka is actually going to emit statistics — otherwise it
        # would never fire and would only waste memory and a manager slot.
        #
        # @param producer [WaterDrop::Producer]
        # @param client [::Rdkafka::Producer]
        # @param monitor [WaterDrop::Instrumentation::Monitor]
        # @param statistics_enabled [Boolean]
        def register_instrumentation_callbacks(producer, client, monitor, statistics_enabled:)
          if statistics_enabled
            ::Karafka::Core::Instrumentation.statistics_callbacks.add(
              producer.id,
              Instrumentation::Callbacks::Statistics.new(
                producer.id,
                client.name,
                monitor,
                producer.config.statistics_decorator
              )
            )
          end

          ::Karafka::Core::Instrumentation.error_callbacks.add(
            producer.id,
            Instrumentation::Callbacks::Error.new(producer.id, client.name, monitor)
          )

          ::Karafka::Core::Instrumentation.oauthbearer_token_refresh_callbacks.add(
            producer.id,
            Instrumentation::Callbacks::OauthbearerTokenRefresh.new(client, monitor)
          )
        end

        # Subscribes the oauth bearer token refresh listener to the monitor if one is configured.
        #
        # We need to subscribe it here because we want it to be ready before any producer
        # callbacks run. In theory because the WaterDrop rdkafka producer is lazy loaded, the
        # user would have enough time to subscribe it himself, but then it would not coop with
        # auto-configuration coming from Karafka. The way it is done here, if it is configured
        # it will be subscribed and if not, the user always can subscribe it himself as long as
        # it is done prior to first usage.
        #
        # @param producer [WaterDrop::Producer]
        # @param monitor [WaterDrop::Instrumentation::Monitor]
        def subscribe_oauth_listener(producer, monitor)
          oauth_listener = producer.config.oauth.token_provider_listener
          monitor.subscribe(oauth_listener) if oauth_listener
        end

        # Transitions the freshly built client into an active state: starts the native side,
        # registers it with our FD poller (when FD polling is enabled), and initializes
        # transactions if the user configured a transactional id. Must run last so all
        # callbacks are already wired up before the client goes live.
        #
        # @param producer [WaterDrop::Producer]
        # @param client [::Rdkafka::Producer]
        # @param kafka_config [Hash]
        def activate_client(producer, client, kafka_config)
          client.start

          # Register with poller if FD polling is enabled. Uses the producer's configured poller
          # (custom or global singleton). This must happen after client.start to ensure the
          # client is ready.
          producer.poller.register(producer, client) if producer.fd_polling?

          # Switch to transactional mode if user provided a transactional id
          client.init_transactions if kafka_config.key?(:"transactional.id")
        end

        # Checks whether there is at least one subscriber to the `statistics.emitted` event on
        # the per-producer monitor. We use this at client build time to decide whether to enable
        # librdkafka statistics at all.
        #
        # @param monitor [WaterDrop::Instrumentation::Monitor] per-producer monitor
        # @return [Boolean] true if any listener is registered for `statistics.emitted`
        def statistics_listener?(monitor)
          listeners = monitor.listeners["statistics.emitted"]
          listeners && !listeners.empty?
        end
      end
    end
  end
end
