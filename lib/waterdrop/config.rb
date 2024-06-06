# frozen_string_literal: true

# Configuration and descriptions are based on the delivery boy zendesk gem
# @see https://github.com/zendesk/delivery_boy
module WaterDrop
  # Configuration object for setting up all options required by WaterDrop
  class Config
    include ::Karafka::Core::Configurable

    # Defaults for kafka settings, that will be overwritten only if not present already
    KAFKA_DEFAULTS = {
      'client.id': 'waterdrop',
      # emit librdkafka statistics every five seconds. This is used in instrumentation.
      # When disabled, part of metrics will not be published and available.
      'statistics.interval.ms': 5_000,
      # We set it to a value that is lower than `max_wait_timeout` to have a final verdict upon
      # sync delivery
      'message.timeout.ms': 50_000,
      # Must be more or equal to `message.timeout.ms` defaults
      'transaction.timeout.ms': 55_000
    }.freeze

    private_constant :KAFKA_DEFAULTS

    # WaterDrop options
    #
    # option [String] id of the producer. This can be helpful when building producer specific
    #   instrumentation or loggers. It is not the kafka client id. It is an id that should be
    #   unique for each of the producers
    setting(
      :id,
      default: false,
      constructor: ->(id) { id || "waterdrop-#{SecureRandom.hex(6)}" }
    )
    # option [Instance] logger that we want to use
    # @note Due to how rdkafka works, this setting is global for all the producers
    setting(
      :logger,
      default: false,
      constructor: ->(logger) { logger || Logger.new($stdout, level: Logger::WARN) }
    )
    # option [Instance] monitor that we want to use. See instrumentation part of the README for
    #   more details
    setting(
      :monitor,
      default: false,
      constructor: ->(monitor) { monitor || WaterDrop::Instrumentation::Monitor.new }
    )
    # option [Integer] max payload size allowed for delivery to Kafka
    setting :max_payload_size, default: 1_000_012
    # option [Integer] Wait that long for the delivery report or raise an error if this takes
    #   longer than the timeout ms.
    setting :max_wait_timeout, default: 60_000
    # option [Boolean] should we upon detecting full librdkafka queue backoff and retry or should
    #   we raise an exception.
    #   When this is set to `true`, upon full queue, we won't raise an error. There will be error
    #   in the `error.occurred` notification pipeline with a proper type as while this is
    #   recoverable, in a high number it still may mean issues.
    #   Waiting is one of the recommended strategies.
    setting :wait_on_queue_full, default: true
    # option [Integer] how long (in seconds) should we backoff before a retry when queue is full
    #   The retry will happen with the same message and backoff should give us some time to
    #   dispatch previously buffered messages.
    setting :wait_backoff_on_queue_full, default: 100
    # option [Numeric] how many ms should we wait with the backoff on queue having space for
    # more messages before re-raising the error.
    setting :wait_timeout_on_queue_full, default: 10_000
    # option [Boolean] should we instrument non-critical, retryable queue full errors
    setting :instrument_on_wait_queue_full, default: true
    # option [Numeric] How long to wait before retrying a retryable transaction related error
    setting :wait_backoff_on_transaction_command, default: 500
    # option [Numeric] How many times to retry a retryable transaction related error before
    #   giving up
    setting :max_attempts_on_transaction_command, default: 5
    # When a fatal transactional error occurs, should we close and recreate the underlying producer
    # to keep going or should we stop. Since we will open a new instance and the failed transaction
    # anyhow rolls back, we should be able to safely reload.
    setting :reload_on_transaction_fatal_error, default: true

    # option [Boolean] should we send messages. Setting this to false can be really useful when
    #   testing and or developing because when set to false, won't actually ping Kafka but will
    #   run all the validations, etc
    setting :deliver, default: true
    # option [Class] class for usage when creating the underlying client used to dispatch messages
    setting :client_class, default: Clients::Rdkafka
    # rdkafka options
    # @see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    setting :kafka, default: {}
    # Middleware chain that can be expanded with useful middleware steps
    setting(
      :middleware,
      default: false,
      constructor: ->(middleware) { middleware || WaterDrop::Middleware.new }
    )

    # Namespace for oauth related configuration
    setting :oauth do
      # option [false, #call] Listener for using oauth bearer. This listener will be able to
      #   get the client name to decide whether to use a single multi-client token refreshing
      #   or have separate tokens per instance.
      setting :token_provider_listener, default: false
    end

    # Configuration method
    # @yield Runs a block of code providing a config singleton instance to it
    # @yieldparam [WaterDrop::Config] WaterDrop config instance
    def setup
      configure do |config|
        yield(config)

        merge_kafka_defaults!(config)

        Contracts::Config.new.validate!(config.to_h, Errors::ConfigurationInvalidError)

        ::Rdkafka::Config.logger = config.logger
      end

      self
    end

    private

    # Propagates the kafka setting defaults unless they are already present
    # This makes it easier to set some values that users usually don't change but still allows them
    # to overwrite the whole hash if they want to
    # @param config [Karafka::Core::Configurable::Node] config of this producer
    def merge_kafka_defaults!(config)
      KAFKA_DEFAULTS.each do |key, value|
        next if config.kafka.key?(key)

        config.kafka[key] = value
      end
    end
  end
end
