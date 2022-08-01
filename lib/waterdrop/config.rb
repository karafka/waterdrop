# frozen_string_literal: true

# Configuration and descriptions are based on the delivery boy zendesk gem
# @see https://github.com/zendesk/delivery_boy
module WaterDrop
  # Configuration object for setting up all options required by WaterDrop
  class Config
    include ::Karafka::Core::Configurable

    # Defaults for kafka settings, that will be overwritten only if not present already
    KAFKA_DEFAULTS = {
      'client.id': 'waterdrop'
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
      constructor: ->(id) { id || SecureRandom.uuid }
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
    #   longer than the timeout.
    setting :max_wait_timeout, default: 5
    # option [Numeric] how long should we wait between re-checks on the availability of the
    #   delivery report. In a really robust systems, this describes the min-delivery time
    #   for a single sync message when produced in isolation
    setting :wait_timeout, default: 0.005 # 5 milliseconds
    # option [Boolean] should we send messages. Setting this to false can be really useful when
    #   testing and or developing because when set to false, won't actually ping Kafka but will
    #   run all the validations, etc
    setting :deliver, default: true
    # rdkafka options
    # @see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    setting :kafka, default: {}

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
