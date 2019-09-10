# frozen_string_literal: true

# Configuration and descriptions are based on the delivery boy zendesk gem
# @see https://github.com/zendesk/delivery_boy
module WaterDrop
  # Configuration object for setting up all options required by WaterDrop
  class Config
    include Dry::Configurable

    # WaterDrop options
    #
    # option [String] id of the producer. This can be helpful when building producer specific
    #   instrumentation or loggers. It is not the kafka producer id
    setting(:id, false) { |id| id || SecureRandom.uuid }
    # option [Instance] logger that we want to use
    # @note Due to how rdkafka works, this setting is global for all the producers
    setting(:logger, false) { |logger| logger || Logger.new($stdout, level: Logger::WARN) }
    # option [Instance] monitor that we want to use. See instrumentation part of the README for
    #   more details
    setting(:monitor, false) { |monitor| monitor || WaterDrop::Instrumentation::Monitor.new }
    # option [Integer] max payload size allowed for delivery to Kafka
    setting :max_payload_size, 1_000_012
    # option [Integer] Wait that long for the delivery report or raise an error if this takes
    #   longer than the timeout.
    setting :max_wait_timeout, 5
    # option [Numeric] how long should we wait between re-checks on the availability of the
    #   delivery report. In a really robust systems, this describes the min-delivery time
    #   for a single sync message when produced in isolation
    setting :wait_timeout, 0.005 # 5 milliseconds
    # option [Boolean] should we send messages. Setting this to false can be really useful when
    #   testing and or developing because when set to false, won't actually ping Kafka but will
    #   run all the validations, etc
    setting :deliver, true
    # rdkafka options
    # @see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    setting :kafka, {}

    # Configuration method
    # @yield Runs a block of code providing a config singleton instance to it
    # @yieldparam [WaterDrop::Config] WaterDrop config instance
    def setup
      configure do |config|
        yield(config)
        validate!(config.to_h)
      end
    end

    private

    # Validates the configuration and if anything is wrong, will raise an exception
    # @param config_hash [Hash] config hash with setup details
    # @raise [WaterDrop::Errors::ConfigurationInvalidError] raised when something is wrong with
    #   the configuration
    def validate!(config_hash)
      result = Contracts::Config.new.call(config_hash)
      return true if result.success?

      raise Errors::ConfigurationInvalidError, result.errors.to_h
    end
  end
end
