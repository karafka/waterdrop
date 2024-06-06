# frozen_string_literal: true

module WaterDrop
  module Contracts
    # Contract with validation rules for WaterDrop configuration details
    class Config < ::Karafka::Core::Contractable::Contract
      configure do |config|
        config.error_messages = YAML.safe_load(
          File.read(
            File.join(WaterDrop.gem_root, 'config', 'locales', 'errors.yml')
          )
        ).fetch('en').fetch('validations').fetch('config')
      end

      required(:id) { |val| val.is_a?(String) && !val.empty? }
      required(:logger) { |val| !val.nil? }
      required(:monitor) { |val| !val.nil? }
      required(:deliver) { |val| [true, false].include?(val) }
      required(:max_payload_size) { |val| val.is_a?(Integer) && val >= 1 }
      required(:max_wait_timeout) { |val| val.is_a?(Numeric) && val >= 0 }
      required(:client_class) { |val| !val.nil? }
      required(:kafka) { |val| val.is_a?(Hash) && !val.empty? }
      required(:wait_on_queue_full) { |val| [true, false].include?(val) }
      required(:instrument_on_wait_queue_full) { |val| [true, false].include?(val) }
      required(:wait_backoff_on_queue_full) { |val| val.is_a?(Numeric) && val >= 0 }
      required(:wait_timeout_on_queue_full) { |val| val.is_a?(Numeric) && val >= 0 }
      required(:wait_backoff_on_transaction_command) { |val| val.is_a?(Numeric) && val >= 0 }
      required(:max_attempts_on_transaction_command) { |val| val.is_a?(Integer) && val >= 1 }
      required(:reload_on_transaction_fatal_error) { |val| [true, false].include?(val) }

      nested(:oauth) do
        required(:token_provider_listener) do |val|
          val == false || val.respond_to?(:on_oauthbearer_token_refresh)
        end
      end

      # rdkafka allows both symbols and strings as keys for config but then casts them to strings
      # This can be confusing, so we expect all keys to be symbolized
      virtual do |config, errors|
        next true unless errors.empty?

        errors = []

        config
          .fetch(:kafka)
          .keys
          .reject { |key| key.is_a?(Symbol) }
          .each { |key| errors << [[:kafka, key], :kafka_key_must_be_a_symbol] }

        errors
      end
    end
  end
end
