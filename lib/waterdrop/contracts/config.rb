# frozen_string_literal: true

module WaterDrop
  module Contracts
    # Contract with validation rules for WaterDrop configuration details
    class Config < Contractable::Contract
      configure do |config|
        config.error_messages = YAML.safe_load(
          File.read(
            File.join(WaterDrop.gem_root, 'config', 'errors.yml')
          )
        ).fetch('en').fetch('validations').fetch('config')
      end

      required(:id) { |id| id.is_a?(String) && !id.empty? }
      required(:logger) { |logger| !logger.nil? }
      required(:deliver) { |deliver| [true, false].include?(deliver) }
      required(:max_payload_size) { |ps| ps.is_a?(Integer) && ps >= 1 }
      required(:max_wait_timeout) { |mwt| mwt.is_a?(Numeric) && mwt >= 0 }
      required(:wait_timeout) { |wt| wt.is_a?(Numeric) && wt.positive? }
      required(:kafka) { |kafka| kafka.is_a?(Hash) && !kafka.empty? }

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
