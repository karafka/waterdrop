# frozen_string_literal: true

module WaterDrop
  module Contracts
    # Contract with validation rules for WaterDrop configuration details

    # Use comma as a separator between multiple seed brokers
    SEED_BROKERS_SEPARATOR = ','
    # Ensure valid format of each seed broker so that rdkafka doesn't fail silently
    SEED_BROKER_FORMAT_REGEXP = %r{(?=^((?!\:\/\/).)*$).+\:\d+}.freeze
    private_constant :SEED_BROKERS_SEPARATOR, :SEED_BROKER_FORMAT_REGEXP

    class Config < Dry::Validation::Contract
      params do
        required(:id).filled(:str?)
        required(:logger).filled
        required(:deliver).filled(:bool?)
        required(:kafka).filled(:hash?)
        required(:max_payload_size).filled(:int?, gteq?: 1)
        required(:max_wait_timeout).filled(:number?, gteq?: 0)
        required(:wait_timeout).filled(:number?, gt?: 0)
      end

      rule(:kafka) do
        next unless value.is_a?(Hash)

        bootstrap_servers = value.symbolize_keys[:"bootstrap.servers"].to_s
        if bootstrap_servers.to_s.empty?
          key.failure(:bootstrap_servers_must_be_filled)
          next
        end

        seed_broker_has_valid_format = lambda do |seed_broker|
          SEED_BROKER_FORMAT_REGEXP.match?(seed_broker)
        end
        unless bootstrap_servers.split(SEED_BROKERS_SEPARATOR).all?(&seed_broker_has_valid_format)
          key.failure(:invalid_seed_brokers_format)
        end
      end
    end
  end
end
