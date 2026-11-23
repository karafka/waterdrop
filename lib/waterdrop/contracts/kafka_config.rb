# frozen_string_literal: true

module WaterDrop
  module Contracts
    # Contract with validation rules for the kafka settings the rdkafka client needs to connect.
    # It runs when the rdkafka client is built, not on setup, so producers with other clients
    # (`Buffered`, `Dummy`, test clients) do not need a reachable cluster config.
    class KafkaConfig < ::Karafka::Core::Contractable::Contract
      configure do |config|
        config.error_messages = YAML.safe_load_file(
          File.join(WaterDrop.gem_root, "config", "locales", "errors.yml")
        ).fetch("en").fetch("validations").fetch("config")
      end

      nested(:kafka) do
        required(:"bootstrap.servers") { |val| val.is_a?(String) && !val.empty? }
      end
    end
  end
end
