# frozen_string_literal: true

module WaterDrop
  module Contracts
    # Context validator to ensure basic sanity of the context alteration data
    class Context < ::Karafka::Core::Contractable::Contract
      # Taken from librdkafka config
      # Those values can be changed on a per topic basis. We do not support experimental or
      # deprecated values. We also do not support settings that would break rdkafka-ruby
      #
      # @see https://karafka.io/docs/Librdkafka-Configuration/#topic-configuration-properties
      TOPIC_CONFIG_KEYS = %w[
        request.required.acks
        acks
        request.timeout.ms
        message.timeout.ms
        delivery.timeout.ms
        partitioner
        compression.codec
        compression.type
        compression.level
      ].freeze

      # Boolean values
      BOOLEANS = [true, false].freeze

      private_constant :TOPIC_CONFIG_KEYS, :BOOLEANS

      configure do |config|
        config.error_messages = YAML.safe_load(
          File.read(
            File.join(WaterDrop.gem_root, 'config', 'locales', 'errors.yml')
          )
        ).fetch('en').fetch('validations').fetch('context')

        required(:default) { |val| BOOLEANS.include?(val) }
      end
    end
  end
end
