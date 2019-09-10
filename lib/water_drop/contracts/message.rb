# frozen_string_literal: true

module WaterDrop
  module Contracts
    # Contract with validation rules for validating that all the message options that
    # we provide to producer ale valid and usable
    class Message < Dry::Validation::Contract
      # Regex to check that topic has a valid format
      TOPIC_REGEXP = /\A(\w|\-|\.)+\z/.freeze

      private_constant :TOPIC_REGEXP

      config.messages.load_paths << File.join(WaterDrop.gem_root, 'config', 'errors.yml')

      option :max_payload_size

      params do
        required(:topic).filled(:str?, format?: TOPIC_REGEXP)
        required(:payload).filled(:str?)
        optional(:key).maybe(:str?, :filled?)
        optional(:partition).filled(:int?, gteq?: -1)
        optional(:timestamp).maybe { time? | int? }
        optional(:headers).maybe(:hash?)
      end

      rule(:headers) do
        next unless value.is_a?(Hash)

        assertion = ->(value) { value.is_a?(String) }

        key.failure(:invalid_key_type) unless value.keys.all?(&assertion)
        key.failure(:invalid_value_type) unless value.values.all?(&assertion)
      end

      rule(:payload) do
        key.failure(:max_payload_size) if value.bytesize > max_payload_size
      end
    end
  end
end
