# frozen_string_literal: true

module WaterDrop
  module Contracts
    # Contract with validation rules for validating that all the message options that
    # we provide to producer ale valid and usable
    class Message < Dry::Validation::Contract
      # Regex to check that topic has a valid format
      TOPIC_REGEXP = /\A(\w|-|\.)+\z/.freeze

      # Checks, that the given value is a string
      STRING_ASSERTION = ->(value) { value.is_a?(String) }.to_proc

      private_constant :TOPIC_REGEXP, :STRING_ASSERTION

      config.messages.load_paths << File.join(WaterDrop.gem_root, 'config', 'errors.yml')

      option :max_payload_size

      params do
        required(:topic).filled(:str?, format?: TOPIC_REGEXP)
        required(:payload).filled(:str?)
        optional(:key).maybe(:str?, :filled?)
        optional(:partition).filled(:int?, gteq?: -1)
        optional(:partition_key).maybe(:str?, :filled?)
        optional(:timestamp).maybe { time? | int? }
        optional(:headers).maybe(:hash?)
      end

      rule(:headers) do
        next unless value.is_a?(Hash)

        key.failure(:invalid_key_type) unless value.keys.all?(&STRING_ASSERTION)
        key.failure(:invalid_value_type) unless value.values.all?(&STRING_ASSERTION)
      end

      rule(:payload) do
        key.failure(:max_payload_size) if value.bytesize > max_payload_size
      end
    end
  end
end
