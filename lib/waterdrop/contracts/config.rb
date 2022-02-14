# frozen_string_literal: true

module WaterDrop
  module Contracts
    # Contract with validation rules for WaterDrop configuration details
    class Config < Base
      params do
        required(:id).filled(:str?)
        required(:logger).filled
        required(:deliver).filled(:bool?)
        required(:max_payload_size).filled(:int?, gteq?: 1)
        required(:max_wait_timeout).filled(:number?, gteq?: 0)
        required(:wait_timeout).filled(:number?, gt?: 0)
        required(:kafka).filled(:hash?)
      end

      # rdkafka allows both symbols and strings as keys for config but then casts them to strings
      # This can be confusing, so we expect all keys to be symbolized
      rule(:kafka) do
        next unless value.is_a?(Hash)

        value.each_key do |key|
          next if key.is_a?(Symbol)

          key(:"kafka.#{key}").failure(:kafka_key_must_be_a_symbol)
        end
      end
    end
  end
end
