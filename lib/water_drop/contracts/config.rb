# frozen_string_literal: true

module WaterDrop
  module Contracts
    # Contract with validation rules for WaterDrop configuration details
    class Config < Base
      # Ensure valid format of each seed broker so that rdkafka doesn't fail silently
      SEED_BROKER_FORMAT_REGEXP = %r{\A([^:/,]+:[0-9]+)(,[^:/,]+:[0-9]+)*\z}

      private_constant :SEED_BROKER_FORMAT_REGEXP

      params do
        required(:id).filled(:str?)
        required(:logger).filled
        required(:deliver).filled(:bool?)
        required(:max_payload_size).filled(:int?, gteq?: 1)
        required(:max_wait_timeout).filled(:number?, gteq?: 0)
        required(:wait_timeout).filled(:number?, gt?: 0)

        required(:kafka).schema do
          required(:'bootstrap.servers').filled(:str?, format?: SEED_BROKER_FORMAT_REGEXP)
        end
      end
    end
  end
end
