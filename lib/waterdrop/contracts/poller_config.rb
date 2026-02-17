# frozen_string_literal: true

module WaterDrop
  module Contracts
    # Contract with validation rules for WaterDrop Poller configuration
    class PollerConfig < ::Karafka::Core::Contractable::Contract
      configure do |config|
        config.error_messages = YAML.safe_load_file(
          File.join(WaterDrop.gem_root, "config", "locales", "errors.yml")
        ).fetch("en").fetch("validations").fetch("poller")
      end

      required(:thread_priority) { |val| val.is_a?(Integer) && val >= -3 && val <= 3 }
      required(:poll_timeout) { |val| val.is_a?(Integer) && val >= 1 }
      required(:backoff_min) { |val| val.is_a?(Integer) && val >= 1 }
      required(:backoff_max) { |val| val.is_a?(Integer) && val >= 1 }

      virtual do |config, errors|
        next true unless errors.empty?
        next true if config[:backoff_max] >= config[:backoff_min]

        [[%i[backoff_max], :backoff_max_must_be_gte_backoff_min]]
      end
    end
  end
end
