# frozen_string_literal: true

module WaterDrop
  module Contracts
    # Contract for validating tombstone message requirements beyond what the standard
    # Message contract checks. Tombstones require a non-nil key and an explicit partition.
    class Tombstone < ::Karafka::Core::Contractable::Contract
      configure do |config|
        config.error_messages = YAML.safe_load_file(
          File.join(WaterDrop.gem_root, "config", "locales", "errors.yml")
        ).fetch("en").fetch("validations").fetch("tombstone")
      end

      required(:key) { |val| val.is_a?(String) && !val.empty? }
      required(:partition) { |val| val.is_a?(Integer) && val >= 0 }
    end
  end
end
