# frozen_string_literal: true

module WaterDrop
  module Contracts
    # Base for all the contracts in WaterDrop
    class Base < Dry::Validation::Contract
      config.messages.load_paths << File.join(WaterDrop.gem_root, 'config', 'errors.yml')

      # @param data [Hash] data for validation
      # @param error_class [Class] error class that should be used when validation fails
      # @return [Boolean] true if all good or exception is raised
      def validate!(data, error_class)
        result = call(data)

        return true if result.success?

        raise error_class, result.errors.to_h
      end
    end
  end
end
