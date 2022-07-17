# frozen_string_literal: true

module WaterDrop
  module Contractable
    # Representation of a validaton result with resolved error messages
    class Result
      # File with error messages
      ERROR_MESSAGES = YAML.safe_load(
        File.read(
          File.join(WaterDrop.gem_root, 'config', 'errors.yml')
        )
      )

      attr_reader :errors

      # Builds a result object and remaps (if needed) error keys to proper error messages
      #
      # @param errors [Array<Array>] array with sub-arrays with paths and error keys
      def initialize(errors)
        # Short track to skip object allocation for the happy path
        if errors.empty?
          @errors = errors
          return
        end

        messages = ERROR_MESSAGES['en']['validations']

        hashed = {}

        errors.each do |error|
          scope = error.first.map(&:to_s).join('.').to_sym

          hashed[scope.to_sym] = messages[error.last.to_s] || messages.fetch("#{scope}_#{error.last}")
        end

        @errors = hashed
      end

      # @return [Boolean] true if no errors
      def success?
        errors.empty?
      end
    end
  end
end
