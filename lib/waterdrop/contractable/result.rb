# frozen_string_literal: true

module WaterDrop
  module Contractable
    # Representation of a validaton result with resolved error messages
    class Result
      attr_reader :errors

      # Builds a result object and remaps (if needed) error keys to proper error messages
      #
      # @param errors [Array<Array>] array with sub-arrays with paths and error keys
      # @param contract [Object] contract that generated the error
      def initialize(errors, contract)
        # Short track to skip object allocation for the happy path
        if errors.empty?
          @errors = errors
          return
        end

        messages = contract.class.config.error_messages

        hashed = {}

        errors.each do |error|
          scope = error.first.map(&:to_s).join('.').to_sym

          hashed[scope.to_sym] = messages.fetch(error.last.to_s) do
            messages.fetch("#{scope}_#{error.last}")
          end
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
