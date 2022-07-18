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


        hashed = {}

        errors.each do |error|
          scope = error.first.map(&:to_s).join('.').to_sym

          # This will allow for usage of custom messages instead of yaml keys if needed
          hashed[scope.to_sym] = if error.last.is_a?(String)
                                   error.last
                                 else
                                   messages = contract.class.config.error_messages

                                   messages.fetch(error.last.to_s) do
                                     messages.fetch("#{scope}_#{error.last}")
                                   end
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
