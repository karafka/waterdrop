# frozen_string_literal: true

module WaterDrop
  module Contractable
    # Base contract for all the contracts that check data format
    #
    # @note This contract does NOT support rules inheritance as it was never needed in Karafka
    class Contract
      extend Configurable

      # Yaml based error messages data
      setting(:error_messages)

      # Class level API definitions
      class << self
        # @return [Array<Rule>] all the validation rules defined for a given contract
        attr_reader :rules

        # Allows for definition of a scope/namespace for nested validations
        #
        # @param path [Symbol] path in the hash for nesting
        # @param block [Proc] nested rule code or more nestings inside
        #
        # @example
        #   nested(:key) do
        #     required(:inside) { |inside| inside.is_a?(String) }
        #   end
        def nested(path, &block)
          init_accu
          @nested << path
          instance_eval(&block)
          @nested = []
        end

        # Defines a rule for a required field (required means, that will automatically create an
        # error if missing)
        #
        # @param keys [Array<Symbol>] single or full path
        # @param block [Proc] validation rule
        def required(*keys, &block)
          init_accu
          @rules << Rule.new(@nested + keys, :required, block).freeze
        end

        # @param keys [Array<Symbol>] single or full path
        # @param block [Proc] validation rule
        def optional(*keys, &block)
          init_accu
          @rules << Rule.new(@nested + keys, :optional, block).freeze
        end

        # @param block [Proc] validation rule
        #
        # @note Virtual rules have different result expectations. Please see contracts or specs for
        #   details.
        def virtual(&block)
          init_accu
          @rules << Rule.new([], :virtual, block).freeze
        end

        private

        # Initializes nestings and rules building accumulator
        def init_accu
          @nested ||= []
          @rules ||= []
        end
      end

      # Runs the validation
      #
      # @param data [Hash] hash with data we want to validate
      # @return [Result] validaton result
      def call(data)
        errors = []

        self.class.rules.map do |rule|
          case rule.type
          when :required
            validate_required(data, rule, errors)
          when :optional
            validate_optional(data, rule, errors)
          when :virtual
            validate_virtual(data, rule, errors)
          else
            raise ArgumentError, rule.type
          end
        end

        Result.new(errors, self)
      end

      # @param data [Hash] data for validation
      # @param error_class [Class] error class that should be used when validation fails
      # @return [Boolean] true
      # @raise [StandardError] any error provided in the error_class that inherits from the
      #   standard error
      def validate!(data, error_class)
        result = call(data)

        return true if result.success?

        raise error_class, result.errors
      end

      private

      # Runs validation for rules on fields that are required and adds errors (if any) to the
      # errors array
      #
      # @param data [Hash] input hash
      # @param rule [Rule] validation rule
      # @param errors [Array] array with errors from previous rules (if any)
      def validate_required(data, rule, errors)
        for_checking = dig(data, rule.path)

        if for_checking.first == :match
          result = rule.validator.call(for_checking.last, data, errors, self)

          return if result == true

          errors << [rule.path, result || :format]
        else
          errors << [rule.path, :missing]
        end
      end

      # Runs validation for rules on fields that are optional and adds errors (if any) to the
      # errors array
      #
      # @param data [Hash] input hash
      # @param rule [Rule] validation rule
      # @param errors [Array] array with errors from previous rules (if any)
      def validate_optional(data, rule, errors)
        for_checking = dig(data, rule.path)

        return unless for_checking.first == :match

        result = rule.validator.call(for_checking.last, data, errors, self)

        return if result == true

        errors << [rule.path, result || :format]
      end

      # Runs validation for rules on virtual fields (aggregates, etc) and adds errors (if any) to
      # the errors array
      #
      # @param data [Hash] input hash
      # @param rule [Rule] validation rule
      # @param errors [Array] array with errors from previous rules (if any)
      def validate_virtual(data, rule, errors)
        result = rule.validator.call(data, errors, self)

        return if result == true

        errors.push(*result)
      end

      # Tries to dig for a given key in a hash and returns it with indication whether or not it was
      # possible to find it (dig returns nil and we don't know if it wasn't the digged key value)
      #
      # @param data [Hash]
      # @param keys [Array<Symbol>]
      # @return [Array<Symbol, Object>] array where the first element is `:match` or `:miss` and
      #   the digged value or nil if not found
      def dig(data, keys)
        current = data
        result = :match

        keys.each do |nesting|
          unless current.key?(nesting)
            result = :miss

            break
          end

          current = current[nesting]
        end

        [result, current]
      end
    end
  end
end
