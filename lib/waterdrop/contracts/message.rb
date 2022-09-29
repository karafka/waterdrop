# frozen_string_literal: true

module WaterDrop
  module Contracts
    # Contract with validation rules for validating that all the message options that
    # we provide to producer are valid and usable
    class Message < ::Karafka::Core::Contractable::Contract
      configure do |config|
        config.error_messages = YAML.safe_load(
          File.read(
            File.join(WaterDrop.gem_root, 'config', 'errors.yml')
          )
        ).fetch('en').fetch('validations').fetch('message')
      end

      # Regex to check that topic has a valid format
      TOPIC_REGEXP = /\A(\w|-|\.)+\z/

      private_constant :TOPIC_REGEXP

      attr_reader :max_payload_size

      # @param max_payload_size [Integer] max payload size
      def initialize(max_payload_size:)
        super()
        @max_payload_size = max_payload_size
      end

      required(:topic) { |val| val.is_a?(String) && TOPIC_REGEXP.match?(val) }
      required(:payload) { |val| val.nil? || val.is_a?(String) }
      optional(:key) { |val| val.nil? || (val.is_a?(String) && !val.empty?) }
      optional(:partition) { |val| val.is_a?(Integer) && val >= -1 }
      optional(:partition_key) { |val| val.nil? || (val.is_a?(String) && !val.empty?) }
      optional(:timestamp) { |val| val.nil? || (val.is_a?(Time) || val.is_a?(Integer)) }
      optional(:headers) { |val| val.nil? || val.is_a?(Hash) }

      virtual do |config, errors|
        next true unless errors.empty?
        next true unless config.key?(:headers)
        next true if config[:headers].nil?

        errors = []

        config.fetch(:headers).each do |key, value|
          errors << [%i[headers], :invalid_key_type] unless key.is_a?(String)
          errors << [%i[headers], :invalid_value_type] unless value.is_a?(String)
        end

        errors
      end

      virtual do |config, errors, validator|
        next true unless errors.empty?
        next if config[:payload].nil? # tombstone payload
        next true if config[:payload].bytesize <= validator.max_payload_size

        [[%i[payload], :max_size]]
      end
    end
  end
end
