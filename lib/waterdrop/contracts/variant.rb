# frozen_string_literal: true

module WaterDrop
  module Contracts
    # Variant validator to ensure basic sanity of the variant alteration data
    class Variant < ::Karafka::Core::Contractable::Contract
      # Taken from librdkafka config
      # Those values can be changed on a per topic basis. We do not support experimental or
      # deprecated values. We also do not support settings that would break rdkafka-ruby
      #
      # @see https://karafka.io/docs/Librdkafka-Configuration/#topic-configuration-properties
      TOPIC_CONFIG_KEYS = %i[
        acks
        compression.codec
        compression.level
        compression.type
        delivery.timeout.ms
        message.timeout.ms
        partitioner
        request.required.acks
        request.timeout.ms
      ].freeze

      # Boolean values
      BOOLEANS = [true, false].freeze

      private_constant :TOPIC_CONFIG_KEYS, :BOOLEANS

      configure do |config|
        config.error_messages = YAML.safe_load(
          File.read(
            File.join(WaterDrop.gem_root, 'config', 'locales', 'errors.yml')
          )
        ).fetch('en').fetch('validations').fetch('variant')
      end

      required(:default) { |val| BOOLEANS.include?(val) }
      required(:max_wait_timeout) { |val| val.is_a?(Numeric) && val >= 0 }

      # Checks if all keys are symbols
      virtual do |config, errors|
        next true unless errors.empty?

        errors = []

        config
          .fetch(:topic_config)
          .keys
          .reject { |key| key.is_a?(Symbol) }
          .each { |key| errors << [[:kafka, key], :kafka_key_must_be_a_symbol] }

        errors
      end

      # Checks if we have any keys that are not allowed
      virtual do |config, errors|
        next true unless errors.empty?

        errors = []

        config
          .fetch(:topic_config)
          .keys
          .reject { |key| TOPIC_CONFIG_KEYS.include?(key) }
          .each { |key| errors << [[:kafka, key], :kafka_key_not_per_topic] }

        errors
      end

      # Ensure, that acks is not changed when in transactional mode
      # acks needs to be set to 'all' and should not be changed when working with transactional
      # producer as it causes librdkafka to crash
      virtual do |config, errors|
        next true unless errors.empty?
        # Relevant only for the transactional producer
        next true unless config.fetch(:transactional)

        errors = []

        config
          .fetch(:topic_config)
          .keys
          .select { |key| key.to_s.include?('acks') }
          .each { |key| errors << [[:kafka, key], :kafka_key_acks_not_changeable] }

        errors
      end

      # Prevent from creating variants altering acks when idempotent
      virtual do |config, errors|
        next true unless errors.empty?
        # Relevant only for the transactional producer
        next true unless config.fetch(:idempotent)

        errors = []

        config
          .fetch(:topic_config)
          .keys
          .select { |key| key.to_s.include?('acks') }
          .each { |key| errors << [[:kafka, key], :kafka_key_acks_not_changeable] }

        errors
      end
    end
  end
end
