# frozen_string_literal: true

module Factories
  # Message factories
  module Message
    # @param overrides [Hash] keys we want to override
    # @return [Hash] valid message
    def valid_message_factory(overrides = {})
      defaults = {
        topic: "it-#{SecureRandom.uuid}",
        payload: rand.to_s,
        partition_key: nil,
        label: nil,
        headers: {}
      }

      message = {}
      attributes = defaults.merge(overrides)

      message[:topic] = attributes[:topic]
      message[:payload] = attributes[:payload]
      message[:partition_key] = attributes[:partition_key] if attributes[:partition_key]
      message[:label] = attributes[:label] if attributes[:label]
      message[:headers] = attributes[:headers] if attributes[:headers]

      message
    end

    # @param _overrides [Hash]
    # @return [Hash] invalid message
    def invalid_message_factory(_overrides = {})
      {}
    end
  end
end
