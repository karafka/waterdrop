# frozen_string_literal: true

module WaterDrop
  # Namespace for Monitor used by WaterDrop and Karafka
  module Monitoring
    # Single notification event wrapping payload with id
    class Event
      attr_reader :id, :payload

      # @param id [String, Symbol] id of the event
      # @param payload [Hash] event payload
      def initialize(id, payload)
        @id = id
        @payload = payload
      end

      # Hash access to the payload data (if present)
      #
      # @param [String, Symbol] name
      def [](name)
        @payload.fetch(name)
      end
    end
  end
end
