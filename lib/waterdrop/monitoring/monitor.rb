# frozen_string_literal: true

module WaterDrop
  module Monitoring
    # Karafka monitor that can be used to pass through instrumentation calls to selected
    # notifications bus.
    #
    # It provides abstraction layer that allows us to use both our internal notifications as well
    # as `ActiveSupport::Notifications`.
    class Monitor
      EMPTY_HASH = {}.freeze

      # @param notifications_bus [Object] either our internal notifications bus or
      #   `ActiveSupport::Notifications`
      # @param namespace [String, nil] namespace for events or nil if no namespace
      def initialize(notifications_bus, namespace = nil)
        @notifications_bus = notifications_bus
        @namespace = namespace
        @mapped_events = Concurrent::Map.new
      end

      # Passes the instrumentation block (if any) into the notifications bus
      #
      # @param event_id [String, Symbol] event id
      # @param payload [Hash]
      # @param block [Proc] block we want to instrument (if any)
      def instrument(event_id, payload = EMPTY_HASH, &block)
        full_event_name = @mapped_events[event_id] ||= [event_id, @namespace].compact.join('.')

        @notifications_bus.instrument(full_event_name, payload, &block)
      end

      # Allows us to subscribe to the notification bus
      #
      # @param args [Array] any arguments that the notification bus subscription layer accepts
      # @param block [Proc] optional block for subscription
      def subscribe(*args, &block)
        @notifications_bus.subscribe(*args, &block)
      end
    end
  end
end
