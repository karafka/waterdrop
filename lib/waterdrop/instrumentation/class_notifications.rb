# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    # Class-level notifications for WaterDrop global instrumentation
    # This only supports events that occur at the class/module level, not per-producer instance
    class ClassNotifications < ::Karafka::Core::Monitoring::Notifications
      # List of events that are available at the class level via WaterDrop.instrumentation
      # These are lifecycle events for producer creation and configuration
      # and connection pool lifecycle events
      EVENTS = %w[
        producer.created
        producer.configured

        connection_pool.created
        connection_pool.setup
        connection_pool.shutdown
        connection_pool.reload
        connection_pool.reloaded
      ].freeze

      # @return [WaterDrop::Instrumentation::ClassNotifications] class-level notification instance
      def initialize
        super
        EVENTS.each { |event| register_event(event) }
      end
    end
  end
end
