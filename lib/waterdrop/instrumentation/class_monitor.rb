# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    # WaterDrop class-level instrumentation monitor for global events
    # This monitor only supports class-level lifecycle events, not per-producer events
    class ClassMonitor < ::Karafka::Core::Monitoring::Monitor
      # @param notifications_bus [Object] class-level notifications bus
      # @param namespace [String, nil] namespace for events or nil if no namespace
      def initialize(
        notifications_bus = WaterDrop::Instrumentation::ClassNotifications.new,
        namespace = nil
      )
        super
      end
    end
  end
end
