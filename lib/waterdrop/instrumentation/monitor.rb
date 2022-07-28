# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    # WaterDrop instrumentation monitor that we use to publish events
    # By default uses our internal notifications bus but can be used with
    # `ActiveSupport::Notifications` as well
    class Monitor < ::Karafka::Core::Monitoring::Monitor
      # @param notifications_bus [Object] either our internal notifications bus or
      #   `ActiveSupport::Notifications`
      # @param namespace [String, nil] namespace for events or nil if no namespace
      def initialize(
        notifications_bus = WaterDrop::Instrumentation::Notifications.new,
        namespace = nil
      )
        super(notifications_bus, namespace)
      end
    end
  end
end
