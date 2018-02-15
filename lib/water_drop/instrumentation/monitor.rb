# frozen_string_literal: true

module WaterDrop
  # Namespace for all the things related with WaterDrop instrumentation process
  module Instrumentation
    # Monitor is used to hookup external monitoring services to monitor how WaterDrop works
    # Since it is a pub-sub based on dry-monitor, you can use as many subscribers/loggers at the
    # same time, which means that you might have for example file logging and newrelic at the same
    # time
    # @note This class acts as a singleton because we are only permitted to have single monitor
    #   per running process (just as logger)
    class Monitor < Dry::Monitor::Notifications
      include Singleton

      # List of events that we support in the system and to which a monitor client can hook up
      # @note The non-error once support timestamp benchmarking
      BASE_EVENTS = %w[
        async_producer.call.error
        async_producer.call.retry
        sync_producer.call.error
        sync_producer.call.retry
      ].freeze

      private_constant :BASE_EVENTS

      # @return [WaterDrop::Instrumentation::Monitor] monitor instance for system instrumentation
      def initialize
        super(:waterdrop)
        BASE_EVENTS.each(&method(:register_event))
      end

      # Allows us to subscribe to events with a code that will be yielded upon events
      # @param event_name_or_listener [String, Object] name of the event we want to subscribe to
      #   or a listener if we decide to go with object listener
      def subscribe(event_name_or_listener)
        return super unless event_name_or_listener.is_a?(String)
        return super if available_events.include?(event_name_or_listener)
        raise Errors::UnregisteredMonitorEvent, event_name_or_listener
      end

      # @return [Array<String>] names of available events to which we can subscribe
      def available_events
        __bus__.events.keys
      end
    end
  end
end
