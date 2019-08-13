# frozen_string_literal: true

module WaterDrop
  # Namespace for all the things related with WaterDrop instrumentation process
  module Instrumentation
    # Monitor is used to hookup external monitoring services to monitor how WaterDrop works
    # Since it is a pub-sub based on dry-monitor, you can use as many subscribers/loggers at the
    # same time, which means that you might have for example file logging and NewRelic at the same
    # time
    # @note This class acts as a singleton because we are only permitted to have single monitor
    #   per running process (just as logger)
    class Monitor < Dry::Monitor::Notifications
      # List of events that we support in the system and to which a monitor client can hook up
      # @note The non-error once support timestamp benchmarking
      EVENTS = %w[
        producer.closed
        message.produced_async
        message.produced_sync
        messages.produced_async
        messages.produced_sync
        message.buffered
        messages.buffered
        buffer.flushed_async
        buffer.flushed_async.error
        buffer.flushed_sync
        buffer.flushed_sync.error
      ].freeze

      private_constant :EVENTS

      # @return [WaterDrop::Instrumentation::Monitor] monitor instance for system instrumentation
      def initialize
        super(:waterdrop)
        EVENTS.each(&method(:register_event))
      end
    end
  end
end
