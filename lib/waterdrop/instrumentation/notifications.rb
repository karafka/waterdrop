# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    # Instrumented is used to hookup external monitoring services to monitor how WaterDrop works
    class Notifications < ::Karafka::Core::Monitoring::Notifications
      # List of events that we support in the system and to which a monitor client can hook up
      # @note The non-error once support timestamp benchmarking
      EVENTS = %w[
        producer.connected
        producer.closing
        producer.closed
        producer.reloaded

        message.produced_async
        message.produced_sync
        message.acknowledged
        message.purged
        message.buffered

        messages.produced_async
        messages.produced_sync
        messages.buffered

        oauthbearer.token_refresh

        transaction.started
        transaction.committed
        transaction.aborted
        transaction.marked_as_consumed
        transaction.finished

        buffer.flushed_async
        buffer.flushed_sync
        buffer.purged

        statistics.emitted

        error.occurred
      ].freeze

      # @return [WaterDrop::Instrumentation::Monitor] monitor instance for system instrumentation
      def initialize
        super
        EVENTS.each { |event| register_event(event) }
      end
    end
  end
end
