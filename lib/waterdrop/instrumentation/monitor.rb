# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    # WaterDrop instrumentation monitor that we use to publish events
    # By default uses our internal notifications bus but can be used with
    # `ActiveSupport::Notifications` as well
    class Monitor < ::Karafka::Core::Monitoring::Monitor
      # Event name for librdkafka statistics emissions
      STATISTICS_EVENT = "statistics.emitted"

      # Method name a listener object must implement in order to receive
      # `statistics.emitted` events via object-based subscription
      STATISTICS_LISTENER_METHOD = :on_statistics_emitted

      private_constant :STATISTICS_EVENT, :STATISTICS_LISTENER_METHOD

      # @param notifications_bus [Object] either our internal notifications bus or
      #   `ActiveSupport::Notifications`
      # @param namespace [String, nil] namespace for events or nil if no namespace
      def initialize(
        notifications_bus = WaterDrop::Instrumentation::Notifications.new,
        namespace = nil
      )
        super
        @statistics_listeners_frozen = false
      end

      # Marks this monitor as no longer accepting new subscriptions to `statistics.emitted`.
      # Called by the rdkafka client builder when it decides to leave librdkafka statistics
      # disabled (because no listener was present at build time). Any subsequent attempt to
      # subscribe to `statistics.emitted` — either via a block or via a listener object that
      # responds to `on_statistics_emitted` — will raise
      # `WaterDrop::Errors::StatisticsNotEnabledError` instead of silently doing nothing.
      def freeze_statistics_listeners!
        @statistics_listeners_frozen = true
      end

      # @return [Boolean] true if new subscriptions to `statistics.emitted` are rejected
      def statistics_listeners_frozen?
        @statistics_listeners_frozen
      end

      # Subscribes to the notifications bus, raising if the user tries to subscribe to
      # `statistics.emitted` after statistics have been disabled at client build time. This
      # prevents the "silent nothing" pitfall where a user expects statistics but no events
      # ever arrive because librdkafka statistics were turned off entirely.
      #
      # @param event_id_or_listener [String, Symbol, Object] event id (with block) or listener
      # @param block [Proc, nil] handler block when subscribing to a named event
      # @raise [WaterDrop::Errors::StatisticsNotEnabledError] when the subscription targets
      #   `statistics.emitted` and this monitor has been frozen for statistics
      def subscribe(event_id_or_listener, &block)
        if @statistics_listeners_frozen && targets_statistics?(event_id_or_listener, block)
          raise Errors::StatisticsNotEnabledError, <<~MSG.tr("\n", " ").strip
            Cannot subscribe to `statistics.emitted` after the producer has been connected.
            Statistics are disabled because no listener was subscribed before the underlying
            rdkafka client was built, so librdkafka is not emitting statistics at all.
            Subscribe your listener BEFORE the first producer use (before the underlying
            client is lazily initialized), or explicitly keep statistics enabled by leaving
            a listener in place at build time.
          MSG
        end

        super
      end

      private

      # Determines whether a subscription call targets `statistics.emitted`. Handles both
      # block-based subscription (where the first argument is the event id string) and
      # listener-object subscription (where the listener responds to `on_statistics_emitted`).
      #
      # @param event_id_or_listener [String, Symbol, Object]
      # @param block [Proc, nil]
      # @return [Boolean]
      def targets_statistics?(event_id_or_listener, block)
        if block
          event_id_or_listener.to_s == STATISTICS_EVENT
        else
          event_id_or_listener.respond_to?(STATISTICS_LISTENER_METHOD)
        end
      end
    end
  end
end
