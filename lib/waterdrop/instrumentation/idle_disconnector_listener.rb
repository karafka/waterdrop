# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    # Idle disconnector listener that monitors producer activity and automatically disconnects
    # idle producers to preserve TCP connections
    #
    # This listener subscribes to statistics.emitted events and tracks the txmsgs (transmitted
    # messages) count. If the producer doesn't send any messages for a configurable timeout
    # period, it will automatically disconnect the producer.
    #
    # @note We do not have to worry about the running transactions or buffer being used because
    #   the disconnect is graceful and will not disconnect unless it is allowed to. This is why
    #   we can simplify things and take interest only in txmsgs.
    #
    # @note For convenience, WaterDrop provides a config shortcut. Instead of manually subscribing
    #   this listener, you can simply set `config.idle_disconnect_timeout` in your producer config.
    #
    # @example Using config shortcut (recommended)
    #   WaterDrop::Producer.new do |config|
    #     config.idle_disconnect_timeout = 5 * 60 * 1000 # 5 minutes
    #   end
    #
    # @example Manual listener usage with 5 minute timeout
    #   producer.monitor.subscribe(
    #     WaterDrop::Instrumentation::IdleDisconnectorListener.new(
    #       producer,
    #       disconnect_timeout: 5 * 60 * 1000)
    #   )
    #
    # @example Usage with custom timeout
    #   idle_disconnector = WaterDrop::Instrumentation::IdleDisconnectorListener.new(
    #     producer,
    #     disconnect_timeout: 10 * 60 * 1000
    #   )
    #   producer.monitor.subscribe(idle_disconnector)
    class IdleDisconnectorListener
      include ::Karafka::Core::Helpers::Time

      # @param producer [WaterDrop::Producer] the producer instance to monitor
      # @param disconnect_timeout [Integer] timeout in milliseconds before disconnecting
      #   (default: 5 minutes). Be aware that if you set it to a value lower than statistics
      #   publishing interval (5 seconds by default) it may be to aggressive in closing
      def initialize(producer, disconnect_timeout: 5 * 60 * 1_000)
        @producer = producer
        @disconnect_timeout = disconnect_timeout
        # We set this initially to -1 so any statistics change triggers a change to prevent an
        # early shutdown
        @last_txmsgs = -1
        @last_activity_time = monotonic_now
      end

      # This method is called automatically when the listener is subscribed to the monitor
      # using producer.monitor.subscribe(listener_instance)
      #
      # @param event [Hash] the statistics event containing producer statistics
      def on_statistics_emitted(event)
        call(event[:statistics])
      end

      private

      # Handles statistics.emitted events to monitor message transmission activity
      # @param statistics [Hash] producer librdkafka statistics
      def call(statistics)
        current_txmsgs = statistics.fetch('txmsgs', 0)
        current_time = monotonic_now

        # Update activity if messages changed
        if current_txmsgs != @last_txmsgs
          @last_txmsgs = current_txmsgs
          @last_activity_time = current_time

          return
        end

        # Check for timeout and attempt disconnect
        return unless (current_time - @last_activity_time) >= @disconnect_timeout

        # Since the statistics operations happen from the rdkafka native thread. we cannot close
        # it from itself as you cannot join on yourself as it would cause a deadlock. We spawn
        # a thread to do this
        # We do an early check if producer is in a viable state for a disconnect so in case its
        # internal state would prevent us from disconnecting, we won't be spamming with new thread
        # creation
        Thread.new { @producer.disconnect } if @producer.disconnectable?

        # We change this always because:
        #   - if we were able to disconnect, this should give us time before any potential future
        #     attempts. While they should not happen because events won't be published on a
        #     disconnected producer, this may still with frequent events be called post disconnect
        #   - if we were not able to disconnect, it means that there was something in the producer
        #     state that prevent it, and we consider this as activity as well
        @last_activity_time = current_time
      rescue StandardError => e
        @monitor.instrument(
          'error.occurred',
          producer_id: @producer.id,
          error: e,
          type: 'producer.disconnect.error'
        )
      end
    end
  end
end
