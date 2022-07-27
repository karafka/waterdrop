# frozen_string_literal: true

module WaterDrop
  module Monitoring
    # A simple notifications layer for WaterDrop and Karafka that aims to provide API compatible
    # with both `ActiveSupport::Notifications` and `dry-monitor`.
    #
    # We do not use any of them by default as our use-case is fairly simple and we do not want
    # to have too many external dependencies.
    class Notifications
      attr_reader :name

      # Raised when someone wants to publish event that was not registered
      EventNotRegistered = Class.new(StandardError)

      # Empty hash for internal referencing
      EMPTY_HASH = {}.freeze

      private_constant :EMPTY_HASH

      def initialize
        @listeners = Concurrent::Map.new { |k, v| k[v] = Concurrent::Array.new }
        # This allows us to optimize the method calling lookups
        @events_methods_map = Concurrent::Map.new
      end

      # Registers a new event on which we can publish
      #
      # @param event_id [String, Symbol] event id
      def register_event(event_id)
        @listeners[event_id]
        @events_methods_map[event_id] = :"on_#{event_id.to_s.tr('.', '_')}"
      end

      # Allows for subscription to an event
      # There are two ways you can subscribe: via block or via listener.
      #
      # @param event_id_or_listener [Object] event id when we want to subscribe to a particular
      #   event with a block or listener if we want to subscribe with general listener
      # @param block [Proc] block of code if we want to subscribe with it
      #
      # @example Subscribe using listener
      #   subscribe(MyListener.new)
      #
      # @example Subscribe via block
      #   subscribe do |event|
      #     puts event
      #   end
      def subscribe(event_id_or_listener, &block)
        if block
          event_id = event_id_or_listener

          raise EventNotRegistered, event_id unless @listeners.key?(event_id)

          @listeners[event_id] << block
        else
          listener = event_id_or_listener

          @listeners.each_key do |reg_event_id|
            next unless listener.respond_to?(@events_methods_map[reg_event_id])

            @listeners[reg_event_id] << listener
          end
        end
      end

      # Allows for code instrumentation
      # Runs the provided code and sends the instrumentation details to all registered listeners
      #
      # @param event_id [String, Symbol] id of the event
      # @param payload [Hash] payload for the instrumentation
      # @param block [Proc] instrumented code
      # @return [Object] whatever the provided block (if any) returns
      #
      # @example Instrument some code
      #   instrument('sleeping') do
      #     sleep(1)
      #   end
      def instrument(event_id, payload = EMPTY_HASH, &block)
        result, time = measure_time_taken(&block) if block_given?

        event = Event.new(
          event_id,
          time ? payload.merge(time: time) : payload
        )

        @listeners[event_id].each do |listener|
          if listener.is_a?(Proc)
            listener.call(event)
          else
            listener.send(@events_methods_map[event_id], event)
          end
        end

        result
      end

      private

      # Measures time taken to execute a given block and returns it together with the result of
      # the block execution
      def measure_time_taken
        start = current_time
        result = yield
        [result, current_time - start]
      end

      # @return [Integer] current monotonic time
      def current_time
        Process.clock_gettime(Process::CLOCK_MONOTONIC, :millisecond)
      end
    end
  end
end
