# frozen_string_literal: true

module WaterDrop
  class Producer
    # Producer lifecycle status object representation
    class Status
      # States in which the producer can be
      LIFECYCLE = %i[
        initial
        configured
        connected
        disconnecting
        disconnected
        closing
        closed
      ].freeze

      private_constant :LIFECYCLE

      # States in which the producer is considered active and able to accept work. Kept as a single
      # set so the current state can be classified in one atomic read (see `#active?` / `#to_sym`)
      # rather than via a chain of predicate calls that could straddle a concurrent transition.
      ACTIVE_STATES = %i[
        connected
        configured
        disconnecting
        disconnected
      ].freeze

      # Creates a new instance of status with the initial state
      # @return [Status]
      def initialize
        @current = LIFECYCLE.first
      end

      # @return [Boolean] true if producer is in a active state. Active means, that we can start
      #   sending messages. Active states are connected (connection established), configured
      #   which means, that producer is configured, but connection with Kafka is not yet
      #   established or disconnected, meaning it was working but user disconnected for his own
      #   reasons though sending could reconnect and continue.
      def active?
        # Single read of @current so a concurrent transition cannot make this return false for a
        # status that is in fact active (for example flipping configured -> connected mid-check
        # while another thread reloads the client after a fatal error).
        ACTIVE_STATES.include?(@current)
      end

      # @return [String] current status as a string
      def to_s
        @current.to_s
      end

      # @return [Symbol] current lifecycle state captured as a single atomic read. Lets callers
      #   branch on one consistent value instead of issuing several predicate calls that could
      #   observe different states if the producer is transitioning on another thread.
      def to_sym
        @current
      end

      LIFECYCLE.each do |state|
        # @example
        #   def initial?
        #     @current == :initial
        #   end
        #
        #   def initial!
        #     @current = :initial
        #   end
        module_eval <<-RUBY, __FILE__, __LINE__ + 1
          # @return [Boolean] true if current status is as we want, otherwise false
          def #{state}?
            @current == :#{state}
          end

          # Sets a given state as current
          def #{state}!
            @current = :#{state}
          end
        RUBY
      end
    end
  end
end
