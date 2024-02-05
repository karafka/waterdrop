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
        closing
        closed
      ].freeze

      private_constant :LIFECYCLE

      # Creates a new instance of status with the initial state
      # @return [Status]
      def initialize
        @current = LIFECYCLE.first
      end

      # Configures all the needed references. We need this since we publish events about the
      # producer lifecycle.
      #
      # @param monitor []
      # @param producer_id [String] id of the producer. We pass it to be consistent with the events
      #   payload we already have.
      def setup(monitor, producer_id)
        @monitor = monitor
        @producer_id = producer_id
      end

      # @return [Boolean] true if producer is in a active state. Active means, that we can start
      #   sending messages. Actives states are connected (connection established) or configured,
      #   which means, that producer is configured, but connection with Kafka is
      #   not yet established.
      def active?
        connected? || configured?
      end

      # @return [String] current status as a string
      def to_s
        @current.to_s
      end

      LIFECYCLE.each do |state|
        module_eval <<-RUBY, __FILE__, __LINE__ + 1
          # @return [Boolean] true if current status is as we want, otherwise false
          def #{state}?
            @current == :#{state}
          end

          # Sets a given state as current
          def #{state}!
            # Monitor is available only post-configuration. Prior events will not be published
            if @monitor
              @monitor.instrument("producer.#{state}", producer_id: @producer_id) do
                yield if block_given?
                @current = :#{state}
              end
            else
              @current = :#{state}
            end
          end
        RUBY
      end
    end
  end
end
