# frozen_string_literal: true

module WaterDrop
  module Clients
    # A dummy client that is supposed to be used instead of Rdkafka::Producer in case we don't
    # want to dispatch anything to Kafka.
    #
    # It does not store anything and just ignores messages. It does however return proper delivery
    # handle that can be materialized into a report.
    class Dummy
      # `::Rdkafka::Producer::DeliveryHandle` object API compatible dummy object
      class Handle < ::Rdkafka::Producer::DeliveryHandle
        # @param topic [String] topic where we want to dispatch message
        # @param partition [Integer] target partition
        # @param offset [Integer] offset assigned by our fake "Kafka"
        def initialize(topic, partition, offset)
          @topic = topic
          @partition = partition
          @offset = offset
        end

        # Does not wait, just creates the result
        #
        # @param _args [Array] anything the wait handle would accept
        # @return [::Rdkafka::Producer::DeliveryReport]
        def wait(*_args)
          create_result
        end

        # Creates a delivery report with details where the message went
        #
        # @return [::Rdkafka::Producer::DeliveryReport]
        def create_result
          ::Rdkafka::Producer::DeliveryReport.new(
            @partition,
            @offset,
            @topic
          )
        end
      end

      # @param _producer [WaterDrop::Producer]
      # @return [Dummy] dummy instance
      def initialize(_producer)
        @counters = Hash.new { |h, k| h[k] = -1 }
      end

      # "Produces" the message
      # @param topic [String, Symbol] topic where we want to dispatch message
      # @param partition [Integer] target partition
      # @param _args [Hash] remaining details that are ignored in the dummy mode
      # @return [Handle] delivery handle
      def produce(topic:, partition: 0, **_args)
        Handle.new(topic.to_s, partition, @counters["#{topic}#{partition}"] += 1)
      end

      # @param _args [Object] anything really, this dummy is suppose to support anything
      def respond_to_missing?(*_args)
        true
      end

      # @param _args [Object] anything really, this dummy is suppose to support anything
      # @return [self] returns self for chaining cases
      def method_missing(*_args)
        self || super
      end
    end
  end
end
