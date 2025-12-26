# frozen_string_literal: true

module WaterDrop
  # Namespace for patches that extend external library functionality
  #
  # This module contains monkey patches that add features to dependencies like rdkafka-ruby.
  # Patches here may be extracted to the respective gems in the future.
  module Patches
    # Patch to add queue_size method to Rdkafka::Producer
    #
    # This patch extends the rdkafka producer with the ability to report the number of messages
    # waiting in the librdkafka output queue.
    #
    # @note This patch can be extracted to karafka-rdkafka gem in the future
    module RdkafkaProducer
      # Returns the number of messages and requests waiting to be sent to the broker as well as
      # delivery reports queued for the application.
      #
      # This provides visibility into the producer's internal queue depth, useful for:
      # - Monitoring producer backpressure
      # - Implementing custom flow control
      # - Debugging message delivery issues
      # - Graceful shutdown logic (wait until queue is empty)
      #
      # @return [Integer] the number of messages in the queue
      # @raise [Rdkafka::ClosedProducerError] if called on a closed producer
      #
      # @note This method is thread-safe as it uses the @native_kafka.with_inner synchronization
      #
      # @example
      #   producer.queue_size #=> 42
      def queue_size
        closed_producer_check(__method__)

        @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_outq_len(inner)
        end
      end

      alias queue_length queue_size
    end
  end
end

# Apply the patch
Rdkafka::Producer.prepend(WaterDrop::Patches::RdkafkaProducer)
