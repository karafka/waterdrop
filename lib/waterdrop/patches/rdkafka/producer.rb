# frozen_string_literal: true

module WaterDrop
  # Patches to external components
  module Patches
    # Rdkafka related patches
    module Rdkafka
      # Rdkafka::Producer patches
      module Producer
        include ::Karafka::Core::Helpers::Time

        # Cache partitions count for 30 seconds
        PARTITIONS_COUNT_TTL = 30_000

        private_constant :PARTITIONS_COUNT_TTL

        # @param args [Object] arguments accepted by the original rdkafka producer
        def initialize(*args)
          super

          @_partitions_count_cache = Concurrent::Hash.new do |cache, topic|
            topic_metadata = nil

            @native_kafka.with_inner do |inner|
              topic_metadata = ::Rdkafka::Metadata.new(inner, topic).topics&.first
            end

            cache[topic] = [
              monotonic_now,
              topic_metadata ? topic_metadata[:partition_count] : nil
            ]
          end
        end

        # Adds a method that allows us to get the native kafka producer name
        #
        # In between rdkafka versions, there are internal changes that force us to add some extra
        # magic to support all the versions.
        #
        # @return [String] producer instance name
        def name
          @_name ||= @native_kafka.with_inner do |inner|
            ::Rdkafka::Bindings.rd_kafka_name(inner)
          end
        end

        # This patch makes sure we cache the partition count for a given topic for given time
        # This prevents us in case someone uses `partition_key` from querying for the count with
        # each message. Instead we query once every 30 seconds at most
        #
        # @param topic [String] topic name
        # @return [Integer] partition count for a given topic
        def partition_count(topic)
          closed_producer_check(__method__)

          @_partitions_count_cache.delete_if do |_, cached|
            monotonic_now - cached.first > PARTITIONS_COUNT_TTL
          end

          @_partitions_count_cache[topic].last
        end
      end
    end
  end
end

::Rdkafka::Producer.prepend ::WaterDrop::Patches::Rdkafka::Producer
