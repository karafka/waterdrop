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
            topic_metadata = ::Rdkafka::Metadata.new(inner_kafka, topic).topics&.first

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
          @_name ||= ::Rdkafka::Bindings.rd_kafka_name(inner_kafka)
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

        # @return [FFI::Pointer] pointer to the raw librdkafka
        def inner_kafka
          unless @_inner_kafka
            version = ::Gem::Version.new(::Rdkafka::VERSION)

            if version < ::Gem::Version.new('0.12.0')
              @_inner_kafka = @native_kafka
            elsif version < ::Gem::Version.new('0.13.0.beta.1')
              @_inner_kafka = @client.native
            else
              @_inner_kafka = @native_kafka.inner
            end
          end

          @_inner_kafka
        end
      end
    end
  end
end

::Rdkafka::Producer.prepend ::WaterDrop::Patches::Rdkafka::Producer
