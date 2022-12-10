# frozen_string_literal: true

module WaterDrop
  # Patches to external components
  module Patches
    # Rdkafka related patches
    module Rdkafka
      # Rdkafka::Producer patches
      module Producer
        # Cache partitions count for 30 seconds
        PARTITIONS_COUNT_TTL = 30_000

        private_constant :PARTITIONS_COUNT_TTL

        def initialize(*args)
          super

          @partitions_count_cache = Concurrent::Hash.new do |cache, topic|
            cache[topic] = [
              now,
              ::Rdkafka::Metadata.new(inner_kafka, topic).topics&.first[:partition_count]
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
        # @return [Integer] partition count for a given topic
        def partition_count(topic)
          closed_producer_check(__method__)

          @partitions_count_cache.delete_if do |topic, cached|
            now - cached.first > PARTITIONS_COUNT_TTL
          end

          @partitions_count_cache[topic].last
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

        # @return [Float] current clock time
        def now
          ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) * 1_000
        end
      end
    end
  end
end

::Rdkafka::Producer.prepend ::WaterDrop::Patches::Rdkafka::Producer
