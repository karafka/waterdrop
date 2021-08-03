# frozen_string_literal: true

module WaterDrop
  # Patches to external components
  module Patches
    # `Rdkafka::Producer` patches
    module RdkafkaProducer
      # Errors upon which we want to retry message production
      # @note Since production happens async, those errors should only occur when using
      #   partition_key, thus only then we handle them
      RETRYABLES = %w[
        leader_not_available
        err_not_leader_for_partition
        invalid_replication_factor
        transport
        timed_out
      ].freeze

      # How many attempts do we want to make before re-raising the error
      MAX_ATTEMPTS = 5

      private_constant :RETRYABLES, :MAX_ATTEMPTS

      # @param args [Object] anything `Rdkafka::Producer#produce` accepts
      #
      # @note This can be removed once this: https://github.com/appsignal/rdkafka-ruby/issues/163
      #   is resolved.
      def produce(**args)
        attempt ||= 0
        attempt += 1

        super
      rescue Rdkafka::RdkafkaError => e
        raise unless args.key?(:partition_key)
        # We care only about specific errors
        # https://docs.confluent.io/platform/current/clients/librdkafka/html/md_INTRODUCTION.html
        raise unless RETRYABLES.any? { |message| e.message.to_s.include?(message) }
        raise if attempt > MAX_ATTEMPTS

        max_sleep = 2**attempt / 10.0
        sleep rand(0.01..max_sleep)

        retry
      end
    end
  end
end

Rdkafka::Producer.prepend(WaterDrop::Patches::RdkafkaProducer)
