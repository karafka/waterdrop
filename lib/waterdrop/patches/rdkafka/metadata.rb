# frozen_string_literal: true

module WaterDrop
  # Patches to external components
  module Patches
    # Rdkafka related patches
    module Rdkafka
      # Rdkafka::Metadata patches
      module Metadata
        # Errors upon which we retry the metadata fetch
        RETRIED_ERRORS = %i[
          timed_out
          leader_not_available
        ].freeze

        private_constant :RETRIED_ERRORS

        # We overwrite this method because there were reports of metadata operation timing out
        # when Kafka was under stress. While the messages dispatch will be retried, metadata
        # fetch happens prior to that, effectively crashing the process. Metadata fetch was not
        # being retried at all.
        #
        # @param args [Array<Object>] all the metadata original arguments
        def initialize(*args)
          attempt ||= 0
          attempt += 1

          super(*args)
        rescue ::Rdkafka::RdkafkaError => e
          raise unless RETRIED_ERRORS.include?(e.code)
          raise if attempt > 10

          backoff_factor = 2**attempt
          timeout = backoff_factor * 0.1

          sleep(timeout)

          retry
        end
      end
    end
  end
end

::Rdkafka::Metadata.prepend ::WaterDrop::Patches::Rdkafka::Metadata
