# frozen_string_literal: true

module WaterDrop
  # Patches to external components
  module Patches
    # Rdkafka related patches
    module Rdkafka
      # Rdkafka::Metadata patches
      module Metadata
        # We overwrite this method because there were reports of metadata operation timing out
        # when Kafka was under stress. While the messages dispatch will be retried, this was not
        # and was causing problems.
        #
        # @param args [Array<Object>] all the metadata original arguments
        def initialize(*args)
          attempt ||= 0
          attempt += 1

          super(*args)
        rescue Rdkafka::RdkafkaError => e
          raise unless e.code == :timed_out
          raise if attempt > 10

          backoff_factor = 2**attempt
          timeout = backoff_factor * 0.1

          sleep(attempt)

          retry
        end
      end
    end
  end
end

::Rdkafka::Metadata.prepend ::WaterDrop::Patches::Rdkafka::Metadata
