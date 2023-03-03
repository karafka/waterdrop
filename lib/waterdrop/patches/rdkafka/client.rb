# frozen_string_literal: true

module WaterDrop
  module Patches
    module Rdkafka
      # Patches for the producer client
      module Client
        # @param _object_id [nil] rdkafka API compatibility argument
        # @param timeout_ms [Integer] final flush timeout in ms
        def close(_object_id = nil, timeout_ms = 5_000)
          return unless @native

          # Indicate to polling thread that we're closing
          @polling_thread[:closing] = true
          # Wait for the polling thread to finish up
          @polling_thread.join

          ::Rdkafka::Bindings.rd_kafka_flush(@native, timeout_ms)
          ::Rdkafka::Bindings.rd_kafka_destroy(@native)

          @native = nil
        end
      end
    end
  end
end

::Rdkafka::Bindings.attach_function(
  :rd_kafka_flush,
  %i[pointer int],
  :void
)

Rdkafka::Producer::Client.prepend WaterDrop::Patches::Rdkafka::Client
