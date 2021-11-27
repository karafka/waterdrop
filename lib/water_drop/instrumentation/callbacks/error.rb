# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    module Callbacks
      class Error
        def initialize(producer_id, client_name, monitor)
          @producer_id = producer_id
          @client_name = client_name
          @monitor = monitor
        end

        def call(client_name, error)
          # Emit only statistics related to our client
          # rdkafka does not have per-instance statistics hook, thus we need to make sure that we
          # emit only stats that are related to current producer. Otherwise we would emit all of
          # all the time.
          return unless @client_name == client_name

          p error

          @monitor.instrument(
            'error.emitted',
            producer_id: @producer_id,
            error: error
          )
        end
      end
    end
  end
end
