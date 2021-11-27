# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    module Callbacks
      # Callback that kicks in when error occurs and is published in a background thread
      class Error
        # @param producer_id [String] id of the current producer
        # @param client_name [String] rdkafka client name
        # @param monitor [WaterDrop::Instrumentation::Monitor] monitor we are using
        def initialize(producer_id, client_name, monitor)
          @producer_id = producer_id
          @client_name = client_name
          @monitor = monitor
        end

        # Runs the instrumentation monitor with error
        # @param client_name [String] rdkafka client name
        # @para error [Rdkafka::Error] error that occurred
        # @note If will only instrument on errors of the client of our producer
        def call(client_name, error)
          # Emit only errors related to our client
          # Same as with statistics (mor explanation there)
          return unless @client_name == client_name

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
