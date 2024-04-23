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
        # @param error [Rdkafka::Error] error that occurred
        # @note It will only instrument on errors of the client of our producer
        # @note When there is a particular message produce error (not internal error), the error
        #   is shipped via the delivery callback, not via error callback.
        def call(client_name, error)
          # Emit only errors related to our client
          # Same as with statistics (mor explanation there)
          return unless @client_name == client_name

          @monitor.instrument(
            'error.occurred',
            caller: self,
            error: error,
            producer_id: @producer_id,
            type: 'librdkafka.error'
          )
        # This runs from the rdkafka thread, thus we want to safe-guard it and prevent absolute
        # crashes even if the instrumentation code fails. If it would bubble-up, it could crash
        # the rdkafka background thread
        rescue StandardError => e
          @monitor.instrument(
            'error.occurred',
            caller: self,
            error: e,
            producer_id: @producer_id,
            type: 'callbacks.error.error'
          )
        end
      end
    end
  end
end
