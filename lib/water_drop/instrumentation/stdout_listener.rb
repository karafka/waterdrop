# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    # Default listener that hooks up to our instrumentation and uses its events for logging
    # It can be removed/replaced or anything without any harm to the Waterdrop flow
    # @note It is a module as we can use it then as a part of the Karafka framework listener
    #   as well as we can use it standalone
    class StdoutListener
      # Log levels that we use in this particular listener
      USED_LOG_LEVELS = %i[
        info
        error
      ].freeze

      %i[
        sync_producer
        async_producer
      ].each do |producer_type|
        error_name = :"on_#{producer_type}_call_error"
        retry_name = :"on_#{producer_type}_call_retry"

        define_method error_name do |event|
          options = event[:options]
          error = event[:error]
          error "Delivery failure to: #{options} because of #{error}"
        end

        define_method retry_name do |event|
          attempts_count = event[:attempts_count]
          options = event[:options]
          error = event[:error]

          info "Attempt #{attempts_count} of delivery to: #{options} because of #{error}"
        end
      end

      USED_LOG_LEVELS.each do |log_level|
        define_method log_level do |*args|
          WaterDrop.logger.send(log_level, *args)
        end
      end
    end
  end
end
