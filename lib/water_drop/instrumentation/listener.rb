# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    # Default listener that hooks up to our instrumentation and uses its events for logging
    # It can be removed/replaced or anything without any harm to the Waterdrop flow
    # @note It is a module as we can use it then as a part of the Karafka framework listener
    #   as well as we can use it standalone
    module Listener
      # Log levels that we use in this particular listener
      USED_LOG_LEVELS = %i[
        info
        error
      ].freeze

      %i[
        sync_producer
        async_producer
      ].each do |producer_type|
        define_method :"on_#{producer_type}_call_error" do |event|
          options = event[:options]
          error = event[:error]
          attempts_count = event[:attempts_count]

          error "Delivery failure to: #{options} because of #{error}"
        end

        define_method :"on_#{producer_type}_call_retry" do |event|
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

      extend self

      USED_LOG_LEVELS.each(&method(:private_class_method))
    end
  end
end
