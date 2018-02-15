# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    # Default listener that hooks up to our instrumentation and uses its events for logging
    # It can be removed/replaced or anything without any harm to the Waterdrop flow
    class Listener
      class << self
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

        private

        %i[
          debug
          info
          error
          fatal
        ].each do |log_method|
          define_method log_method do |*args|
            WaterDrop.logger.send(log_method, *args)
          end
        end
      end
    end
  end
end
