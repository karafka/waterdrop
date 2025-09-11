# frozen_string_literal: true

module WaterDrop
  class Producer
    # Module that provides class-level instrumentation capabilities to producers
    # This allows producers to emit lifecycle events that can be subscribed to at the global level
    module ClassMonitor
      private

      # @return [WaterDrop::Instrumentation::ClassMonitor] global class-level monitor for
      #   instrumentation events
      def class_monitor
        WaterDrop.instrumentation
      end
    end
  end
end
