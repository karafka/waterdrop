# frozen_string_literal: true

module WaterDrop
  # Namespace for all the things related with WaterDrop instrumentation process
  module Instrumentation
    class << self
      # @return [WaterDrop::]
      def statistics_runners
        @statistics_runners ||= CallbacksRunner.new
      end
    end
  end
end
