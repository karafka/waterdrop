# frozen_string_literal: true

# WaterDrop main module
module WaterDrop
  # Namespace for FD-based polling components
  # Contains the global Poller singleton and State class for managing producer polling
  module Polling
    class << self
      # Configures the global FD poller settings
      # @param block [Proc] Configuration block
      # @yieldparam config [Karafka::Core::Configurable::Node] config node
      # @example Configure before creating any producers
      #   WaterDrop::Polling.setup do |config|
      #     config.thread_priority = -1
      #     config.poll_timeout = 500
      #   end
      def setup(&block)
        Config.setup(&block)
      end
    end
  end
end
