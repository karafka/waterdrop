# frozen_string_literal: true

# WaterDrop main module
module WaterDrop
  # Namespace for FD-based polling components
  module Polling
    # Configuration for the global FD poller singleton
    # These settings apply to all producers using FD polling mode
    #
    # @example Configure before creating any producers
    #   WaterDrop::Polling::Config.setup do |config|
    #     config.thread_priority = -1
    #     config.poll_timeout = 500
    #   end
    class Config
      extend ::Karafka::Core::Configurable

      # Ruby thread priority for the poller thread
      # Valid range: -3 to 3 (Ruby's thread priority range)
      # Higher values = higher priority
      setting :thread_priority, default: 0

      # IO.select timeout in milliseconds
      # Controls how often periodic polling happens when no FD events occur
      # Lower values = more responsive OAuth/stats callbacks but higher CPU
      setting :poll_timeout, default: 1_000

      # Initial backoff delay in milliseconds after a polling error
      setting :backoff_min, default: 100

      # Maximum backoff delay in milliseconds after repeated errors
      # Backoff doubles on each consecutive error up to this limit
      setting :backoff_max, default: 30_000

      class << self
        # Configures the poller settings
        # @yield [config] Configuration block
        # @yieldparam config [Karafka::Core::Configurable::Node] config node
        def setup
          configure do |config|
            yield(config)
          end

          Contracts::PollerConfig.new.validate!(
            self.config.to_h,
            Errors::ConfigurationInvalidError
          )
        end
      end
    end
  end
end
