# frozen_string_literal: true

# External components
%w[
  json
  delivery_boy
  null_logger
  singleton
  dry-configurable
  dry/monitor/notifications
  dry-validation
].each { |lib| require lib }

# Internal components
base_path = File.dirname(__FILE__) + '/water_drop'

# WaterDrop library
module WaterDrop
  class << self
    attr_accessor :logger

    # Sets up the whole configuration
    # @param [Block] block configuration block
    def setup(&block)
      Config.setup(&block)
      DeliveryBoy.logger = self.logger = config.logger
      ConfigApplier.call(DeliveryBoy.config,Config.config.to_h)
    end

    # @return [WaterDrop::Config] config instance
    def config
      Config.config
    end

    # @return [::WaterDrop::Monitor] monitor that we want to use
    def monitor
      config.monitor
    end

    # @return [String] root path of this gem
    def gem_root
      Pathname.new(File.expand_path('..', __dir__))
    end
  end
end

%w[
  version
  instrumentation/monitor
  instrumentation/listener
  schemas/message_options
  schemas/config
  config
  config_applier
  errors
  base_producer
  sync_producer
  async_producer
].each { |lib| require "#{base_path}/#{lib}" }
