# frozen_string_literal: true

# External components
%w[
  json
  delivery_boy
  singleton
  dry-configurable
  dry/monitor/notifications
  dry-validation
  zeitwerk
].each { |lib| require lib }

# WaterDrop library
module WaterDrop
  class << self
    attr_accessor :logger

    # Sets up the whole configuration
    # @param [Block] block configuration block
    def setup(&block)
      Config.setup(&block)
      DeliveryBoy.logger = self.logger = config.logger
      ConfigApplier.call(DeliveryBoy.config, Config.config.to_h)
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

Zeitwerk::Loader
  .for_gem
  .tap { |loader| loader.ignore("#{__dir__}/waterdrop.rb") }
  .tap(&:setup)
  .tap(&:eager_load)
