# frozen_string_literal: true

# External components
%w[
  json
  delivery_boy
  null_logger
  dry-configurable
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

      # Recursive lambda for mapping config down to delivery boy
      applier = lambda { |db_config, h|
        h.each do |k, v|
          applier.call(db_config, v) && next if v.is_a?(Hash)
          next unless db_config.respond_to?(:"#{k}=")
          db_config.public_send(:"#{k}=", v)
        end
      }

      DeliveryBoy.config.tap do |config|
        config.brokers = Config.config.kafka.seed_brokers
        applier.call(config, Config.config.to_h)
      end
    end

    # @return [WaterDrop::Config] config instance
    def config
      Config.config
    end

    # @return [String] root path of this gem
    def gem_root
      Pathname.new(File.expand_path('../..', __FILE__))
    end
  end
end

%w[
  version
  schemas/message_options
  schemas/config
  config
  errors
  base_producer
  sync_producer
  async_producer
].each { |lib| require "#{base_path}/#{lib}" }
