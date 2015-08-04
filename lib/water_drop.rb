# External components
%w(
  rake
  rubygems
  bundler
  logger
  pathname
  poseidon
  aspector
  forwardable
  connection_pool
).each { |lib| require lib }

# Internal components
%w(
  version
  pool
  config
  event
  null_logger
  aspects/base_aspect
  aspects/formatter
  aspects/after_aspect
  aspects/around_aspect
  aspects/before_aspect
).each { |lib| require "water_drop/#{lib}" }

# WaterDrop library
module WaterDrop
  class << self
    attr_writer :logger

    # @return [Logger] logger that we want to use
    def logger
      @logger ||= NullLogger
    end

    # Sets up the whole configuration
    # @param [Block] block configuration block
    def setup(&block)
      Config.setup(&block)
    end

    # @return [WaterDrop::Config] config instance
    def config
      Config.config
    end
  end
end
