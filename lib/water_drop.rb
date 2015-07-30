# External components
%w(
  rake
  rubygems
  bundler
  pathname
  poseidon
  aspector
  connection_pool
).each { |lib| require lib }

# Internal components
%w(
  version
  pool
  config
  event
  aspects/base_aspect
  aspects/formatter
  aspects/after_aspect
  aspects/around_aspect
  aspects/before_aspect
).each { |lib| require "water_drop/#{lib}" }

# WaterDrop library
module WaterDrop
  # @param [Logger] logger that we want to use
  # @return [Logger] assigned logger
  # Assignes new logger
  # @note We don't have any logger by default
  def self.logger=(logger)
    @logger = logger
  end

  # @return [Logger] logger that we want to use
  def self.logger
    @logger
  end

  # Sets up the whole configuration
  # @param [Block] configuration block
  def self.setup(&block)
    Config.setup(&block)
  end

  # @return [WaterDrop::Config] config instance
  def self.config
    Config.config
  end
end

WaterDrop.setup do |config|
  config.connection_pool_size = 20
  config.connection_pool_timeout = 1
  config.kafka_ports = %w( 9092 )
  config.kafka_host = '172.17.0.7'
  config.send_events = true
end

WaterDrop::Event.new('topic', 'message').send!
