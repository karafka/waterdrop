module WaterDrop
  # Configurator for setting up all options required by WaterDrop
  class Config
    class << self
      attr_accessor :config
    end

    # Available config options
    OPTIONS = %i(
      connection_pool_size
      connection_pool_timeout
      kafka_ports
      kafka_host
      send_events
    )

    OPTIONS.each do |attr_name|
      attr_accessor attr_name

      # @return [Boolean] is given command enabled
      define_method :"#{attr_name}?" do
        public_send(attr_name) == true
      end
    end

    # Configurating method
    def self.setup(&block)
      self.config = new

      block.call(config)
      config.freeze
    end
  end
end
