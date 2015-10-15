module WaterDrop
  # Configurator for setting up all options required by WaterDrop
  class Config
    class << self
      attr_accessor :config
    end

    # Available options
    # @option connection_pool_size [Fixnum] The number of connections to pool.
    # @option connection_pool_timeout [Fixnum] Amount of time in seconds to wait for a connection
    #         if none currently available.
    # @option kafka_hosts [Array<String>] Array that contains Kafka hosts with ports
    # @option send_messages [Boolean] boolean value to define whether messages should be sent
    # @option raise_on_failure [Boolean] Should raise error when failed to deliver a message
    OPTIONS = %i(
      connection_pool_size
      connection_pool_timeout
      kafka_hosts
      send_messages
      raise_on_failure
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
