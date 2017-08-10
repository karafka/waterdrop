# frozen_string_literal: true

module WaterDrop
  # Configurator for setting up all options required by WaterDrop
  class Config
    extend Dry::Configurable

    # Available options
    # @option connection_pool_timeout [Fixnum] Amount of time in seconds to wait for a connection
    #         if none currently available.
    setting :connection_pool_timeout
    # @option send_messages [Boolean] boolean value to define whether messages should be sent
    setting :send_messages
    # @option raise_on_failure [Boolean] Should raise error when failed to deliver a message
    setting :raise_on_failure
    # @option connection_pool_size [Fixnum] The number of connections to pool.
    setting :connection_pool_size
    # option kafka [Hash] - optional - kafka configuration options (hosts)
    setting :kafka do
      # @option hosts [Array<String>] Array that contains Kafka hosts with ports
      setting :hosts
      # SSL authentication related settings
      setting :ssl do
        # option ca_cert [String] SSL CA certificate
        setting :ca_cert, nil
        # option ssl_ca_cert_file_path [String] SSL CA certificate
        setting :ca_cert_file_path, nil
        # option client_cert [String] SSL client certificate
        setting :client_cert, nil
        # option client_cert_key [String] SSL client certificate password
        setting :client_cert_key, nil
      end
    end

    class << self
      # Configurating method
      # @yield Runs a block of code providing a config singleton instance to it
      # @yieldparam [WaterDrop::Config] WaterDrop config instance
      def setup
        configure do |config|
          yield(config)
        end
      end
    end
  end
end
