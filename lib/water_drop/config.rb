# frozen_string_literal: true

module WaterDrop
  # Configurator for setting up all options required by WaterDrop
  class Config
    extend Dry::Configurable

    # option client_id [String] identifier of this producer
    setting :client_id, 'waterdrop'
    # option logger [Instance, nil] logger that we want to use or nil to
    #   fallback to ruby-kafka logger
    setting :logger, nil
    # Available options
    setting :send_messages
    # @option raise_on_failure [Boolean] Should raise error when failed to deliver a message
    setting :raise_on_failure
    # @option required_acks [:all, 0, 1] acknowledgement level ()
    setting :required_acks, :all

    # Connection pool options
    setting :connection_pool do
      # Connection pool size for producers. Note that we take a bigger number because there
      # are cases when we might have more sidekiq threads than Karafka consumers (small app)
      # or the opposite for bigger systems
      setting :size, 2
      # How long should we wait for a working resource from the pool before rising timeout
      # With a proper connection pool size, this should never happen
      setting :timeout, 5
    end

    # option kafka [Hash] - optional - kafka configuration options (hosts)
    setting :kafka do
      # @option seed_brokers [Array<String>] Array that contains Kafka seed broker hosts with ports
      setting :seed_brokers
      # option connect_timeout [Integer] Sets the number of seconds to wait while connecting to
      # a broker for the first time. When ruby-kafka initializes, it needs to connect to at
      # least one host.
      setting :connect_timeout, 10
      # option socket_timeout [Integer] Sets the number of seconds to wait when reading from or
      # writing to a socket connection to a broker. After this timeout expires the connection
      # will be killed. Note that some Kafka operations are by definition long-running, such as
      # waiting for new messages to arrive in a partition, so don't set this value too low
      setting :socket_timeout, 10
      # SSL authentication related settings
      # option ca_cert [String] SSL CA certificate
      setting :ssl_ca_cert, nil
      # option ssl_ca_cert_file_path [String] SSL CA certificate file path
      setting :ssl_ca_cert_file_path, nil
      # option client_cert [String] SSL client certificate
      setting :ssl_client_cert, nil
      # option client_cert_key [String] SSL client certificate password
      setting :ssl_client_cert_key, nil
      # option sasl_gssapi_principal [String] sasl principal
      setting :sasl_gssapi_principal, nil
      # option sasl_gssapi_keytab [String] sasl keytab
      setting :sasl_gssapi_keytab, nil
      # option sasl_plain_authzid [String] The authorization identity to use
      setting :sasl_plain_authzid, ''
      # option sasl_plain_username [String] The username used to authenticate
      setting :sasl_plain_username, nil
      # option sasl_plain_password [String] The password used to authenticate
      setting :sasl_plain_password, nil
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
