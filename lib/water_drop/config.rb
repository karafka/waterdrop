# frozen_string_literal: true

# Configuration and descriptions are based on the delivery boy zendesk gem
# @see https://github.com/zendesk/delivery_boy
module WaterDrop
  # Configurator for setting up all options required by WaterDrop
  class Config
    extend Dry::Configurable

    # WaterDrop options
    # option client_id [String] identifier of this producer
    setting :client_id, 'waterdrop'
    # option [Instance, nil] logger that we want to use or nil to fallback to ruby-kafka logger
    setting :logger, NullLogger.new
    # option [Boolean] should we send messages. Setting this to false can be really useful when
    #   testing and or developing because when set to false, won't actually ping Kafka
    setting :deliver, true
    # option [Boolean] if you're producing messages faster than the framework or the network can
    #   send them off, ruby-kafka might reject them. If that happens, WaterDrop will either raise
    #   or ignore - this setting manages that behavior. This only applies to async producer as
    #   sync producer will always raise upon problems
    setting :raise_on_buffer_overflow, true

    # Settings directly related to the Kafka driver
    setting :kafka do
      # option [Array<String>] Array that contains Kafka seed broker hosts with ports
      setting :seed_brokers

      # Network timeouts
      # option connect_timeout [Integer] Sets the number of seconds to wait while connecting to
      # a broker for the first time. When ruby-kafka initializes, it needs to connect to at
      # least one host.
      setting :connect_timeout, 10
      # option socket_timeout [Integer] Sets the number of seconds to wait when reading from or
      # writing to a socket connection to a broker. After this timeout expires the connection
      # will be killed. Note that some Kafka operations are by definition long-running, such as
      # waiting for new messages to arrive in a partition, so don't set this value too low
      setting :socket_timeout, 30

      # Buffering for async producer
      # @option [Integer] The maximum number of bytes allowed in the buffer before new messages
      #   are rejected.
      setting :max_buffer_bytesize, 10_000_000
      # @option [Integer] The maximum number of messages allowed in the buffer before new messages
      #   are rejected.
      setting :max_buffer_size, 1000
      # @option [Integer] The maximum number of messages allowed in the queue before new messages
      #   are rejected. The queue is used to ferry messages from the foreground threads of your
      #   application to the background thread that buffers and delivers messages.
      setting :max_queue_size, 1000

      # option [Integer] A timeout executed by a broker when the client is sending messages to it.
      #   It defines the number of seconds the broker should wait for replicas to acknowledge the
      #   write before responding to the client with an error. As such, it relates to the
      #   required_acks setting. It should be set lower than socket_timeout.
      setting :ack_timeout, 5
      # option [Integer] The number of seconds between background message
      #   deliveries. Default is 10 seconds. Disable timer-based background deliveries by
      #   setting this to 0.
      setting :delivery_interval, 10
      # option [Integer] The number of buffered messages that will trigger a background message
      #   delivery. Default is 100 messages. Disable buffer size based background deliveries by
      #   setting this to 0.
      setting :delivery_threshold, 100

      # option [Integer] The number of retries when attempting to deliver messages.
      setting :max_retries, 2
      # option [Integer]
      setting :required_acks, -1
      # option [Integer]
      setting :retry_backoff, 1

      # option [Integer] The minimum number of messages that must be buffered before compression is
      #   attempted. By default only one message is required. Only relevant if compression_codec
      #   is set.
      setting :compression_threshold, 1
      # option [Symbol] The codec used to compress messages. Must be either snappy or gzip.
      setting :compression_codec, nil

      # SSL authentication related settings
      # option ca_cert [String, nil] SSL CA certificate
      setting :ssl_ca_cert, nil
      # option ssl_ca_cert_file_path [String, nil] SSL CA certificate file path
      setting :ssl_ca_cert_file_path, nil
      # option ssl_ca_certs_from_system [Boolean] Use the CA certs from your system's default
      #   certificate store
      setting :ssl_ca_certs_from_system, false
      # option ssl_client_cert [String, nil] SSL client certificate
      setting :ssl_client_cert, nil
      # option ssl_client_cert_key [String, nil] SSL client certificate password
      setting :ssl_client_cert_key, nil
      # option sasl_gssapi_principal [String, nil] sasl principal
      setting :sasl_gssapi_principal, nil
      # option sasl_gssapi_keytab [String, nil] sasl keytab
      setting :sasl_gssapi_keytab, nil
      # option sasl_plain_authzid [String] The authorization identity to use
      setting :sasl_plain_authzid, ''
      # option sasl_plain_username [String, nil] The username used to authenticate
      setting :sasl_plain_username, nil
      # option sasl_plain_password [String, nil] The password used to authenticate
      setting :sasl_plain_password, nil
      # option sasl_scram_username [String, nil] The username used to authenticate
      setting :sasl_scram_username, nil
      # option sasl_scram_password [String, nil] The password used to authenticate
      setting :sasl_scram_password, nil
      # option sasl_scram_mechanism [String, nil] Scram mechanism, either 'sha256' or 'sha512'
      setting :sasl_scram_mechanism, nil
    end

    # option producer [Hash] - optional - producer configuration options
    setting :producer do
      # option compression_codec [Symbol] Sets producer compression codec
      # More: https://github.com/zendesk/ruby-kafka#compression
      # More: https://github.com/zendesk/ruby-kafka/blob/master/lib/kafka/compression.rb
      setting :compression_codec, nil
      # option compression_codec [Integer] Sets producer compression threshold
      # More: https://github.com/zendesk/ruby-kafka#compression
      setting :compression_threshold, 1
    end

    class << self
      # Configurating method
      # @yield Runs a block of code providing a config singleton instance to it
      # @yieldparam [WaterDrop::Config] WaterDrop config instance
      def setup
        configure do |config|
          yield(config)
          validate!(config.to_h)
        end
      end

      private

      # Validates the configuration and if anything is wrong, will raise an exception
      # @param config_hash [Hash] config hash with setup details
      # @raise [WaterDrop::Errors::InvalidConfiguration] raised when something is wrong with
      #   the configuration
      def validate!(config_hash)
        validation_result = Schemas::Config.call(config_hash)
        return true if validation_result.success?
        raise Errors::InvalidConfiguration, validation_result.errors
      end
    end
  end
end
