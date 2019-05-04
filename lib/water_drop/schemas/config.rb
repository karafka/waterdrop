# frozen_string_literal: true

module WaterDrop
  module Schemas
    # Schema with validation rules for WaterDrop configuration details
    class Config < Dry::Validation::Contract
      # Valid uri schemas of Kafka broker url
      URI_SCHEMES ||= %w[kafka kafka+ssl plaintext ssl].freeze

      # Available sasl scram mechanism of authentication (plus nil)
      SASL_SCRAM_MECHANISMS ||= %w[sha256 sha512].freeze

      AnyObject = Dry::Types::Nominal.new(Object)

      config.messages.load_paths << File.join(WaterDrop.gem_root, 'config', 'errors.yml')

      params do
        required(:client_id).filled(:str?, format?: Schemas::TOPIC_REGEXP)
        required(:logger).filled
        required(:deliver).filled(:bool?)
        required(:raise_on_buffer_overflow).filled(:bool?)

        required(:kafka).schema do
          required(:seed_brokers).filled(:array).each(:str?)
          required(:connect_timeout).filled(:int?, gt?: 0)
          required(:socket_timeout).filled(:int?, gt?: 0)
          required(:compression_threshold).filled(:int?, gteq?: 1)
          optional(:compression_codec).maybe(included_in?: %i[snappy gzip lz4])

          required(:max_buffer_bytesize).filled(:int?, gt?: 0)
          required(:max_buffer_size).filled(:int?, gt?: 0)
          required(:max_queue_size).filled(:int?, gt?: 0)

          required(:ack_timeout).filled(:int?, gt?: 0)
          required(:delivery_interval).filled(:int?, gteq?: 0)
          required(:delivery_threshold).filled(:int?, gteq?: 0)

          required(:max_retries).filled(:int?, gteq?: 0)
          required(:retry_backoff).filled(:int?, gteq?: 0)
          required(:required_acks).filled(included_in?: [1, 0, -1, :all])

          %i[
            ssl_ca_cert
            ssl_ca_cert_file_path
            ssl_client_cert
            ssl_client_cert_key
            ssl_client_cert_chain
            ssl_client_cert_key_password
            sasl_gssapi_principal
            sasl_gssapi_keytab
            sasl_plain_authzid
            sasl_plain_username
            sasl_plain_password
            sasl_scram_username
            sasl_scram_password
          ].each do |encryption_attribute|
            optional(encryption_attribute).maybe(:str?)
          end

          optional(:ssl_ca_certs_from_system).maybe(:bool?)
          optional(:sasl_over_ssl).maybe(:bool?)
          optional(:sasl_oauth_token_provider).value(:any)

          # It's not with other encryptions as it has some more rules
          optional(:sasl_scram_mechanism)
            .maybe(:str?, included_in?: SASL_SCRAM_MECHANISMS)
        end
      end

      rule(kafka: :seed_brokers) do
        values[:kafka][:seed_brokers].each_with_index do |value, idx|
          key([:kafka, :seed_brokers, idx]).failure(:broker_schema) unless broker_schema?(value)
        end
      end

      rule(:kafka, ssl_client_cert_with_ssl_client_cert_key: %i[ssl_client_cert  ssl_client_cert_key]) do
        kafka = values[:kafka]

        if kafka[:ssl_client_cert] && kafka[:ssl_client_cert_key].nil?
          key([:kafka, :ssl_client_cert_key]).failure(:ssl_client_cert_with_ssl_client_cert_key)
        end
      end

      rule(:ssl_client_cert_key_with_ssl_client_cert, :kafka) do
        kafka = values[:kafka]

        if kafka[:ssl_client_cert_key] && kafka[:ssl_client_cert].nil?
          key.failure(:ssl_client_cert_key_with_ssl_client_cert)
        end
      end

      rule(:ssl_client_cert_chain_with_ssl_client_cert, :kafka) do
        kafka = values[:kafka]

        if kafka[:ssl_client_cert_chain] && kafka[:ssl_client_cert].nil?
          key.failure(:ssl_client_cert_chain_with_ssl_client_cert)
        end
      end

      rule(:ssl_client_cert_key_password_with_ssl_client_cert_key, :kafka) do
        kafka = values[:kafka]

        if kafka[:ssl_client_cert_key_password] && kafka[:ssl_client_cert_key].nil?
          key.failure(:ssl_client_cert_key_password_with_ssl_client_cert_key)
        end
      end

      rule(:sasl_oauth_token_provider_respond_to_token, :kafka) do
        kafka = values[:kafka]

        if kafka[:sasl_oauth_token_provider] && !kafka[:sasl_oauth_token_provider].respond_to?(:token)
          key.failure(:sasl_oauth_token_provider_respond_to_token)
        end
      end

      # Uri validator to check if uri is in a Kafka acceptable format
      # @param uri [String] uri we want to validate
      # @return [Boolean] true if it is a valid uri, otherwise false
      def broker_schema?(uri)
        uri = URI.parse(uri)
        URI_SCHEMES.include?(uri.scheme) && uri.port
      rescue URI::InvalidURIError
        false
      end

      def self.call(options)
        new.call(options)
      end
    end
  end
end
