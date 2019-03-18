# frozen_string_literal: true

module WaterDrop
  # Namespace for all the schemas for config validations
  module Schemas
    # Schema with validation rules for WaterDrop configuration details
    Config = Dry::Validation.Schema do
      # Valid uri schemas of Kafka broker url
      URI_SCHEMES ||= %w[kafka kafka+ssl plaintext ssl].freeze

      # Available sasl scram mechanism of authentication (plus nil)
      SASL_SCRAM_MECHANISMS ||= %w[sha256 sha512].freeze

      configure do
        config.messages_file = File.join(
          WaterDrop.gem_root, 'config', 'errors.yml'
        )

        # Uri validator to check if uri is in a Kafka acceptable format
        # @param uri [String] uri we want to validate
        # @return [Boolean] true if it is a valid uri, otherwise false
        def broker_schema?(uri)
          uri = URI.parse(uri)
          URI_SCHEMES.include?(uri.scheme) && uri.port
        rescue URI::InvalidURIError
          false
        end

        # @param object [Object] an object that is suppose to respond to #token method
        # @return [Boolean] true if an object responds to #token method
        def respond_to_token?(object)
          object.respond_to?(:token)
        end
      end

      required(:client_id).filled(:str?, format?: Schemas::TOPIC_REGEXP)
      required(:logger).filled
      required(:deliver).filled(:bool?)
      required(:raise_on_buffer_overflow).filled(:bool?)

      required(:kafka).schema do
        required(:seed_brokers).filled { each(:broker_schema?) }
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
        optional(:sasl_oauth_token_provider).maybe

        # It's not with other encryptions as it has some more rules
        optional(:sasl_scram_mechanism)
          .maybe(:str?, included_in?: WaterDrop::Schemas::SASL_SCRAM_MECHANISMS)

        rule(
          ssl_client_cert_with_ssl_client_cert_key: %i[
            ssl_client_cert
            ssl_client_cert_key
          ]
        ) do |ssl_client_cert, ssl_client_cert_key|
          ssl_client_cert.filled? > ssl_client_cert_key.filled?
        end

        rule(
          ssl_client_cert_key_with_ssl_client_cert: %i[
            ssl_client_cert
            ssl_client_cert_key
          ]
        ) do |ssl_client_cert, ssl_client_cert_key|
          ssl_client_cert_key.filled? > ssl_client_cert.filled?
        end

        rule(
          ssl_client_cert_chain_with_ssl_client_cert: %i[
            ssl_client_cert
            ssl_client_cert_chain
          ]
        ) do |ssl_client_cert, ssl_client_cert_chain|
          ssl_client_cert_chain.filled? > ssl_client_cert.filled?
        end

        rule(
          ssl_client_cert_key_password_with_ssl_client_cert_key: %i[
            ssl_client_cert_key_password
            ssl_client_cert_key
          ]
        ) do |ssl_client_cert_key_password, ssl_client_cert_key|
          ssl_client_cert_key_password.filled? > ssl_client_cert_key.filled?
        end

        rule(
          sasl_oauth_token_provider_respond_to_token: %i[
            sasl_oauth_token_provider
          ]
        ) do |sasl_oauth_token_provider|
          sasl_oauth_token_provider.filled? > sasl_oauth_token_provider.respond_to_token?
        end
      end
    end
  end
end
