# frozen_string_literal: true

module WaterDrop
  module Schemas
    # Schema with validation rules for WaterDrop configuration details
    Config = Dry::Validation.Schema do
      # Valid uri schemas of Kafka broker url
      URI_SCHEMES = %w[kafka kafka+ssl].freeze

      configure do
        # Uri validator to check if uri is in a Kafka acceptable format
        # @param uri [String] uri we want to validate
        # @return [Boolean] true if it is a valid uri, otherwise false
        def broker_schema?(uri)
          uri = URI.parse(uri)
          URI_SCHEMES.include?(uri.scheme) && uri.port
        rescue URI::InvalidURIError
          return false
        end
      end

      required(:client_id).filled(:str?, format?: Schemas::TOPIC_REGEXP)
      required(:logger)
      required(:send_messages).filled(:bool?)

      required(:kafka).schema do
        required(:seed_brokers).filled { each(:broker_schema?) }
        required(:connect_timeout).filled { (int? | float?) & gt?(0) }
        required(:socket_timeout).filled { (int? | float?) & gt?(0) }
        required(:compression_threshold).filled(:int?, gteq?: 1)
        optional(:compression_codec).maybe(included_in?: %i[snappy gzip])

        required(:max_buffer_bytesize).filled(:int?, gt?: 0)
        required(:max_buffer_size).filled(:int?, gt?: 0)
        required(:max_queue_size).filled(:int?, gt?: 0)

        required(:ack_timeout).filled(:int?, gt?: 0)
        required(:delivery_interval).filled(:int?, gteq?: 0)
        required(:delivery_threshold).filled(:int?, gteq?: 0)

        required(:max_retries).filled(:int?, gteq?: 0)
        required(:required_acks).filled(included_in?: [1, 0, -1, :all])
        required(:retry_backoff).filled(:int?, gteq?: 0)

        %i[
          ssl_ca_cert
          ssl_ca_cert_file_path
          ssl_client_cert
          ssl_client_cert_key
          sasl_plain_authzid
          sasl_plain_username
          sasl_plain_password
          sasl_gssapi_principal
          sasl_gssapi_keytab
        ].each do |encryption_attribute|
          optional(encryption_attribute).maybe(:str?)
        end
      end
    end
  end
end
