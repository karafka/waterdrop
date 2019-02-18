# frozen_string_literal: true

module WaterDrop
  # Engine used to propagate config application to DeliveryBoy with corner case handling
  module ConfigApplier
    class << self
      # @param delivery_boy_config [DeliveryBoy::Config] delivery boy config instance
      # @param settings [Hash] hash with WaterDrop settings
      def call(delivery_boy_config, settings)
        # Recursive lambda for mapping config down to delivery boy
        settings.each do |key, value|
          call(delivery_boy_config, value) && next if value.is_a?(Hash)

          # If this is a special case that needs manual setup instead of a direct reassignment
          if respond_to?(key, true)
            send(key, delivery_boy_config, value)
          else
            # If this setting is our internal one, we should not sync it with the delivery boy
            next unless delivery_boy_config.respond_to?(:"#{key}=")

            delivery_boy_config.public_send(:"#{key}=", value)
          end
        end
      end

      private

      # Extra setup for the compression codec as it behaves differently than other settings
      # that are ported 1:1 from ruby-kafka
      # For some crazy reason, delivery boy requires compression codec as a string, when
      # ruby-kafka as a symbol. We follow ruby-kafka internal design, so we had to mimic
      # that by assigning a stringified version that down the road will be symbolized again
      # by delivery boy
      # @param delivery_boy_config [DeliveryBoy::Config] delivery boy config instance
      # @param codec_name [Symbol] codec name as a symbol
      def compression_codec(delivery_boy_config, codec_name)
        # If there is no compression codec, we don't apply anything
        return unless codec_name

        delivery_boy_config.compression_codec = codec_name.to_s
      end

      # We use the "seed_brokers" name and DeliveryBoy uses "brokers" so we pass the values
      #   manually
      # @param delivery_boy_config [DeliveryBoy::Config] delivery boy config instance
      # @param seed_brokers [Array<String>] kafka seed brokers
      def seed_brokers(delivery_boy_config, seed_brokers)
        delivery_boy_config.brokers = seed_brokers
      end
    end
  end
end
