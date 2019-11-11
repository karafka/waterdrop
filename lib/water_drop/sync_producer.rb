# frozen_string_literal: true

# WaterDrop library
module WaterDrop
  # Sync producer for messages
  class SyncProducer < BaseProducer
    # Performs message delivery using deliver_async method
    # @param message [String] message that we want to send to Kafka
    # @param options [Hash] options (including topic) for producer
    # @raise [WaterDrop::Errors::InvalidMessageOptions] raised when message options are
    #   somehow invalid and we cannot perform delivery because of that
    def self.call(message, options)
      attempts_count ||= 0
      attempts_count += 1

      validate!(options)
      return unless WaterDrop.config.deliver

      DeliveryBoy.deliver(message, options)
    rescue Kafka::Error => e
      graceful_attempt?(attempts_count, message, options, e) ? retry : raise(e)
    end
  end
end
