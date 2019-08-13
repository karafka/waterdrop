# frozen_string_literal: true

module WaterDrop
  module Patches
    module RdkafkaDeliveryHandle
      CURRENT_TIME = -> { Process.clock_gettime(Process::CLOCK_MONOTONIC) }.freeze

      private_constant :CURRENT_TIME

      # Wait for the delivery report or raise an error if this takes longer than the timeout.
      # If there is a timeout this does not mean the message is not delivered, rdkafka might
      #   still be working on delivering the message. In this case it is possible to call wait
      #   again.
      #
      # @param max_wait_timeout [Numeric, nil] Amount of time to wait before timing out.
      #   If this is nil it does not time out.
      # @param wait_timeout [Numeric] Amount of time we should wait before we recheck if there
      #   is a delivery report available
      #
      # @raise [RdkafkaError] When delivering the message failed
      # @raise [WaitTimeoutError] When the timeout has been reached and the handle is still pending
      #
      # @return [DeliveryReport]
      def wait(max_wait_timeout = 60, wait_timeout = 0.1)
        timeout = max_wait_timeout ? CURRENT_TIME.call + max_wait_timeout : nil

        loop do
          if pending?
            if timeout && timeout <= CURRENT_TIME.call
              raise Rdkafka::Producer::DeliveryHandle::WaitTimeoutError.new(
                "Waiting for delivery timed out after #{max_wait_timeout} seconds"
              )
            end
            sleep wait_timeout
          elsif self[:response] != 0
            raise RdkafkaError.new(self[:response])
          else
            return Rdkafka::Producer::DeliveryReport.new(self[:partition], self[:offset])
          end
        end
      end
    end
  end
end

Rdkafka::Producer::DeliveryHandle.prepend(
  WaterDrop::Patches::RdkafkaDeliveryHandle
)
