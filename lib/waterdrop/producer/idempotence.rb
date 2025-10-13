# frozen_string_literal: true

module WaterDrop
  class Producer
    # Component for idempotent producer operations and error recovery
    module Idempotence
      # @return [Boolean] true if current producer is idempotent
      def idempotent?
        # Every transactional producer is idempotent by default always
        return true if transactional?
        return @idempotent unless @idempotent.nil?

        @idempotent = config.kafka.to_h.fetch(:'enable.idempotence', false)
      end

      # Checks if the given error should trigger an idempotent producer reload
      #
      # @param error [Rdkafka::RdkafkaError] the error to check
      # @return [Boolean] true if the error should trigger a reload
      #
      # @note Returns true only if all of the following conditions are met:
      #   - Error is fatal
      #   - Producer is idempotent
      #   - Producer is not transactional
      #   - reload_on_idempotent_fatal_error config is enabled
      #   - Error is not in the non_reloadable_errors config list
      def idempotent_reloadable?(error)
        return false unless error.fatal?
        return false unless idempotent?
        return false if transactional?
        return false unless config.reload_on_idempotent_fatal_error
        return false if config.non_reloadable_errors.include?(error.code)

        true
      end

      # Checks if we can still retry reloading after an idempotent fatal error
      #
      # @return [Boolean] true if we haven't exceeded the max reload attempts yet
      def idempotent_retryable?
        @idempotent_fatal_error_attempts < config.max_attempts_on_idempotent_fatal_error
      end

      private

      # Reloads the underlying client instance when a fatal error occurs on an idempotent producer
      #
      # This method handles fatal errors that can occur in idempotent (non-transactional) producers
      # When a fatal error is detected, it will flush pending messages, purge the queue, close the
      # old client, and create a new client instance to continue operations.
      #
      # @param attempt [Integer] the current reload attempt number
      #
      # @note This is only called for idempotent, non-transactional producers when
      #   `reload_on_idempotent_fatal_error` is enabled
      # @note After reload, the producer will automatically retry the failed operation
      def idempotent_reload_client_on_fatal_error(attempt)
        @operating_mutex.synchronize do
          @monitor.instrument(
            'producer.reloaded',
            producer_id: id,
            attempt: attempt
          ) do
            @client.flush(current_variant.max_wait_timeout)
            purge
            @client.close
            @client = Builder.new.call(self, @config)
          end
        end
      end
    end
  end
end
