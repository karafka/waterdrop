# frozen_string_literal: true

module WaterDrop
  class Producer
    # Transactions related producer functionalities
    module Transactions
      # Creates a transaction.
      #
      # Karafka transactions work in a similar manner to SQL db transactions though there are some
      # crucial differences. When you start a transaction, all messages produced during it will
      # be delivered together or will fail together. The difference is, that messages from within
      # a single transaction can be delivered and will have a delivery handle but will be then
      # compacted prior to moving the LSO forward. This means, that not every delivery handle for
      # async dispatches will emit a queue purge error. None for sync as the delivery has happened
      # but they will never be visible by the transactional consumers.
      #
      # Transactions **are** thread-safe however they lock a mutex. This means, that for
      # high-throughput transactional messages production in multiple threads
      # (for example in Karafka), it may be much better to use few instances that can work in
      # parallel.
      #
      # Please note, that if a producer is configured as transactional, it **cannot** produce
      # messages outside of transactions, that is why by default all dispatches will be wrapped
      # with a transaction. One transaction per single dispatch and for `produce_many` it will be
      # a single transaction wrapping all messages dispatches (not one per message).
      #
      # @return Block result
      #
      # @example Simple transaction
      #   producer.transaction do
      #     producer.produce_async(topic: 'topic', payload: 'data')
      #   end
      #
      # @example Aborted transaction - messages producer won't be visible by consumers
      #   producer.transaction do
      #     producer.produce_sync(topic: 'topic', payload: 'data')
      #     throw(:abort)
      #   end
      #
      # @example Use block result last handler to wait on all messages ack
      #   handler = producer.transaction do
      #               producer.produce_async(topic: 'topic', payload: 'data')
      #             end
      #
      #   handler.wait
      def transaction
        # This will safely allow us to support one operation transactions so a transactional
        # producer can work without the transactional block if needed
        return yield if @transaction_mutex.owned?

        @transaction_mutex.synchronize do
          transactional_instrument(:committed) do
            with_transactional_error_handling(:begin) do
              transactional_instrument(:started) { client.begin_transaction }
            end

            result = nil
            commit = false

            catch(:abort) do
              result = yield
              commit = true
            end

            commit || raise(WaterDrop::Errors::AbortTransaction)

            with_transactional_error_handling(:commit) do
              client.commit_transaction
            end

            result
          rescue StandardError => e
            with_transactional_error_handling(:abort) do
              transactional_instrument(:aborted) { client.abort_transaction }
            end

            raise unless e.is_a?(WaterDrop::Errors::AbortTransaction)
          end
        end
      end

      # @return [Boolean] Is this producer a transactional one
      def transactional?
        return @transactional if instance_variable_defined?(:'@transactional')

        @transactional = config.kafka.to_h.key?(:'transactional.id')
      end

      private

      # Runs provided code with a transaction wrapper if transactions are enabled.
      # This allows us to simplify the async and sync batch dispatchers because we can ensure that
      # their internal dispatches will be wrapped only with a single transaction and not
      # a transaction per message
      # @param block [Proc] code we want to run
      def with_transaction_if_transactional(&block)
        transactional? ? transaction(&block) : yield
      end

      # Instruments the transactional operation with producer id
      #
      # @param key [Symbol] transaction operation key
      # @param block [Proc] block to run inside the instrumentation or nothing if not given
      def transactional_instrument(key, &block)
        @monitor.instrument("transaction.#{key}", producer_id: id, &block)
      end

      # Error handling for transactional operations is a bit special. There are three types of
      # errors coming from librdkafka:
      #   - retryable - indicates that a given operation (like offset commit) can be retried after
      #     a backoff and that is should be operating later as expected. We try to retry those
      #     few times before finally failing.
      #   - fatal - errors that will not recover no matter what (for example being fenced out)
      #   - abortable - error from which we cannot recover but for which we should abort the
      #     current transaction.
      #
      # The code below handles this logic also publishing the appropriate notifications via our
      # notifications pipeline.
      #
      # @param action [Symbol] action type
      # @param allow_abortable [Boolean] should we allow for the abortable flow. This is set to
      #   false internally to prevent attempts to abort from failed abort operations
      def with_transactional_error_handling(action, allow_abortable: true)
        attempt ||= 0
        attempt += 1

        yield
      rescue ::Rdkafka::RdkafkaError => e
        # Decide if there is a chance to retry given error
        do_retry = e.retryable? && attempt < config.max_attempts_on_transaction_command

        @monitor.instrument(
          'error.occurred',
          producer_id: id,
          caller: self,
          error: e,
          type: "transaction.#{action}",
          retry: do_retry,
          attempt: attempt
        )

        raise if e.fatal?

        if do_retry
          # Backoff more and more before retries
          sleep(config.wait_backoff_on_transaction_command * attempt)

          retry
        end

        if e.abortable? && allow_abortable
          # Always attempt to abort but if aborting fails with an abortable error, do not attempt
          # to abort from abort as this could create an infinite loop
          with_transactional_error_handling(:abort, allow_abortable: false) do
            transactional_instrument(:aborted) { @client.abort_transaction }
          end

          raise
        end

        raise
      end
    end
  end
end
