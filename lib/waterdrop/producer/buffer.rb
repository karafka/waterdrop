# frozen_string_literal: true

module WaterDrop
  class Producer
    # Component for buffered operations
    module Buffer
      # Adds given message into the internal producer buffer without flushing it to Kafka
      #
      # @param message [Hash] hash that complies with the {Contracts::Message} contract
      # @raise [Errors::MessageInvalidError] When provided message details are invalid and the
      #   message could not be sent to Kafka
      def buffer(message)
        ensure_active!

        # The append runs under @buffer_mutex because flush/purge/close swap @messages for a fresh
        # array under the same lock. Without it, a concurrent swap between reading @messages and
        # appending would land the message in the orphaned old array and silently lose it.
        #
        # The liveness check is repeated under the same lock because `close` marks the producer
        # `:closing` before its final flush acquires @buffer_mutex. Checking only above the lock
        # would let a close complete in between, stranding the message in a closed producer's
        # buffer while this call returned success.
        @monitor.instrument(
          "message.buffered",
          producer_id: id,
          message: message,
          buffer: @messages
        ) do
          @buffer_mutex.synchronize do
            ensure_active!
            @messages << message
          end
        end
      end

      # Adds given messages into the internal producer buffer without flushing them to Kafka
      #
      # @param messages [Array<Hash>] array with messages that comply with the
      #   {Contracts::Message} contract
      # @raise [Errors::MessageInvalidError] When any of the provided messages details are invalid
      #   and the message could not be sent to Kafka
      def buffer_many(messages)
        ensure_active!

        # The concat runs under @buffer_mutex for the same reason as #buffer: flush/purge/close swap
        # @messages under the lock, so an unguarded concat could append into an array that has just
        # been captured for dispatch (or discarded), silently losing the messages. The liveness
        # check is repeated under the lock for the same reason as in #buffer: a close that
        # completes between the check above and the lock would otherwise strand the batch.
        @monitor.instrument(
          "messages.buffered",
          producer_id: id,
          messages: messages,
          buffer: @messages
        ) do
          @buffer_mutex.synchronize do
            ensure_active!
            @messages.concat(messages)
          end

          messages
        end
      end

      # Flushes the internal buffer to Kafka in an async way
      # @return [Array<Rdkafka::Producer::DeliveryHandle>] delivery handles for messages that were
      #   flushed
      def flush_async
        @monitor.instrument(
          "buffer.flushed_async",
          producer_id: id,
          messages: messages
        ) { flush(false) }
      end

      # Flushes the internal buffer to Kafka in a sync way
      # @return [Array<Rdkafka::Producer::DeliveryHandle>] delivery handles for messages that were
      #   flushed (handles are in final state, call `#create_result` to get delivery report)
      def flush_sync
        @monitor.instrument(
          "buffer.flushed_sync",
          producer_id: id,
          messages: messages
        ) { flush(true) }
      end

      private

      # Method for triggering the buffer
      # @param sync [Boolean] should it flush in a sync way
      # @return [Array<Rdkafka::Producer::DeliveryHandle>] delivery handles (in final state for
      #   sync, pending for async)
      # @raise [Errors::ProduceManyError] when there was a failure in flushing
      # @note We use this method underneath to provide a different instrumentation for sync and
      #   async flushing within the public API
      def flush(sync)
        fresh = nil
        requeued = nil
        data_for_dispatch = []

        @buffer_mutex.synchronize do
          requeued = @requeued
          @requeued = []
          fresh = @messages
          @messages = []
        end

        # Middleware runs exactly once per message: on the fresh messages here, never again on the
        # already-transformed ones coming back from a failed flush, which a second pass would
        # corrupt.
        #
        # A middleware step raising here (a serializer, encryptor or schema lookup failing) would
        # otherwise destroy both buffers, since they were emptied above and nothing has been
        # dispatched yet to recover them from. Messages are run one by one so that on failure the
        # ones whose chain completed are kept as returned and are not transformed again, while the
        # failing one and those after it stay on the middleware path. The failing one is not
        # requeued because that would dispatch it without the rest of its chain.
        #
        # Known edge case (#46840): the failing message is re-buffered in whatever state its chain
        # left it. Copy-style steps leave it untouched, but in-place steps that ran before the
        # raising one leave it partially transformed, and the next flush runs its full chain again.
        # We accept this rather than snapshotting every message before its run, which would cost a
        # copy per message on the success path as well. See Middleware#run.
        processed = []

        begin
          fresh.each { |message| processed << middleware.run(message) }
        rescue
          restore_undispatched(fresh.drop(processed.size), requeued.concat(processed))

          raise
        end

        data_for_dispatch = requeued.concat(processed)

        # Do nothing if nothing to flush
        return data_for_dispatch if data_for_dispatch.empty?

        if sync
          produce_many_sync(data_for_dispatch, run_middleware: false)
        else
          produce_many_async(data_for_dispatch, run_middleware: false)
        end
      rescue Errors::ProduceManyError => e
        # A dispatch failed partway through the batch. Re-buffer the messages that never reached
        # librdkafka so a partial failure does not silently drop valid buffered messages. For a
        # transactional producer the whole batch is rolled back (nothing is visible to consumers),
        # so all of it is restored; for a regular producer `e.dispatched` holds the handles already
        # created, so only the remainder is restored.
        requeue_unflushed(transactional? ? data_for_dispatch : data_for_dispatch.drop(e.dispatched.size))

        raise
      rescue Errors::MessageInvalidError
        # Validation runs before anything is dispatched, so nothing reached librdkafka. Restore the
        # whole batch instead of dropping valid messages alongside the invalid one.
        requeue_unflushed(data_for_dispatch)

        raise
      end

      # Puts both sets back after a failure that happened before anything reached librdkafka. They
      # go to different buffers because middleware has run on one of them and not the other, which
      # is what keeps it applied exactly once per message.
      #
      # @param fresh [Array<Hash>] messages whose middleware chain has not completed
      # @param requeued [Array<Hash>] already middleware-processed messages
      def restore_undispatched(fresh, requeued)
        @buffer_mutex.synchronize do
          @messages.unshift(*fresh)
          @requeued.unshift(*requeued)
        end
      end

      # Puts not-yet-dispatched messages back at the front of the retry buffer (preserving their
      # original order relative to each other), so a failed flush does not lose them.
      #
      # @param messages [Array<Hash>] already middleware-processed messages to restore
      def requeue_unflushed(messages)
        return if messages.empty?

        @buffer_mutex.synchronize { @requeued.unshift(*messages) }
      end
    end
  end
end
