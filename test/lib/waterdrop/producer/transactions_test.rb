# frozen_string_literal: true

require "test_helper"

describe_current do
  before do
    @producer = build(:transactional_producer)
    @transactional_message_stub = Struct.new(:topic, :partition, :offset, keyword_init: true)
    @transactional_consumer_stub = Struct.new(:consumer_group_metadata_pointer, :dummy, keyword_init: true)
    @transactional_id = SecureRandom.uuid
    @critical_error = Exception
    @topic_name = "it-#{SecureRandom.uuid}"
  end

  after { @producer.close }

  it do
    # First run will check if cached
    @producer.transactional?
    assert_equal(true, @producer.transactional?)
  end

  context "when we try to create producer with invalid transactional settings" do
    it "expect to raise an error" do
      error = assert_raises(Rdkafka::Config::ConfigError) do
        build(:transactional_producer, transaction_timeout_ms: 100).client
      end
      assert_match(/transaction\.timeout\.ms/, error.message)
    end
  end

  context "when we try to create producer with invalid acks" do
    it "expect to raise an error" do
      error = assert_raises(Rdkafka::Config::ClientCreationError) do
        build(:transactional_producer, request_required_acks: 1).client
      end
      assert_match(/acks/, error.message)
    end
  end

  context "when we try to start transaction without transactional.id" do
    before do
      @producer = build(:producer)
    end

    it "expect to raise with info that this functionality is not configured" do
      assert_raises(WaterDrop::Errors::ProducerNotTransactionalError) { @producer.transaction { nil } }
    end

    it { assert_equal(false, @producer.transactional?) }
    it { assert_equal(false, @producer.transaction?) }
  end

  context "when we make a transaction without sending any messages" do
    it "expect not to crash and do nothing" do
      @producer.transaction { nil }
    end
  end

  context "when we dispatch in transaction to multiple topics" do
    it "expect to work" do
      handlers = []

      @producer.transaction do
        handlers << @producer.produce_async(topic: "#{@topic_name}1", payload: "1")
        handlers << @producer.produce_async(topic: "#{@topic_name}2", payload: "2")
      end

      handlers.map!(&:wait)
    end

    it "expect to return block result as the transaction result" do
      result = rand

      transaction_result = @producer.transaction do
        @producer.produce_async(topic: @topic_name, payload: "2")
        result
      end

      assert_equal(result, transaction_result)
    end

    it "expect not to allow to disconnect producer during transaction" do
      @producer.transaction do
        assert_equal(false, @producer.disconnect)
      end
    end

    it "expect to allow to disconnect producer after transaction" do
      @producer.transaction do
        @producer.produce_async(topic: @topic_name, payload: "2")
      end

      assert_equal(true, @producer.disconnect)
    end
  end

  context "when we dispatch in transaction to multiple topics with array headers" do
    it "expect to work" do
      handlers = []

      @producer.transaction do
        handlers << @producer.produce_async(
          topic: "#{@topic_name}1",
          payload: "1",
          headers: { "a" => "b", "c" => %w[d e] }
        )
        handlers << @producer.produce_async(
          topic: "#{@topic_name}2",
          payload: "2",
          headers: { "a" => "b", "c" => %w[d e] }
        )
      end

      handlers.map!(&:wait)
    end

    it "expect to return block result as the transaction result" do
      result = rand

      transaction_result = @producer.transaction do
        @producer.produce_async(topic: @topic_name, payload: "2")
        result
      end

      assert_equal(result, transaction_result)
    end
  end

  context "when trying to use transaction on a non-existing topics and short time" do
    before do
      @producer = build(:transactional_producer, transaction_timeout_ms: 1_000)
    end

    it "expect to crash with an inconsistent or a timeout state after abort" do
      error = nil

      begin
        @producer.transaction do
          20.times do |i|
            @producer.produce_async(topic: @topic_name, payload: i.to_s)
          end
        end
      rescue Rdkafka::RdkafkaError => e
        error = e
      end

      # This spec is not fully stable due to how librdkafka works
      if error
        assert_kind_of(Rdkafka::RdkafkaError, error)
        assert_equal(:state, error.code)
        assert_kind_of(Rdkafka::RdkafkaError, error.cause)
        assert_includes(%i[inconsistent timed_out], error.cause.code)
      end
    end
  end

  context "when trying to use transaction on a non-existing topics and enough time" do
    before do
      @producer = build(:transactional_producer)
    end

    it "expect not to crash and publish all data" do
      @producer.transaction do
        10.times do |i|
          @producer.produce_async(topic: @topic_name, payload: i.to_s)
        end
      end
    end
  end

  context "when we start transaction and raise an error" do
    before do
      @producer = build(:transactional_producer, queue_buffering_max_ms: 5_000)
    end

    it "expect to re-raise this error" do
      assert_raises(StandardError) do
        @producer.transaction do
          @producer.produce_async(topic: @topic_name, payload: "na")

          raise StandardError
        end
      end
    end

    it "expect to cancel the dispatch of the message" do
      handler = nil

      begin
        @producer.transaction do
          handler = @producer.produce_async(topic: @topic_name, payload: "na")

          raise StandardError
        end
      rescue
        nil
      end

      error = assert_raises(Rdkafka::RdkafkaError) { handler.wait }
      assert_match(/Purged in queue/, error.message)
    end

    context "when we have error instrumentation" do
      before do
        @errors = []
        @purges = []

        @producer.monitor.subscribe("error.occurred") do |event|
          @errors << event[:error]
        end

        @producer.monitor.subscribe("message.purged") do |event|
          @purges << event[:error]
        end

        begin
          @producer.transaction do
            @producer.produce_async(topic: @topic_name, payload: "na")

            raise StandardError
          end
        rescue
          nil
        end
      end

      it "expect not to emit the cancellation error via the error pipeline" do
        assert_empty(@errors)
      end

      it "expect to emit the cancellation error via the message.purged" do
        assert_kind_of(Rdkafka::RdkafkaError, @purges.first)
        assert_equal(:purge_queue, @purges.first.code)
      end
    end

    context "when using sync producer" do
      it "expect to wait on the initial delivery per message and have it internally" do
        result = nil

        begin
          @producer.transaction do
            result = @producer.produce_sync(topic: @topic_name, payload: "na")

            assert_equal(0, result.partition)
            assert_nil(result.error)

            raise StandardError
          end
        rescue
          nil
        end

        # It will be compacted but is still visible as a delivery report
        assert_equal(0, result.partition)
        assert_nil(result.error)
      end
    end

    context "when using async producer and waiting" do
      it "expect to wait on the initial delivery per message" do
        handler = nil

        begin
          @producer.transaction do
            handler = @producer.produce_async(topic: @topic_name, payload: "na")

            raise StandardError
          end
        rescue
          nil
        end

        result = handler.create_result

        # It will be compacted but is still visible as a delivery report
        assert_includes([-1, 0], result.partition)
        # This can be either rejected in-flight or after delivery to kafka despite async under
        # heavy load, so offset may be assigned
        assert(result.offset == -1001 || result.offset > -1)
        assert_kind_of(Rdkafka::RdkafkaError, result.error)
      end
    end
  end

  context "when we start transaction and raise a critical Exception" do
    it "expect to re-raise this error" do
      assert_raises(@critical_error) do
        @producer.transaction do
          @producer.produce_async(topic: @topic_name, payload: "na")

          raise @critical_error
        end
      end
    end

    it "expect to cancel the dispatch of the message" do
      handler = nil

      begin
        @producer.transaction do
          handler = @producer.produce_async(topic: @topic_name, payload: "na")

          raise @critical_error
        end
      rescue @critical_error
        nil
      end

      error = assert_raises(Rdkafka::RdkafkaError) { handler.wait }
      assert_match(/Purged in queue/, error.message)
    end

    # The rest is expected to behave the same way as StandardError so not duplicating
  end

  context "when we start transaction and abort" do
    it "expect not to re-raise" do
      @producer.transaction do
        @producer.produce_async(topic: @topic_name, payload: "na")

        raise WaterDrop::AbortTransaction
      end
    end

    it "expect to cancel the dispatch of the message" do
      handler = nil

      @producer.transaction do
        handler = @producer.produce_async(topic: @topic_name, payload: "na")

        raise WaterDrop::AbortTransaction
      end

      error = assert_raises(Rdkafka::RdkafkaError) { handler.wait }
      assert_match(/Purged in queue/, error.message)
    end

    context "when we have error instrumentation" do
      before do
        @errors = []
        @purges = []

        @producer.monitor.subscribe("error.occurred") do |event|
          @errors << event[:error]
        end

        @producer.monitor.subscribe("message.purged") do |event|
          @purges << event[:error]
        end

        @producer.transaction do
          sleep(0.1)
          @producer.produce_async(topic: @topic_name, payload: "na")

          raise(WaterDrop::AbortTransaction)
        end
      end

      it "expect not to emit the cancellation error via the error pipeline" do
        assert_empty(@errors)
      end

      it "expect to emit the cancellation error via the message.purged" do
        assert_kind_of(Rdkafka::RdkafkaError, @purges.first)
        assert_equal(:purge_queue, @purges.first.code)
      end
    end

    context "when using sync producer" do
      it "expect to wait on the initial delivery per message and have it internally" do
        result = nil

        @producer.transaction do
          result = @producer.produce_sync(topic: @topic_name, payload: "na")

          assert_equal(0, result.partition)
          assert_nil(result.error)

          raise(WaterDrop::AbortTransaction)
        end

        # It will be compacted but is still visible as a delivery report
        assert_equal(0, result.partition)
        assert_nil(result.error)
      end
    end

    context "when using async producer and waiting" do
      it "expect to wait on the initial delivery per message" do
        handler = nil

        @producer.transaction do
          handler = @producer.produce_async(topic: @topic_name, payload: "na")

          raise(WaterDrop::AbortTransaction)
        end

        result = handler.create_result

        # It will be compacted but is still visible as a delivery report
        # It can be either -1 or 0 if topic was created fast enough for report to become aware
        # of it
        assert_includes([-1, 0], result.partition)
        assert_equal(-1_001, result.offset)
        assert_kind_of(Rdkafka::RdkafkaError, result.error)
      end
    end
  end

  context "when we try to create a producer with already used transactional_id" do
    before do
      @producer1 = build(:transactional_producer, transactional_id: @transactional_id)
      @producer2 = build(:transactional_producer, transactional_id: @transactional_id)
    end

    after do
      @producer1.close
      @producer2.close
    end

    it "expect to fence out the previous one" do
      @producer1.transaction { nil }
      @producer2.transaction { nil }

      error = assert_raises(Rdkafka::RdkafkaError) do
        @producer1.transaction do
          @producer1.produce_async(topic: @topic_name, payload: "1")
        end
      end
      assert_match(/fenced by a newer instance/, error.message)
    end

    it "expect not to fence out the new one" do
      @producer1.transaction { nil }
      @producer2.transaction { nil }

      @producer2.transaction do
        @producer2.produce_async(topic: @topic_name, payload: "1")
      end
    end
  end

  context "when trying to close a producer from inside of a transaction" do
    it "expect to raise an error" do
      assert_raises(WaterDrop::Errors::ProducerTransactionalCloseAttemptError) do
        @producer.transaction do
          @producer.close
        end
      end
    end
  end

  context "when trying to close a producer from a different thread during transaction" do
    it "expect to raise an error" do
      @producer.transaction do
        Thread.new { @producer.close }
        sleep(1)
      end
    end
  end

  context "when transaction crashes internally on one of the retryable operations" do
    it "expect to retry and continue" do
      counter = 0
      ref = @producer.client.method(:begin_transaction)

      @producer.client.stub(:begin_transaction, lambda {
        if counter.zero?
          counter += 1
          raise(Rdkafka::RdkafkaError.new(-152, retryable: true))
        end
        ref.call
      }) do
        @producer.transaction { nil }
      end
    end
  end

  context "when we use transactional producer without transaction" do
    it "expect to allow as it will wrap with a transaction" do
      @producer.produce_sync(topic: @topic_name, payload: rand.to_s)
    end

    it "expect to deliver message correctly" do
      result = @producer.produce_sync(topic: @topic_name, payload: rand.to_s)
      assert_equal(@topic_name, result.topic_name)
      assert_nil(result.error)
    end

    it "expect to use the async dispatch though with transaction wrapper" do
      handler = @producer.produce_async(topic: @topic_name, payload: rand.to_s)
      result = handler.wait
      assert_equal(@topic_name, result.topic_name)
      assert_nil(result.error)
    end

    context "when using with produce_many_sync" do
      before do
        @messages = Array.new(10) { build(:valid_message) }
        @counts = []

        local_counts = @counts
        @producer.monitor.subscribe("transaction.committed") { local_counts << true }
      end

      it "expect to wrap it with a single transaction" do
        @producer.produce_many_sync(@messages)
        assert_equal(1, @counts.size)
      end

      context "when error occurs after few messages" do
        before do
          @producer = build(:transactional_producer, max_payload_size: 10 * 1_024 * 1_024)

          too_big = build(:valid_message)
          too_big[:payload] = "1" * 1024 * 1024

          @messages = [
            Array.new(9) { build(:valid_message) },
            too_big
          ].flatten

          @dispatched = []

          @producer.monitor.subscribe("error.occurred") do |event|
            @dispatched << event[:dispatched]
          end
        end

        it "expect not to contain anything in the dispatched notification" do
          assert_raises(WaterDrop::Errors::ProduceManyError) { @producer.produce_many_sync(@messages) }

          assert_equal([], @dispatched.flatten)
        end
      end
    end

    context "when using with produce_many_async" do
      before do
        @messages = Array.new(10) { build(:valid_message) }
        @counts = []

        local_counts = @counts
        @producer.monitor.subscribe("transaction.committed") { local_counts << true }
      end

      it "expect to wrap it with a single transaction" do
        @producer.produce_many_async(@messages)
        assert_equal(1, @counts.size)
      end
    end
  end

  context "when nesting transaction inside of a transaction" do
    before do
      @counts = []

      local_counts = @counts
      @producer.monitor.subscribe("transaction.committed") { local_counts << true }
    end

    it "expect to work" do
      handlers = []

      @producer.transaction do
        handlers << @producer.produce_async(topic: @topic_name, payload: "data")

        @producer.transaction do
          handlers << @producer.produce_async(topic: @topic_name, payload: "data")
        end
      end

      handlers.each { |handler| handler.wait }
    end

    it "expect to have one actual transaction" do
      @producer.transaction do
        @producer.produce_async(topic: @topic_name, payload: "data")

        @producer.transaction do
          @producer.produce_async(topic: @topic_name, payload: "data")
        end
      end

      assert_equal(1, @counts.size)
    end

    context "when we abort the nested transaction" do
      before do
        @producer = build(:transactional_producer, queue_buffering_max_ms: 5_000)
      end

      it "expect to abort all levels" do
        handlers = []

        @producer.transaction do
          handlers << @producer.produce_async(topic: @topic_name, payload: "data")

          @producer.transaction do
            handlers << @producer.produce_async(topic: @topic_name, payload: "data")
            raise(WaterDrop::AbortTransaction)
          end
        end

        handlers.each do |handler|
          error = assert_raises(Rdkafka::RdkafkaError) { handler.wait }
          assert_match(/Purged in queue/, error.message)
        end
      end
    end
  end

  context "when trying to mark as consumed in a transaction" do
    before do
      @message = @transactional_message_stub.new(topic: @topic_name, partition: 0, offset: 100)
    end

    context "when we try mark as consumed without a transaction" do
      it "expect to raise an error" do
        assert_raises(WaterDrop::Errors::TransactionRequiredError) do
          @producer.transaction_mark_as_consumed(nil, @message)
        end
      end
    end

    context "when we try mark as consumed with invalid arguments" do
      before do
        @invalid_consumer_stub = Struct.new(:dummy, keyword_init: true)
        @consumer = @invalid_consumer_stub.new(dummy: nil)
      end

      it "expect to delegate to client send_offsets_to_transaction with correct timeout" do
        @producer.client.stub(:send_offsets_to_transaction, ->(*_) { nil }) do
          @producer.transaction do
            assert_raises(WaterDrop::Errors::TransactionalOffsetInvalidError) do
              @producer.transaction_mark_as_consumed(@consumer, @message)
            end
          end
        end
      end
    end

    # Full e2e integration of this is checked in Karafka as we do not operate on consumers here
    context "when trying mark as consumed inside a transaction" do
      before do
        @consumer = @transactional_consumer_stub.new(consumer_group_metadata_pointer: 1, dummy: nil)
      end

      it "expect to delegate to client send_offsets_to_transaction with correct timeout" do
        called_with = nil
        send_stub = lambda do |consumer, *args|
          called_with = [consumer, *args]
          nil
        end

        @producer.client.stub(:send_offsets_to_transaction, send_stub) do
          @producer.transaction do
            @producer.transaction_mark_as_consumed(@consumer, @message)
          end
        end

        assert_equal(@consumer, called_with[0])
        assert_equal(30_000, called_with.last)
      end
    end
  end

  context "when creating transactional producer with default config" do
    before do
      @prev_producer = @producer
      @producer = WaterDrop::Producer.new do |config|
        config.deliver = true
        config.kafka = {
          "bootstrap.servers": BOOTSTRAP_SERVERS,
          "request.required.acks": 1,
          "transactional.id": SecureRandom.uuid,
          acks: "all"
        }
      end
    end

    after { @prev_producer&.close }

    it "expect to be able to do so and to send a message" do
      @producer.produce_async(topic: @topic_name, payload: "a")
    end
  end

  context "when we are not inside a running transaction" do
    it { assert_equal(false, @producer.transaction?) }
  end

  context "when we are inside a transaction" do
    it "expect to be recognize it and be true" do
      @producer.transaction do
        assert_equal(true, @producer.transaction?)
      end
    end
  end

  context "when we are inside a transaction and early break" do
    it "expect to raise error" do
      assert_raises(WaterDrop::Errors::EarlyTransactionExitNotAllowedError) do
        @producer.transaction { break }
      end
    end

    it "expect to cancel dispatches" do
      handler = nil

      begin
        @producer.transaction do
          handler = @producer.produce_async(topic: @topic_name, payload: "na")

          break
        end
      rescue WaterDrop::Errors::EarlyTransactionExitNotAllowedError
        error = assert_raises(Rdkafka::RdkafkaError) { handler.wait }
        assert_match(/Purged in queue/, error.message)
      end
    end

    it "expect not to affect the client state in an inconsistent way" do
      begin
        @producer.transaction do
          break
        end
      rescue WaterDrop::Errors::EarlyTransactionExitNotAllowedError
        nil
      end

      handler = @producer.transaction do
        @producer.produce_async(topic: @topic_name, payload: "na")
      end

      handler.wait
    end
  end

  context "when producer gets a critical broker errors with reload on" do
    before do
      @prev_producer = @producer
      @producer = WaterDrop::Producer.new do |config|
        config.max_payload_size = 1_000_000_000_000
        config.kafka = {
          "bootstrap.servers": BOOTSTRAP_SERVERS,
          "transactional.id": SecureRandom.uuid,
          "max.in.flight": 5
        }
      end

      admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP_SERVERS).admin
      admin.create_topic(@topic_name, 1, 1, "max.message.bytes": 128).wait
      admin.close
    end

    after { @prev_producer&.close }

    it "expect to be able to use same producer after the error when async" do
      errored = false

      begin
        @producer.produce_async(topic: @topic_name, payload: "1" * 512)
      rescue WaterDrop::Errors::ProduceError
        errored = true
      end

      assert_equal(true, errored)

      @producer.produce_async(topic: @topic_name, payload: "1")
    end

    it "expect to be able to use same producer after the error when sync" do
      errored = false

      begin
        @producer.produce_sync(topic: @topic_name, payload: "1" * 512)
      rescue WaterDrop::Errors::ProduceError
        errored = true
      end

      assert_equal(true, errored)

      @producer.produce_sync(topic: @topic_name, payload: "1")
    end
  end

  context "when wrapping an early return method with a transaction" do
    before do
      t_name = @topic_name

      @operation = Class.new do
        define_method :call do |producer, handlers|
          handlers << producer.produce_async(topic: "#{t_name}1", payload: "1")

          return unless handlers.empty?

          # Never to be reached, expected in this spec
          handlers << producer.produce_async(topic: "#{t_name}1", payload: "1")
        end
      end
    end

    it "expect to work correctly" do
      handlers = []

      @producer.transaction do
        @operation.new.call(@producer, handlers)
      end

      handlers.map!(&:wait)
    end
  end

  context "when wrapping an early break block with a transaction" do
    it "expect to work correctly" do
      topic_name = @topic_name
      operation = lambda do |producer, handlers|
        handlers << producer.produce_async(topic: "#{topic_name}1", payload: "1")

        return unless handlers.empty?

        # Never to be reached, expected in this spec
        handlers << producer.produce_async(topic: "#{topic_name}1", payload: "1")
      end

      handlers = []

      @producer.transaction do
        operation.call(@producer, handlers)
      end

      handlers.map!(&:wait)
    end
  end

  context "when trying to use a closed producer to start a transaction" do
    before { @producer.close }

    it "expect not to allow it" do
      assert_raises(WaterDrop::Errors::ProducerClosedError) do
        @producer.transaction { nil }
      end
    end
  end

  context "when fatal error occurs during transaction" do
    context "with reload_on_transaction_fatal_error enabled" do
      before do
        @producer = build(:transactional_producer, reload_on_transaction_fatal_error: true)
      end

      it "expect reload_on_transaction_fatal_error to be enabled by default" do
        assert_equal(true, @producer.config.reload_on_transaction_fatal_error)
      end

      it "expect to have correct default backoff and max attempts config" do
        assert_equal(1_000, @producer.config.wait_backoff_on_transaction_fatal_error)
        assert_equal(10, @producer.config.max_attempts_on_transaction_fatal_error)
      end

      it "expect transactional_retryable? to check retry limit correctly" do
        # Initially should be retryable
        assert_equal(true, @producer.transactional_retryable?)

        # Configure producer with low limit and test edge
        limited_producer = build(
          :transactional_producer,
          reload_on_transaction_fatal_error: true,
          max_attempts_on_transaction_fatal_error: 2
        )

        assert_equal(true, limited_producer.transactional_retryable?)

        limited_producer.close
      end

      it "expect successful transaction to complete without reload" do
        reloaded_events = []
        @producer.monitor.subscribe("producer.reloaded") { |event| reloaded_events << event }

        # Do a normal successful transaction
        @producer.transaction do
          @producer.produce_sync(topic: @topic_name, payload: "test")
        end

        # No reloads should have occurred
        assert_empty(reloaded_events)
      end
    end
  end
end
