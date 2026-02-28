# frozen_string_literal: true

class ProducerTransactionsTest < WaterDropTest::Base
  def setup
    @producer = build(:transactional_producer)
    @topic_name = "it-#{SecureRandom.uuid}"
    @transactional_message_stub = Struct.new(:topic, :partition, :offset, keyword_init: true)
    @transactional_consumer_stub = Struct.new(
      :consumer_group_metadata_pointer, :dummy, keyword_init: true
    )
  end

  def teardown
    @producer.close
    super
  end

  def test_transactional_returns_true
    @producer.transactional?

    assert_same true, @producer.transactional?
  end

  def test_invalid_transactional_settings_raises
    producer = build(:transactional_producer, transaction_timeout_ms: 100)

    assert_raises(Rdkafka::Config::ConfigError) do
      producer.client
    end
  end

  def test_invalid_acks_raises
    producer = build(:transactional_producer, request_required_acks: 1)

    assert_raises(Rdkafka::Config::ClientCreationError) do
      producer.client
    end
  end

  def test_transaction_without_transactional_id
    producer = build(:producer)

    assert_raises(WaterDrop::Errors::ProducerNotTransactionalError) do
      producer.transaction { nil }
    end

    assert_same false, producer.transactional?
    assert_same false, producer.transaction?

    producer.close
  end

  def test_transaction_without_messages
    @producer.transaction { nil }
  end

  def test_transaction_dispatches_to_multiple_topics
    handlers = []

    @producer.transaction do
      handlers << @producer.produce_async(topic: "#{@topic_name}1", payload: "1")
      handlers << @producer.produce_async(topic: "#{@topic_name}2", payload: "2")
    end

    handlers.map!(&:wait)
  end

  def test_transaction_returns_block_result
    result = rand

    transaction_result = @producer.transaction do
      @producer.produce_async(topic: @topic_name, payload: "2")
      result
    end

    assert_equal result, transaction_result
  end

  def test_disconnect_not_allowed_during_transaction
    @producer.transaction do
      assert_same false, @producer.disconnect
    end
  end

  def test_disconnect_allowed_after_transaction
    @producer.transaction do
      @producer.produce_async(topic: @topic_name, payload: "2")
    end

    assert_same true, @producer.disconnect
  end

  def test_transaction_with_array_headers
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

  def test_transaction_with_array_headers_returns_block_result
    result = rand

    transaction_result = @producer.transaction do
      @producer.produce_async(topic: @topic_name, payload: "2")
      result
    end

    assert_equal result, transaction_result
  end
end

class ProducerTransactionsNonExistingTopicShortTimeTest < WaterDropTest::Base
  def setup
    @producer = build(:transactional_producer, transaction_timeout_ms: 1_000)
    @topic_name = "it-#{SecureRandom.uuid}"
  end

  def teardown
    @producer.close
    super
  end

  def test_crash_with_inconsistent_or_timeout_state_after_abort
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

    if error
      assert_kind_of Rdkafka::RdkafkaError, error
      assert_equal :state, error.code
      assert_kind_of Rdkafka::RdkafkaError, error.cause
      assert_includes %i[inconsistent timed_out], error.cause.code
    end
  end
end

class ProducerTransactionsNonExistingTopicEnoughTimeTest < WaterDropTest::Base
  def setup
    @producer = build(:transactional_producer)
    @topic_name = "it-#{SecureRandom.uuid}"
  end

  def teardown
    @producer.close
    super
  end

  def test_publish_all_data_without_crash
    @producer.transaction do
      10.times do |i|
        @producer.produce_async(topic: @topic_name, payload: i.to_s)
      end
    end
  end
end

class ProducerTransactionsErrorRaisingTest < WaterDropTest::Base
  def setup
    @producer = build(:transactional_producer, queue_buffering_max_ms: 5_000)
    @topic_name = "it-#{SecureRandom.uuid}"
  end

  def teardown
    @producer.close
    super
  end

  def test_re_raises_standard_error
    assert_raises(StandardError) do
      @producer.transaction do
        @producer.produce_async(topic: @topic_name, payload: "na")
        raise StandardError
      end
    end
  end

  def test_cancels_dispatch_on_standard_error
    handler = nil

    begin
      @producer.transaction do
        handler = @producer.produce_async(topic: @topic_name, payload: "na")
        raise StandardError
      end
    rescue
      nil
    end

    assert_raises(Rdkafka::RdkafkaError) { handler.wait }
  end

  def test_error_instrumentation_no_error_pipeline_on_standard_error
    errors = []
    purges = []

    @producer.monitor.subscribe("error.occurred") { |event| errors << event[:error] }
    @producer.monitor.subscribe("message.purged") { |event| purges << event[:error] }

    begin
      @producer.transaction do
        @producer.produce_async(topic: @topic_name, payload: "na")
        raise StandardError
      end
    rescue
      nil
    end

    assert_empty errors
  end

  def test_error_instrumentation_purge_event_on_standard_error
    purges = []

    @producer.monitor.subscribe("message.purged") { |event| purges << event[:error] }

    begin
      @producer.transaction do
        @producer.produce_async(topic: @topic_name, payload: "na")
        raise StandardError
      end
    rescue
      nil
    end

    assert_kind_of Rdkafka::RdkafkaError, purges.first
    assert_equal :purge_queue, purges.first.code
  end

  def test_sync_producer_within_transaction_on_error
    result = nil

    begin
      @producer.transaction do
        result = @producer.produce_sync(topic: @topic_name, payload: "na")

        assert_equal 0, result.partition
        assert_nil result.error

        raise StandardError
      end
    rescue
      nil
    end

    assert_equal 0, result.partition
    assert_nil result.error
  end

  def test_async_producer_and_waiting_within_transaction_on_error
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

    assert_includes [-1, 0], result.partition
    assert(result.offset == -1_001 || result.offset > -1)
    assert_kind_of Rdkafka::RdkafkaError, result.error
  end
end

class ProducerTransactionsCriticalExceptionTest < WaterDropTest::Base
  def setup
    @producer = build(:transactional_producer)
    @topic_name = "it-#{SecureRandom.uuid}"
  end

  def teardown
    @producer.close
    super
  end

  def test_re_raises_critical_exception
    assert_raises(Exception) do
      @producer.transaction do
        @producer.produce_async(topic: @topic_name, payload: "na")
        raise Exception # rubocop:disable Lint/RaiseException
      end
    end
  end

  def test_cancels_dispatch_on_critical_exception
    handler = nil

    begin
      @producer.transaction do
        handler = @producer.produce_async(topic: @topic_name, payload: "na")
        raise Exception # rubocop:disable Lint/RaiseException
      end
    rescue Exception # rubocop:disable Lint/RescueException
      nil
    end

    assert_raises(Rdkafka::RdkafkaError) { handler.wait }
  end
end

class ProducerTransactionsAbortTest < WaterDropTest::Base
  def setup
    @producer = build(:transactional_producer)
    @topic_name = "it-#{SecureRandom.uuid}"
  end

  def teardown
    @producer.close
    super
  end

  def test_abort_does_not_re_raise
    @producer.transaction do
      @producer.produce_async(topic: @topic_name, payload: "na")
      raise WaterDrop::AbortTransaction
    end
  end

  def test_abort_cancels_dispatch
    handler = nil

    @producer.transaction do
      handler = @producer.produce_async(topic: @topic_name, payload: "na")
      raise WaterDrop::AbortTransaction
    end

    assert_raises(Rdkafka::RdkafkaError) { handler.wait }
  end

  def test_abort_error_instrumentation_no_error_pipeline
    errors = []
    purges = []

    @producer.monitor.subscribe("error.occurred") { |event| errors << event[:error] }
    @producer.monitor.subscribe("message.purged") { |event| purges << event[:error] }

    @producer.transaction do
      sleep(0.1)
      @producer.produce_async(topic: @topic_name, payload: "na")
      raise(WaterDrop::AbortTransaction)
    end

    assert_empty errors
  end

  def test_abort_error_instrumentation_purge_event
    purges = []

    @producer.monitor.subscribe("message.purged") { |event| purges << event[:error] }

    @producer.transaction do
      sleep(0.1)
      @producer.produce_async(topic: @topic_name, payload: "na")
      raise(WaterDrop::AbortTransaction)
    end

    assert_kind_of Rdkafka::RdkafkaError, purges.first
    assert_equal :purge_queue, purges.first.code
  end

  def test_abort_sync_producer_within_transaction
    result = nil

    @producer.transaction do
      result = @producer.produce_sync(topic: @topic_name, payload: "na")

      assert_equal 0, result.partition
      assert_nil result.error

      raise(WaterDrop::AbortTransaction)
    end

    assert_equal 0, result.partition
    assert_nil result.error
  end

  def test_abort_async_producer_and_waiting
    handler = nil

    @producer.transaction do
      handler = @producer.produce_async(topic: @topic_name, payload: "na")
      raise(WaterDrop::AbortTransaction)
    end

    result = handler.create_result

    assert_includes [-1, 0], result.partition
    assert_equal(-1_001, result.offset)
    assert_kind_of Rdkafka::RdkafkaError, result.error
  end
end

class ProducerTransactionsDuplicateTransactionalIdTest < WaterDropTest::Base
  def setup
    @transactional_id = SecureRandom.uuid
    @producer1 = build(:transactional_producer, transactional_id: @transactional_id)
    @producer2 = build(:transactional_producer, transactional_id: @transactional_id)
    @topic_name = "it-#{SecureRandom.uuid}"
  end

  def teardown
    @producer1.close
    @producer2.close
    super
  end

  def test_fences_out_previous_producer
    @producer1.transaction { nil }
    @producer2.transaction { nil }

    assert_raises(Rdkafka::RdkafkaError) do
      @producer1.transaction do
        @producer1.produce_async(topic: @topic_name, payload: "1")
      end
    end
  end

  def test_does_not_fence_new_producer
    @producer1.transaction { nil }
    @producer2.transaction { nil }

    @producer2.transaction do
      @producer2.produce_async(topic: @topic_name, payload: "1")
    end
  end
end

class ProducerTransactionsCloseInsideTest < WaterDropTest::Base
  def setup
    @producer = build(:transactional_producer)
    @topic_name = "it-#{SecureRandom.uuid}"
  end

  def teardown
    @producer.close
    super
  end

  def test_close_from_inside_raises_error
    assert_raises(WaterDrop::Errors::ProducerTransactionalCloseAttemptError) do
      @producer.transaction do
        @producer.close
      end
    end
  end

  def test_close_from_different_thread_during_transaction
    @producer.transaction do
      Thread.new { @producer.close }
      sleep(1)
    end
  end
end

class ProducerTransactionsRetryableErrorTest < WaterDropTest::Base
  def setup
    @producer = build(:transactional_producer)
    @topic_name = "it-#{SecureRandom.uuid}"
  end

  def teardown
    @producer.close
    super
  end

  def test_retries_and_continues_on_retryable_error
    counter = 0
    original_method = @producer.client.method(:begin_transaction)

    @producer.client.stub(:begin_transaction, lambda {
      if counter.zero?
        counter += 1
        raise(Rdkafka::RdkafkaError.new(-152, retryable: true))
      end
      original_method.call
    }) do
      @producer.transaction { nil }
    end
  end
end

class ProducerTransactionsWithoutExplicitTransactionTest < WaterDropTest::Base
  def setup
    @producer = build(:transactional_producer)
    @topic_name = "it-#{SecureRandom.uuid}"
  end

  def teardown
    @producer.close
    super
  end

  def test_allows_produce_sync_without_transaction
    @producer.produce_sync(topic: @topic_name, payload: rand.to_s)
  end

  def test_delivers_message_correctly_without_transaction
    result = @producer.produce_sync(topic: @topic_name, payload: rand.to_s)

    assert_equal @topic_name, result.topic_name
    assert_nil result.error
  end

  def test_async_dispatch_with_transaction_wrapper
    handler = @producer.produce_async(topic: @topic_name, payload: rand.to_s)
    result = handler.wait

    assert_equal @topic_name, result.topic_name
    assert_nil result.error
  end

  def test_produce_many_sync_wraps_with_single_transaction
    messages = Array.new(10) { build(:valid_message) }
    counts = []
    @producer.monitor.subscribe("transaction.committed") { counts << true }

    @producer.produce_many_sync(messages)

    assert_equal 1, counts.size
  end

  def test_produce_many_sync_error_after_few_messages_empty_dispatched
    producer = build(:transactional_producer, max_payload_size: 10 * 1_024 * 1_024)

    too_big = build(:valid_message)
    too_big[:payload] = "1" * 1024 * 1024

    messages = [
      Array.new(9) { build(:valid_message) },
      too_big
    ].flatten

    dispatched = []
    producer.monitor.subscribe("error.occurred") { |event| dispatched << event[:dispatched] }

    assert_raises(WaterDrop::Errors::ProduceManyError) do
      producer.produce_many_sync(messages)
    end

    assert_equal [], dispatched.flatten

    producer.close
  end

  def test_produce_many_async_wraps_with_single_transaction
    messages = Array.new(10) { build(:valid_message) }
    counts = []
    @producer.monitor.subscribe("transaction.committed") { counts << true }

    @producer.produce_many_async(messages)

    assert_equal 1, counts.size
  end
end

class ProducerTransactionsNestingTest < WaterDropTest::Base
  def setup
    @producer = build(:transactional_producer)
    @topic_name = "it-#{SecureRandom.uuid}"
    @counts = []
    @producer.monitor.subscribe("transaction.committed") { @counts << true }
  end

  def teardown
    @producer.close
    super
  end

  def test_nested_transactions_work
    handlers = []

    @producer.transaction do
      handlers << @producer.produce_async(topic: @topic_name, payload: "data")

      @producer.transaction do
        handlers << @producer.produce_async(topic: @topic_name, payload: "data")
      end
    end

    handlers.each { |handler| handler.wait }
  end

  def test_nested_transactions_count_as_one
    @producer.transaction do
      @producer.produce_async(topic: @topic_name, payload: "data")

      @producer.transaction do
        @producer.produce_async(topic: @topic_name, payload: "data")
      end
    end

    assert_equal 1, @counts.size
  end

  def test_abort_nested_transaction_aborts_all_levels
    producer = build(:transactional_producer, queue_buffering_max_ms: 5_000)
    handlers = []

    producer.transaction do
      handlers << producer.produce_async(topic: @topic_name, payload: "data")

      producer.transaction do
        handlers << producer.produce_async(topic: @topic_name, payload: "data")
        raise(WaterDrop::AbortTransaction)
      end
    end

    handlers.each do |handler|
      assert_raises(Rdkafka::RdkafkaError) { handler.wait }
    end

    producer.close
  end
end

class ProducerTransactionsMarkAsConsumedTest < WaterDropTest::Base
  def setup
    @producer = build(:transactional_producer)
    @topic_name = "it-#{SecureRandom.uuid}"
    @transactional_message_stub = Struct.new(:topic, :partition, :offset, keyword_init: true)
    @transactional_consumer_stub = Struct.new(
      :consumer_group_metadata_pointer, :dummy, keyword_init: true
    )
  end

  def teardown
    @producer.close
    super
  end

  def test_mark_as_consumed_without_transaction_raises
    message = @transactional_message_stub.new(topic: @topic_name, partition: 0, offset: 100)

    assert_raises(WaterDrop::Errors::TransactionRequiredError) do
      @producer.transaction_mark_as_consumed(nil, message)
    end
  end

  def test_mark_as_consumed_with_invalid_arguments_raises
    invalid_consumer_stub = Struct.new(:dummy, keyword_init: true)
    consumer = invalid_consumer_stub.new(dummy: nil)
    message = @transactional_message_stub.new(topic: @topic_name, partition: 0, offset: 100)

    @producer.transaction do
      assert_raises(WaterDrop::Errors::TransactionalOffsetInvalidError) do
        @producer.transaction_mark_as_consumed(consumer, message)
      end
    end
  end

  def test_mark_as_consumed_inside_transaction_delegates_correctly
    consumer = @transactional_consumer_stub.new(
      consumer_group_metadata_pointer: 1, dummy: nil
    )
    message = @transactional_message_stub.new(topic: @topic_name, partition: 0, offset: 100)

    send_called_with_timeout = nil

    @producer.client.stub(
      :send_offsets_to_transaction,
      lambda { |_consumer, _tpl, timeout|
        send_called_with_timeout = timeout
      }
    ) do
      @producer.transaction do
        @producer.transaction_mark_as_consumed(consumer, message)
      end
    end

    assert_equal 30_000, send_called_with_timeout
  end
end

class ProducerTransactionsDefaultConfigTest < WaterDropTest::Base
  def setup
    @topic_name = "it-#{SecureRandom.uuid}"
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

  def teardown
    @producer.close
    super
  end

  def test_can_send_message_with_default_config
    @producer.produce_async(topic: @topic_name, payload: "a")
  end
end

class ProducerTransactionsOutsideTransactionTest < WaterDropTest::Base
  def setup
    @producer = build(:transactional_producer)
  end

  def teardown
    @producer.close
    super
  end

  def test_not_inside_running_transaction
    assert_same false, @producer.transaction?
  end

  def test_inside_transaction_returns_true
    @producer.transaction do
      assert_same true, @producer.transaction?
    end
  end
end

class ProducerTransactionsEarlyBreakTest < WaterDropTest::Base
  def setup
    @producer = build(:transactional_producer)
    @topic_name = "it-#{SecureRandom.uuid}"
  end

  def teardown
    @producer.close
    super
  end

  def test_early_break_raises_error
    assert_raises(WaterDrop::Errors::EarlyTransactionExitNotAllowedError) do
      @producer.transaction { break }
    end
  end

  def test_early_break_cancels_dispatches
    handler = nil

    begin
      @producer.transaction do
        handler = @producer.produce_async(topic: @topic_name, payload: "na")
        break
      end
    rescue WaterDrop::Errors::EarlyTransactionExitNotAllowedError
      assert_raises(Rdkafka::RdkafkaError) { handler.wait }
    end
  end

  def test_early_break_does_not_affect_client_state
    begin
      @producer.transaction { break }
    rescue WaterDrop::Errors::EarlyTransactionExitNotAllowedError
      nil
    end

    handler = @producer.transaction do
      @producer.produce_async(topic: @topic_name, payload: "na")
    end

    handler.wait
  end
end

class ProducerTransactionsCriticalBrokerErrorsWithReloadTest < WaterDropTest::Base
  def setup
    @topic_name = "it-#{SecureRandom.uuid}"
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

  def teardown
    @producer.close
    super
  end

  def test_can_use_same_producer_after_error_when_async
    errored = false

    begin
      @producer.produce_async(topic: @topic_name, payload: "1" * 512)
    rescue WaterDrop::Errors::ProduceError
      errored = true
    end

    assert_same true, errored

    @producer.produce_async(topic: @topic_name, payload: "1")
  end

  def test_can_use_same_producer_after_error_when_sync
    errored = false

    begin
      @producer.produce_sync(topic: @topic_name, payload: "1" * 512)
    rescue WaterDrop::Errors::ProduceError
      errored = true
    end

    assert_same true, errored

    @producer.produce_sync(topic: @topic_name, payload: "1")
  end
end

class ProducerTransactionsEarlyReturnMethodTest < WaterDropTest::Base
  def setup
    @producer = build(:transactional_producer)
    @topic_name = "it-#{SecureRandom.uuid}"
  end

  def teardown
    @producer.close
    super
  end

  def test_wrapping_early_return_method_with_transaction
    t_name = @topic_name

    operation = Class.new do
      define_method :call do |producer, handlers|
        handlers << producer.produce_async(topic: "#{t_name}1", payload: "1")

        return unless handlers.empty?

        handlers << producer.produce_async(topic: "#{t_name}1", payload: "1")
      end
    end

    handlers = []

    @producer.transaction do
      operation.new.call(@producer, handlers)
    end

    handlers.map!(&:wait)
  end

  def test_wrapping_early_return_block_with_transaction
    topic_name = @topic_name

    operation = lambda do |producer, handlers|
      handlers << producer.produce_async(topic: "#{topic_name}1", payload: "1")

      return unless handlers.empty?

      handlers << producer.produce_async(topic: "#{topic_name}1", payload: "1")
    end

    handlers = []

    @producer.transaction do
      operation.call(@producer, handlers)
    end

    handlers.map!(&:wait)
  end
end

class ProducerTransactionsClosedProducerTest < WaterDropTest::Base
  def setup
    @producer = build(:transactional_producer)
    @producer.close
  end

  def test_closed_producer_cannot_start_transaction
    assert_raises(WaterDrop::Errors::ProducerClosedError) do
      @producer.transaction { nil }
    end
  end
end

class ProducerTransactionsFatalErrorWithReloadEnabledTest < WaterDropTest::Base
  def setup
    @producer = build(:transactional_producer, reload_on_transaction_fatal_error: true)
    @topic_name = "it-#{SecureRandom.uuid}"
  end

  def teardown
    @producer.close
    super
  end

  def test_reload_on_transaction_fatal_error_enabled_by_default
    assert_same true, @producer.config.reload_on_transaction_fatal_error
  end

  def test_correct_default_backoff_and_max_attempts_config
    assert_equal 1_000, @producer.config.wait_backoff_on_transaction_fatal_error
    assert_equal 10, @producer.config.max_attempts_on_transaction_fatal_error
  end

  def test_transactional_retryable_checks_retry_limit
    assert_same true, @producer.transactional_retryable?

    limited_producer = build(
      :transactional_producer,
      reload_on_transaction_fatal_error: true,
      max_attempts_on_transaction_fatal_error: 2
    )

    assert_same true, limited_producer.transactional_retryable?

    limited_producer.close
  end

  def test_successful_transaction_without_reload
    reloaded_events = []
    @producer.monitor.subscribe("producer.reloaded") { |event| reloaded_events << event }

    @producer.transaction do
      @producer.produce_sync(topic: @topic_name, payload: "test")
    end

    assert_empty reloaded_events
  end
end
