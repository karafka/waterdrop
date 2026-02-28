# frozen_string_literal: true

class ProducerAsyncProduceAsyncTest < WaterDropTest::Base
  def setup
    @producer = build(:producer)
  end

  def teardown
    @producer.close
    super
  end

  def test_produce_async_with_invalid_message
    message = build(:invalid_message)

    assert_raises(WaterDrop::Errors::MessageInvalidError) do
      @producer.produce_async(message)
    end
  end

  def test_produce_async_with_valid_message
    message = build(:valid_message)
    delivery = @producer.produce_async(message)

    assert_kind_of Rdkafka::Producer::DeliveryHandle, delivery
  end

  def test_produce_async_with_valid_message_and_array_headers
    message = build(:valid_message, headers: { "a" => %w[b c] })
    delivery = @producer.produce_async(message)

    assert_kind_of Rdkafka::Producer::DeliveryHandle, delivery
  end

  def test_produce_async_with_valid_message_and_label
    message = build(:valid_message, label: "test")
    delivery = @producer.produce_async(message)

    assert_equal "test", delivery.label
  end

  def test_produce_async_with_tombstone_message
    message = build(:valid_message, payload: nil)
    delivery = @producer.produce_async(message)

    assert_kind_of Rdkafka::Producer::DeliveryHandle, delivery
  end

  def test_produce_async_with_good_middleware
    message = build(:valid_message, payload: nil)
    @producer.produce_sync(topic: message[:topic], payload: nil)

    mid = lambda do |msg|
      msg[:partition_key] = "1"
      msg
    end

    @producer.middleware.append mid
    delivery = @producer.produce_async(message)

    assert_kind_of Rdkafka::Producer::DeliveryHandle, delivery
  end

  def test_produce_async_with_corrupted_middleware
    message = build(:valid_message, payload: nil)

    mid = lambda do |msg|
      msg[:partition_key] = -1
      msg
    end

    @producer.middleware.append mid

    assert_raises(WaterDrop::Errors::MessageInvalidError) do
      @producer.produce_async(message)
    end
  end
end

class ProducerAsyncInlineErrorNoRetryTest < WaterDropTest::Base
  def setup
    @errors = []
    @occurred = []
    @producer = build(:limited_producer)

    @producer.monitor.subscribe("error.occurred") do |event|
      event.payload[:error] = event[:error].dup
      @occurred << event
    end

    begin
      message = build(:valid_message)
      100.times { @producer.produce_async(message) }
    rescue WaterDrop::Errors::ProduceError => e
      @errors << e
    end
  end

  def teardown
    @producer.close
    super
  end

  def test_error_is_produce_error
    assert_kind_of WaterDrop::Errors::ProduceError, @errors.first
  end

  def test_error_message_matches_cause_inspect
    error = @errors.first

    assert_equal error.cause.inspect, error.message
  end

  def test_error_cause_is_rdkafka_error
    assert_kind_of Rdkafka::RdkafkaError, @errors.first.cause
  end

  def test_occurred_error_cause_is_rdkafka_error
    assert_kind_of Rdkafka::RdkafkaError, @occurred.first.payload[:error].cause
  end

  def test_occurred_type_is_produce_async
    assert_equal "message.produce_async", @occurred.first.payload[:type]
  end
end

class ProducerAsyncInlineErrorRetryOnQueueFullTest < WaterDropTest::Base
  def setup
    @errors = []
    @occurred = []
    @producer = build(:slow_producer, wait_on_queue_full: true)
    @producer.config.wait_on_queue_full = true

    @producer.monitor.subscribe("error.occurred") do |event|
      @occurred << event
    end

    begin
      message = build(:valid_message, label: "test")
      5.times { @producer.produce_async(message) }
    rescue WaterDrop::Errors::ProduceError => e
      @errors << e
    end
  end

  def teardown
    @producer.close
    super
  end

  def test_no_errors_raised
    assert_empty @errors
  end

  def test_occurred_error_cause_is_rdkafka_error
    assert_kind_of Rdkafka::RdkafkaError, @occurred.first.payload[:error].cause
  end

  def test_occurred_type_is_produce_async
    assert_equal "message.produce_async", @occurred.first.payload[:type]
  end

  def test_occurred_label_is_nil
    assert_nil @occurred.first.payload[:label]
  end
end

class ProducerAsyncLingerLongerThanShutdownTest < WaterDropTest::Base
  def setup
    @occurred = []

    while @occurred.empty?
      producer = build(
        :slow_producer,
        kafka: {
          "bootstrap.servers": BOOTSTRAP_SERVERS,
          "queue.buffering.max.ms": 0,
          "message.timeout.ms": 1
        }
      )

      producer.monitor.subscribe("error.occurred") do |event|
        @occurred << event
      end

      message = build(:valid_message, label: "test")
      100.times { producer.produce_async(message) }
      producer.close
    end

    @error = @occurred.first[:error]
  end

  def test_occurred_is_not_empty
    refute_empty @occurred
  end

  def test_error_is_rdkafka_error
    assert_kind_of Rdkafka::RdkafkaError, @error
  end

  def test_error_code_is_msg_timed_out_or_transport
    assert_includes %i[msg_timed_out transport], @error.code
  end
end

class ProducerAsyncInlineErrorRetryNoInstrumentationTest < WaterDropTest::Base
  def setup
    @errors = []
    @occurred = []
    @producer = build(:slow_producer, wait_on_queue_full: true)
    @producer.config.wait_on_queue_full = true
    @producer.config.instrument_on_wait_queue_full = false

    @producer.monitor.subscribe("error.occurred") do |event|
      @occurred << event
    end

    begin
      message = build(:valid_message, label: "test")
      5.times { @producer.produce_async(message) }
    rescue WaterDrop::Errors::ProduceError => e
      @errors << e
    end
  end

  def teardown
    @producer.close
    super
  end

  def test_no_errors_raised
    assert_empty @errors
  end

  def test_no_occurred_events
    assert_empty @occurred
  end
end

class ProducerAsyncInlineErrorBeyondMaxWaitTest < WaterDropTest::Base
  def setup
    @errors = []
    @occurred = []
    @producer = build(
      :slow_producer,
      wait_on_queue_full: true,
      wait_timeout_on_queue_full: 0.5
    )
    @producer.config.wait_on_queue_full = true

    @producer.monitor.subscribe("error.occurred") do |event|
      @occurred << event
    end

    begin
      message = build(:valid_message, label: "test")
      5.times { @producer.produce_async(message) }
    rescue WaterDrop::Errors::ProduceError => e
      @errors << e
    end
  end

  def teardown
    @producer.close
    super
  end

  def test_errors_present
    refute_empty @errors
  end

  def test_occurred_error_cause_is_rdkafka_error
    assert_kind_of Rdkafka::RdkafkaError, @occurred.first.payload[:error].cause
  end

  def test_occurred_type_is_produce_async
    assert_equal "message.produce_async", @occurred.first.payload[:type]
  end

  def test_occurred_label_is_nil
    assert_nil @occurred.first.payload[:label]
  end
end

class ProducerProduceManyAsyncTest < WaterDropTest::Base
  def setup
    @producer = build(:producer)
  end

  def teardown
    @producer.close
    super
  end

  def test_produce_many_async_with_several_invalid_messages
    messages = Array.new(10) { build(:invalid_message) }

    assert_raises(WaterDrop::Errors::MessageInvalidError) do
      @producer.produce_many_async(messages)
    end
  end

  def test_produce_many_async_last_message_invalid_raises
    messages = [build(:valid_message), build(:invalid_message)]

    assert_raises(WaterDrop::Errors::MessageInvalidError) do
      @producer.produce_many_async(messages)
    end
  end

  def test_produce_many_async_with_valid_messages
    messages = Array.new(10) { build(:valid_message) }
    delivery = @producer.produce_many_async(messages)

    delivery.each { |d| assert_kind_of Rdkafka::Producer::DeliveryHandle, d }
  end
end

class ProducerProduceManyAsyncInlineErrorTest < WaterDropTest::Base
  def setup
    @errors = []
    @producer = build(:limited_producer)

    messages = Array.new(100) { build(:valid_message) }

    begin
      @producer.produce_many_async(messages)
    rescue WaterDrop::Errors::ProduceError => e
      @errors << e
    end
  end

  def teardown
    @producer.close
    super
  end

  def test_dispatched_size_is_one
    assert_equal 1, @errors.first.dispatched.size
  end

  def test_dispatched_first_is_delivery_handle
    assert_kind_of Rdkafka::Producer::DeliveryHandle, @errors.first.dispatched.first
  end

  def test_error_is_produce_error
    assert_kind_of WaterDrop::Errors::ProduceError, @errors.first
  end

  def test_error_message_matches_cause_inspect
    error = @errors.first

    assert_equal error.cause.inspect, error.message
  end

  def test_error_cause_is_rdkafka_error
    assert_kind_of Rdkafka::RdkafkaError, @errors.first.cause
  end
end

class ProducerProduceManyAsyncDispatchedNotInKafkaTest < WaterDropTest::Base
  def setup
    @producer = build(
      :slow_producer,
      kafka: {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "queue.buffering.max.ms": 5_000,
        "queue.buffering.max.messages": 2_000
      }
    )
  end

  def teardown
    @producer.close
    super
  end

  def test_not_allow_disconnect
    assert_same false, @producer.disconnect
  end

  def test_allow_disconnect_after_dispatched
    message = build(:valid_message, label: "test")
    dispatched = Array.new(1_000) { @producer.produce_async(message) }
    dispatched.each(&:wait)

    assert_same true, @producer.disconnect
  end
end

class ProducerAsyncFatalErrorProduceAsyncTest < WaterDropTest::Base
  def setup
    @producer = build(
      :idempotent_producer,
      reload_on_idempotent_fatal_error: true,
      max_attempts_on_idempotent_fatal_error: 3,
      wait_backoff_on_idempotent_fatal_error: 100
    )
    @topic_name = "it-#{SecureRandom.uuid}"
    @message = build(:valid_message, topic: @topic_name)
    @producer.singleton_class.include(WaterDrop::Producer::Testing)
  end

  def teardown
    @producer.close
    super
  end

  def test_detects_fatal_error_and_recovers_via_reload_on_produce_async
    handle = @producer.produce_async(@message)

    assert_kind_of Rdkafka::Producer::DeliveryHandle, handle
    report = handle.wait

    assert_nil report.error

    @producer.trigger_test_fatal_error(47, "Fatal error for produce_async test")

    fatal_error = @producer.fatal_error

    refute_nil fatal_error
    assert_equal 47, fatal_error[:error_code]

    handle = @producer.produce_async(@message)

    assert_kind_of Rdkafka::Producer::DeliveryHandle, handle
    report = handle.wait

    assert_nil report.error
  end

  def test_produce_async_before_fatal_error_injection
    handles = []

    5.times do
      handle = @producer.produce_async(@message)

      assert_kind_of Rdkafka::Producer::DeliveryHandle, handle
      handles << handle
    end

    handles.each do |handle|
      report = handle.wait

      assert_nil report.error
    end

    assert_nil @producer.fatal_error
  end

  def test_maintains_fatal_error_state_across_multiple_queries
    @producer.trigger_test_fatal_error(64, "Async fatal error state test")

    first = @producer.fatal_error
    second = @producer.fatal_error
    third = @producer.fatal_error

    assert_equal first, second
    assert_equal second, third
    assert_equal 64, first[:error_code]
  end
end

class ProducerAsyncFatalErrorProduceManyAsyncTest < WaterDropTest::Base
  def setup
    @producer = build(
      :idempotent_producer,
      reload_on_idempotent_fatal_error: true,
      max_attempts_on_idempotent_fatal_error: 3,
      wait_backoff_on_idempotent_fatal_error: 100
    )
    @topic_name = "it-#{SecureRandom.uuid}"
    @messages = Array.new(3) { build(:valid_message, topic: @topic_name) }
    @producer.singleton_class.include(WaterDrop::Producer::Testing)
  end

  def teardown
    @producer.close
    super
  end

  def test_detects_fatal_error_and_recovers_via_reload_on_produce_many_async
    handles = @producer.produce_many_async(@messages)

    assert_kind_of Array, handles
    assert_equal 3, handles.size

    reports = handles.map(&:wait)

    reports.each { |report| assert_nil report.error }

    @producer.trigger_test_fatal_error(47, "Fatal error for produce_many_async test")

    fatal_error = @producer.fatal_error

    refute_nil fatal_error
    assert_equal 47, fatal_error[:error_code]

    handles = @producer.produce_many_async(@messages)

    assert_kind_of Array, handles
    assert_equal 3, handles.size

    reports = handles.map(&:wait)

    reports.each { |report| assert_nil report.error }
  end

  def test_produce_async_batches_before_fatal_error_injection
    all_handles = []

    3.times do
      handles = @producer.produce_many_async(@messages)

      assert_equal 3, handles.size
      all_handles.concat(handles)
    end

    all_handles.each do |handle|
      report = handle.wait

      assert_nil report.error
    end

    assert_nil @producer.fatal_error
  end

  def test_handles_different_batch_sizes_before_fatal_error
    small_batch = [build(:valid_message, topic: @topic_name)]
    handles = @producer.produce_many_async(small_batch)

    assert_equal 1, handles.size
    handles.each { |h| assert_nil h.wait.error }

    medium_batch = Array.new(5) { build(:valid_message, topic: @topic_name) }
    handles = @producer.produce_many_async(medium_batch)

    assert_equal 5, handles.size
    handles.each { |h| assert_nil h.wait.error }

    assert_nil @producer.fatal_error
  end
end

class ProducerAsyncFatalErrorWithoutReloadTest < WaterDropTest::Base
  def setup
    @producer = build(
      :idempotent_producer,
      reload_on_idempotent_fatal_error: false
    )
    @topic_name = "it-#{SecureRandom.uuid}"
    @message = build(:valid_message, topic: @topic_name)
    @messages = Array.new(3) { build(:valid_message, topic: @topic_name) }
    @reload_events = []
    @reloaded_events = []

    @producer.singleton_class.include(WaterDrop::Producer::Testing)
    @producer.monitor.subscribe("producer.reload") { |event| @reload_events << event }
    @producer.monitor.subscribe("producer.reloaded") { |event| @reloaded_events << event }
  end

  def teardown
    @producer.close
    super
  end

  def test_produce_async_raises_error_consistently_without_reload
    @producer.trigger_test_fatal_error(47, "No reload async test")

    3.times do
      error = assert_raises(WaterDrop::Errors::ProduceError) do
        @producer.produce_async(@message)
      end

      assert_kind_of Rdkafka::RdkafkaError, error.cause
      assert_same true, error.cause.fatal?

      refute_nil @producer.fatal_error
      assert_equal 47, @producer.fatal_error[:error_code]
    end

    assert_empty @reload_events
    assert_empty @reloaded_events
  end

  def test_produce_many_async_raises_error_consistently_without_reload
    @producer.trigger_test_fatal_error(47, "No reload async batch test")

    3.times do
      error = assert_raises(WaterDrop::Errors::ProduceError) do
        @producer.produce_many_async(@messages)
      end

      assert_kind_of Rdkafka::RdkafkaError, error.cause
      assert_same true, error.cause.fatal?

      refute_nil @producer.fatal_error
      assert_equal 47, @producer.fatal_error[:error_code]
    end

    assert_empty @reload_events
    assert_empty @reloaded_events
  end
end
