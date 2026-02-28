# frozen_string_literal: true

class ProducerSyncProduceSyncTest < WaterDropTest::Base
  def setup
    @producer = build(:producer)
    @topic_name = "it-#{SecureRandom.uuid}"
  end

  def teardown
    @producer.close
    super
  end

  def test_produce_sync_with_invalid_message
    message = build(:invalid_message)

    assert_raises(WaterDrop::Errors::MessageInvalidError) do
      @producer.produce_sync(message)
    end
  end

  def test_produce_sync_with_valid_message
    message = build(:valid_message)
    delivery = @producer.produce_sync(message)

    assert_kind_of Rdkafka::Producer::DeliveryReport, delivery
  end

  def test_produce_sync_with_array_headers
    message = build(:valid_message, headers: { "a" => %w[b c] })
    delivery = @producer.produce_sync(message)

    assert_kind_of Rdkafka::Producer::DeliveryReport, delivery
  end

  def test_produce_sync_with_invalid_headers
    message = build(:valid_message, headers: { "a" => %i[b c] })

    assert_raises(WaterDrop::Errors::MessageInvalidError) do
      @producer.produce_sync(message)
    end
  end

  def test_produce_sync_with_label
    message = build(:valid_message, label: "test")
    delivery = @producer.produce_sync(message)

    assert_equal "test", delivery.label
  end

  def test_produce_sync_with_topic_as_symbol
    message = build(:valid_message)
    message[:topic] = message[:topic].to_sym
    delivery = @producer.produce_sync(message)

    assert_kind_of Rdkafka::Producer::DeliveryReport, delivery
  end

  def test_produce_sync_to_unreachable_cluster
    producer = build(:unreachable_producer)
    message = build(:valid_message)

    assert_raises(WaterDrop::Errors::ProduceError) do
      producer.produce_sync(message)
    end
    producer.close
  end

  def test_produce_sync_with_partition_key_to_nonexistent_topic
    message = build(:valid_message, partition_key: "test", key: "test")
    delivery = @producer.produce_sync(message)

    assert_kind_of Rdkafka::Producer::DeliveryReport, delivery
  end

  def test_produce_sync_with_auto_create_topics_false
    producer = build(
      :producer,
      kafka: {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "allow.auto.create.topics": false,
        "message.timeout.ms": 500
      }
    )
    message = build(:valid_message)

    assert_raises(WaterDrop::Errors::ProduceError) do
      producer.produce_sync(message)
    end
    producer.close
  end

  def test_produce_sync_with_auto_create_topics_false_and_partition_key
    producer = build(
      :producer,
      kafka: {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "allow.auto.create.topics": false,
        "message.timeout.ms": 500
      }
    )
    message = build(:valid_message, partition_key: "test", key: "test")

    assert_raises(WaterDrop::Errors::ProduceError) do
      producer.produce_sync(message)
    end
    producer.close
  end
end

class ProducerSyncInlineErrorTest < WaterDropTest::Base
  def setup
    @errors = []
    @occurred = []
    @producer = build(:limited_producer)

    @producer.monitor.subscribe("error.occurred") do |event|
      event.payload[:error] = event[:error].dup
      @occurred << event
    end

    message = build(:valid_message, label: "test")
    threads = Array.new(20) do
      Thread.new do
        @producer.produce_sync(message)
      rescue => e
        @errors << e
      end
    end

    threads.each(&:join)
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

  def test_occurred_type_is_produce_sync
    assert_equal "message.produce_sync", @occurred.first.payload[:type]
  end

  def test_occurred_label_is_nil
    assert_nil @occurred.first.payload[:label]
  end
end

class ProducerSyncWithPartitionKeyTest < WaterDropTest::Base
  def setup
    @producer = build(:producer)
    @topic_name = "it-#{SecureRandom.uuid}"
    @producer.produce_sync(topic: @topic_name, payload: "1")
  end

  def teardown
    @producer.close
    super
  end

  def test_produce_sync_with_partition_key
    message = build(:valid_message, partition_key: rand.to_s, topic: @topic_name)
    delivery = @producer.produce_sync(message)

    assert_kind_of Rdkafka::Producer::DeliveryReport, delivery
  end
end

class ProducerProduceManySyncTest < WaterDropTest::Base
  def setup
    @producer = build(:producer)
    @topic_name = "it-#{SecureRandom.uuid}"
  end

  def teardown
    @producer.close
    super
  end

  def test_produce_many_sync_with_several_invalid_messages
    messages = Array.new(10) { build(:invalid_message) }

    assert_raises(WaterDrop::Errors::MessageInvalidError) do
      @producer.produce_many_sync(messages)
    end
  end

  def test_produce_many_sync_last_message_invalid_raises
    messages = [build(:valid_message), build(:invalid_message)]

    assert_raises(WaterDrop::Errors::MessageInvalidError) do
      @producer.produce_many_sync(messages)
    end
  end

  def test_produce_many_sync_with_valid_messages
    messages = Array.new(10) { build(:valid_message) }
    delivery = @producer.produce_many_sync(messages)

    delivery.each { |d| assert_kind_of Rdkafka::Producer::DeliveryHandle, d }
  end

  def test_produce_many_sync_with_valid_messages_and_array_headers
    messages = Array.new(10) { build(:valid_message, headers: { "a" => %w[b c] }) }
    delivery = @producer.produce_many_sync(messages)

    delivery.each { |d| assert_kind_of Rdkafka::Producer::DeliveryHandle, d }
  end

  def test_produce_many_sync_to_multiple_topics_with_invalid_partition_key
    topic1 = @topic_name
    topic2 = "#{topic1}-2"

    messages = [
      { topic: topic1, payload: "message1", partition: 0 },
      { topic: topic1, payload: "message2", partition: 0 },
      { topic: topic2, payload: "message3", partition: 1 },
      { topic: topic2, payload: "message4", partition: 0 },
      { topic: topic2, payload: "message5", partition: 0 }
    ]

    @producer.produce_sync(topic: topic1, payload: "setup1", partition: 0)
    @producer.produce_sync(topic: topic2, payload: "setup2", partition: 0)

    assert_raises(WaterDrop::Errors::ProduceManyError) do
      @producer.produce_many_sync(messages)
    end
  end
end

class ProducerSyncCompressionCodecTest < WaterDropTest::Base
  def teardown
    @producer.close
    super
  end

  def test_gzip_compression
    @producer = build(
      :producer,
      kafka: { "bootstrap.servers": BOOTSTRAP_SERVERS, "compression.codec": "gzip" }
    )
    message = build(:valid_message)

    assert_kind_of Rdkafka::Producer::DeliveryReport, @producer.produce_sync(message)
  end

  def test_zstd_compression
    @producer = build(
      :producer,
      kafka: { "bootstrap.servers": BOOTSTRAP_SERVERS, "compression.codec": "zstd" }
    )
    message = build(:valid_message)

    assert_kind_of Rdkafka::Producer::DeliveryReport, @producer.produce_sync(message)
  end

  def test_lz4_compression
    @producer = build(
      :producer,
      kafka: { "bootstrap.servers": BOOTSTRAP_SERVERS, "compression.codec": "lz4" }
    )
    message = build(:valid_message)

    assert_kind_of Rdkafka::Producer::DeliveryReport, @producer.produce_sync(message)
  end

  def test_snappy_compression
    @producer = build(
      :producer,
      kafka: { "bootstrap.servers": BOOTSTRAP_SERVERS, "compression.codec": "snappy" }
    )
    message = build(:valid_message)

    assert_kind_of Rdkafka::Producer::DeliveryReport, @producer.produce_sync(message)
  end
end

class ProducerSyncDisconnectReconnectTest < WaterDropTest::Base
  def setup
    @producer = build(:producer)
  end

  def teardown
    @producer.close
    super
  end

  def test_disconnect_and_reconnect_loop
    message = build(:valid_message)

    100.times do |i|
      assert_equal i, @producer.produce_sync(message).offset
      @producer.disconnect
    end
  end
end

class ProducerSyncFatalErrorProduceSyncTest < WaterDropTest::Base
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

  def test_detects_fatal_error_and_recovers_via_reload_on_produce_sync
    report = @producer.produce_sync(@message)

    assert_kind_of Rdkafka::Producer::DeliveryReport, report
    assert_nil report.error

    @producer.trigger_test_fatal_error(47, "Fatal error for produce_sync test")

    fatal_error = @producer.fatal_error

    refute_nil fatal_error
    assert_equal 47, fatal_error[:error_code]

    report = @producer.produce_sync(@message)

    assert_kind_of Rdkafka::Producer::DeliveryReport, report
    assert_nil report.error
  end

  def test_produce_successfully_before_fatal_error_injection
    5.times do
      report = @producer.produce_sync(@message)

      assert_kind_of Rdkafka::Producer::DeliveryReport, report
      assert_nil report.error
    end

    assert_nil @producer.fatal_error
  end

  def test_multiple_produce_sync_calls_succeed_after_fatal_error
    @producer.trigger_test_fatal_error(47, "Multiple calls test")

    3.times do
      report = @producer.produce_sync(@message)

      assert_kind_of Rdkafka::Producer::DeliveryReport, report
      assert_nil report.error
    end
  end
end

class ProducerSyncFatalErrorProduceManySyncTest < WaterDropTest::Base
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

  def test_detects_fatal_error_and_recovers_via_reload_on_produce_many_sync
    handles = @producer.produce_many_sync(@messages)

    assert_kind_of Array, handles
    assert_equal 3, handles.size
    handles.each { |h| assert_kind_of Rdkafka::Producer::DeliveryHandle, h }

    @producer.trigger_test_fatal_error(47, "Fatal error for produce_many_sync test")

    fatal_error = @producer.fatal_error

    refute_nil fatal_error
    assert_equal 47, fatal_error[:error_code]

    handles = @producer.produce_many_sync(@messages)

    assert_kind_of Array, handles
    assert_equal 3, handles.size
    handles.each { |h| assert_kind_of Rdkafka::Producer::DeliveryHandle, h }
  end

  def test_produce_batches_before_fatal_error_injection
    3.times do
      handles = @producer.produce_many_sync(@messages)

      assert_equal 3, handles.size
      handles.each { |h| assert_kind_of Rdkafka::Producer::DeliveryHandle, h }
    end

    assert_nil @producer.fatal_error
  end

  def test_different_batch_sizes_before_fatal_error
    small_batch = [build(:valid_message, topic: @topic_name)]
    reports = @producer.produce_many_sync(small_batch)

    assert_equal 1, reports.size

    medium_batch = Array.new(5) { build(:valid_message, topic: @topic_name) }
    reports = @producer.produce_many_sync(medium_batch)

    assert_equal 5, reports.size

    large_batch = Array.new(10) { build(:valid_message, topic: @topic_name) }
    reports = @producer.produce_many_sync(large_batch)

    assert_equal 10, reports.size

    assert_nil @producer.fatal_error
  end
end

class ProducerSyncFatalErrorWithoutReloadTest < WaterDropTest::Base
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

  def test_produce_sync_raises_error_consistently_without_reload
    @producer.trigger_test_fatal_error(47, "No reload test")

    3.times do
      error = assert_raises(WaterDrop::Errors::ProduceError) do
        @producer.produce_sync(@message)
      end

      assert_kind_of Rdkafka::RdkafkaError, error.cause
      assert_same true, error.cause.fatal?

      refute_nil @producer.fatal_error
      assert_equal 47, @producer.fatal_error[:error_code]
    end

    assert_empty @reload_events
    assert_empty @reloaded_events
  end

  def test_produce_many_sync_raises_error_consistently_without_reload
    @producer.trigger_test_fatal_error(47, "No reload batch test")

    3.times do
      error = assert_raises(WaterDrop::Errors::ProduceManyError) do
        @producer.produce_many_sync(@messages)
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
