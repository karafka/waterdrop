# frozen_string_literal: true

class WaterDropInstrumentationCallbacksDeliveryCallTest < WaterDropTest::Base
  def setup
    super
    @delivery_report_stub = Struct.new(
      :offset, :partition, :topic_name, :error, :label, keyword_init: true
    )
    @producer = build(:producer)
    @producer_id = SecureRandom.uuid
    @transactional = @producer.transactional?
    @monitor = WaterDrop::Instrumentation::Monitor.new
    @delivery_report = @delivery_report_stub.new(
      offset: rand(100),
      partition: rand(100),
      topic_name: rand(100).to_s,
      error: 0,
      label: nil
    )
    @changed = []

    @monitor.subscribe("message.acknowledged") do |event|
      @changed << event
    end

    @callback = WaterDrop::Instrumentation::Callbacks::Delivery.new(
      @producer_id, @transactional, @monitor
    )
    @callback.call(@delivery_report)
    @event = @changed.first
  end

  def teardown
    @producer.close
    super
  end

  def test_event_id
    assert_equal "message.acknowledged", @event.id
  end

  def test_event_producer_id
    assert_equal @producer_id, @event[:producer_id]
  end

  def test_event_offset
    assert_equal @delivery_report.offset, @event[:offset]
  end

  def test_event_partition
    assert_equal @delivery_report.partition, @event[:partition]
  end

  def test_event_topic
    assert_equal @delivery_report.topic_name, @event[:topic]
  end
end

class WaterDropInstrumentationCallbacksDeliveryCallErrorHandlingTest < WaterDropTest::Base
  def setup
    super
    @delivery_report_stub = Struct.new(
      :offset, :partition, :topic_name, :error, :label, keyword_init: true
    )
    @producer = build(:producer)
    @producer_id = SecureRandom.uuid
    @transactional = @producer.transactional?
    @monitor = WaterDrop::Instrumentation::Monitor.new
    @delivery_report = @delivery_report_stub.new(
      offset: rand(100),
      partition: rand(100),
      topic_name: rand(100).to_s,
      error: 0,
      label: nil
    )
    @tracked_errors = []

    @monitor.subscribe("message.acknowledged") do
      raise
    end

    local_errors = @tracked_errors

    @monitor.subscribe("error.occurred") do |event|
      local_errors << event
    end

    @callback = WaterDrop::Instrumentation::Callbacks::Delivery.new(
      @producer_id, @transactional, @monitor
    )
  end

  def teardown
    @producer.close
    super
  end

  def test_contains_notify_and_continue
    @callback.call(@delivery_report)

    assert_equal 1, @tracked_errors.size
    assert_equal "callbacks.delivery.error", @tracked_errors.first[:type]
  end
end

class WaterDropInstrumentationCallbacksDeliveryE2ESuccessTest < WaterDropTest::Base
  def setup
    super
    @changed = []
    @producer = build(:producer)
    @message = build(:valid_message)

    @producer.monitor.subscribe("message.acknowledged") do |event|
      @changed << event
    end

    @producer.produce_sync(@message)
    sleep(0.01) until @changed.size.positive?

    @event = @changed.first
  end

  def teardown
    @producer.close
    super
  end

  def test_event_partition
    assert_equal 0, @event.payload[:partition]
  end

  def test_event_offset
    assert_equal 0, @event.payload[:offset]
  end

  def test_event_topic
    assert_equal @message[:topic], @event[:topic]
  end
end

class WaterDropInstrumentationCallbacksDeliveryE2EAsyncFailureTest < WaterDropTest::Base
  def setup
    super
    @changed = []
    @producer = build(:producer)

    @producer.monitor.subscribe("error.occurred") do |event|
      @changed << event
    end

    100.times do
      @producer.send(:client).produce(topic: "$%^&*", payload: "1")
    rescue Rdkafka::RdkafkaError
      nil
    end

    sleep(0.01) until @changed.size.positive?

    @event = @changed.last
  end

  def teardown
    @producer.close
    super
  end

  def test_event_error_is_rdkafka_error
    assert_kind_of Rdkafka::RdkafkaError, @event.payload[:error]
  end

  def test_event_partition
    assert_equal(-1, @event.payload[:partition])
  end

  def test_event_offset
    assert_equal(-1001, @event.payload[:offset])
  end

  def test_event_topic
    assert_equal "$%^&*", @event.payload[:topic]
  end
end

class WaterDropInstrumentationCallbacksDeliveryE2ESyncFailureTest < WaterDropTest::Base
  def setup
    super
    @changed = []
    @producer = build(:producer)

    @producer.monitor.subscribe("error.occurred") do |event|
      @changed << event
    end

    begin
      @producer.send(:client).produce(topic: "$%^&*", payload: "1").wait
    rescue Rdkafka::RdkafkaError
      nil
    end

    sleep(0.01) until @changed.size.positive?

    @event = @changed.first
  end

  def teardown
    @producer.close
    super
  end

  def test_event_error_is_rdkafka_error
    assert_kind_of Rdkafka::RdkafkaError, @event.payload[:error]
  end

  def test_event_partition
    assert_equal(-1, @event.payload[:partition])
  end

  def test_event_offset
    assert_equal(-1001, @event.payload[:offset])
  end
end

class WaterDropInstrumentationCallbacksDeliveryE2EInlineErrorTest < WaterDropTest::Base
  def setup
    super
    @changed = []
    @errors = []
    @producer = build(:limited_producer)

    @producer.monitor.subscribe("error.occurred") do |event|
      @changed << event
    end

    begin
      msg = build(:valid_message)
      100.times { @producer.produce_async(msg) }
    rescue WaterDrop::Errors::ProduceError => e
      @errors << e
    end

    sleep(0.01) until @changed.size.positive?

    @event = @changed.first
  end

  def teardown
    @producer.close
    super
  end

  def test_errors_first_is_produce_error
    assert_kind_of WaterDrop::Errors::ProduceError, @errors.first
  end

  def test_errors_first_cause_is_rdkafka_error
    assert_kind_of Rdkafka::RdkafkaError, @errors.first.cause
  end

  def test_event_error_is_produce_error
    assert_kind_of WaterDrop::Errors::ProduceError, @event[:error]
  end

  def test_event_error_cause_is_rdkafka_error
    assert_kind_of Rdkafka::RdkafkaError, @event[:error].cause
  end
end

class WaterDropInstrumentationCallbacksDeliveryE2ENonTransactionalPurgeTest < WaterDropTest::Base
  def setup
    super
    @producer = build(:slow_producer)
    @errors = []
    @purges = []

    @producer.monitor.subscribe("error.occurred") do |event|
      @errors << event[:error]
    end

    @producer.monitor.subscribe("message.purged") do |event|
      @purges << event[:error]
    end

    @producer.produce_async(build(:valid_message))
    @producer.purge

    sleep(0.01) until @errors.size.positive?
  end

  def teardown
    @producer.close
    super
  end

  def test_has_error_in_errors
    assert_kind_of Rdkafka::RdkafkaError, @errors.first
    assert_equal :purge_queue, @errors.first.code
  end

  def test_does_not_publish_purge_notification
    assert_empty @purges
  end
end
