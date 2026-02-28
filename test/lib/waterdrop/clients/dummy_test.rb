# frozen_string_literal: true

class WaterDropClientsDummyTest < WaterDropTest::Base
  def setup
    @client = WaterDrop::Clients::Dummy.new(nil)
    @producer = WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    @producer.close
    super
  end

  def test_publish_sync_returns_self_for_chaining
    assert_equal @client, @client.publish_sync({})
  end

  def test_respond_to_returns_true_for_any_method
    assert_same true, @client.respond_to?(:test)
  end

  def test_queue_size_returns_zero
    assert_equal 0, @client.queue_size
  end

  def test_queue_length_returns_zero
    assert_equal 0, @client.queue_length
  end
end

class WaterDropClientsDummyProduceAsyncTest < WaterDropTest::Base
  def setup
    @producer = WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    @producer.close
    super
  end

  def test_produce_async_returns_handle
    handler = @producer.produce_async(topic: "test", partition: 2, payload: "1")

    assert_kind_of WaterDrop::Clients::Dummy::Handle, handler
  end

  def test_produce_async_wait_returns_correct_topic_name
    handler = @producer.produce_async(topic: "test", partition: 2, payload: "1")

    assert_equal "test", handler.wait.topic_name
  end

  def test_produce_async_wait_returns_correct_partition
    handler = @producer.produce_async(topic: "test", partition: 2, payload: "1")

    assert_equal 2, handler.wait.partition
  end

  def test_produce_async_wait_returns_offset_zero
    handler = @producer.produce_async(topic: "test", partition: 2, payload: "1")

    assert_equal 0, handler.wait.offset
  end

  def test_produce_async_multiple_dispatches_same_partition_offset_increments
    @producer.produce_async(topic: "test", partition: 2, payload: "1")
    handler = nil
    3.times { handler = @producer.produce_async(topic: "test", partition: 2, payload: "1") }

    assert_kind_of WaterDrop::Clients::Dummy::Handle, handler
    assert_equal "test", handler.wait.topic_name
    assert_equal 2, handler.wait.partition
    assert_equal 3, handler.wait.offset
  end

  def test_produce_async_multiple_dispatches_different_partitions
    @producer.produce_async(topic: "test", partition: 2, payload: "1")
    handler = nil
    3.times { |i| handler = @producer.produce_async(topic: "test", partition: i, payload: "1") }

    assert_kind_of WaterDrop::Clients::Dummy::Handle, handler
    assert_equal "test", handler.wait.topic_name
    assert_equal 2, handler.wait.partition
    assert_equal 1, handler.wait.offset
  end

  def test_produce_async_multiple_dispatches_different_topics
    handler = @producer.produce_async(topic: "test", partition: 2, payload: "1")
    3.times { |i| @producer.produce_async(topic: "test#{i}", partition: 0, payload: "1") }

    assert_kind_of WaterDrop::Clients::Dummy::Handle, handler
    assert_equal "test", handler.wait.topic_name
    assert_equal 2, handler.wait.partition
    assert_equal 0, handler.wait.offset
  end
end

class WaterDropClientsDummyProduceSyncTest < WaterDropTest::Base
  def setup
    @producer = WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    @producer.close
    super
  end

  def test_produce_sync_returns_delivery_report
    report = @producer.produce_sync(topic: "test", partition: 2, payload: "1")

    assert_kind_of Rdkafka::Producer::DeliveryReport, report
  end

  def test_produce_sync_returns_correct_topic_name
    report = @producer.produce_sync(topic: "test", partition: 2, payload: "1")

    assert_equal "test", report.topic_name
  end

  def test_produce_sync_returns_correct_partition
    report = @producer.produce_sync(topic: "test", partition: 2, payload: "1")

    assert_equal 2, report.partition
  end

  def test_produce_sync_returns_offset_zero
    report = @producer.produce_sync(topic: "test", partition: 2, payload: "1")

    assert_equal 0, report.offset
  end

  def test_produce_sync_multiple_dispatches_same_partition_offset_increments
    @producer.produce_sync(topic: "test", partition: 2, payload: "1")
    report = nil
    3.times { report = @producer.produce_sync(topic: "test", partition: 2, payload: "1") }

    assert_kind_of Rdkafka::Producer::DeliveryReport, report
    assert_equal "test", report.topic_name
    assert_equal 2, report.partition
    assert_equal 3, report.offset
  end

  def test_produce_sync_multiple_dispatches_different_partitions
    @producer.produce_sync(topic: "test", partition: 2, payload: "1")
    report = nil
    3.times { |i| report = @producer.produce_sync(topic: "test", partition: i, payload: "1") }

    assert_kind_of Rdkafka::Producer::DeliveryReport, report
    assert_equal "test", report.topic_name
    assert_equal 2, report.partition
    assert_equal 1, report.offset
  end

  def test_produce_sync_multiple_dispatches_different_topics
    report = @producer.produce_sync(topic: "test", partition: 2, payload: "1")
    3.times { |i| @producer.produce_sync(topic: "test#{i}", partition: 0, payload: "1") }

    assert_kind_of Rdkafka::Producer::DeliveryReport, report
    assert_equal "test", report.topic_name
    assert_equal 2, report.partition
    assert_equal 0, report.offset
  end
end

class WaterDropClientsDummyTransactionTest < WaterDropTest::Base
  def setup
    @producer = WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "transactional.id": SecureRandom.uuid
      }
    end
  end

  def teardown
    @producer.close
    super
  end

  def test_transaction_returns_block_value
    assert_equal 1, @producer.transaction { 1 }
  end

  def test_abort_transaction_does_not_raise
    @producer.transaction { raise(WaterDrop::AbortTransaction) }
  end

  def test_different_error_raises
    assert_raises(StandardError) do
      @producer.transaction { raise(StandardError) }
    end
  end

  def test_nested_transaction_works
    result = @producer.transaction do
      @producer.transaction do
        @producer.produce_sync(topic: "1", payload: "2")
        2
      end
    end

    assert_equal 2, result
  end
end
