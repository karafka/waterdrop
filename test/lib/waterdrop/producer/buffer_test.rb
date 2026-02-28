# frozen_string_literal: true

class ProducerBufferTest < WaterDropTest::Base
  def setup
    @producer = build(:producer)
    @invalid_error = WaterDrop::Errors::MessageInvalidError
  end

  def teardown
    @producer.purge
    @producer.close
    super
  end

  # buffer tests

  def test_buffer_when_producer_is_closed
    @producer.close
    message = build(:valid_message)

    assert_raises(WaterDrop::Errors::ProducerClosedError) do
      @producer.buffer(message)
    end
  end

  def test_buffer_with_invalid_message_does_not_raise
    message = build(:invalid_message)
    @producer.buffer(message)
  end

  def test_buffer_with_invalid_message_raises_on_flush
    message = build(:invalid_message)
    @producer.buffer(message)

    assert_raises(@invalid_error) do
      @producer.flush_async
    end
  end

  def test_buffer_with_valid_message
    message = build(:valid_message)
    result = @producer.buffer(message)

    assert_includes result, message
  end

  def test_buffer_middleware_runs_only_once
    message = build(:valid_message)

    middleware = lambda do |msg|
      msg[:payload] += "test "
      msg
    end

    @producer.middleware.append(middleware)
    @producer.buffer(message)
    @producer.flush_async

    assert_equal 1, message[:payload].scan("test").size
  end

  # buffer_many tests

  def test_buffer_many_when_producer_is_closed
    @producer.close
    messages = [build(:valid_message)]

    assert_raises(WaterDrop::Errors::ProducerClosedError) do
      @producer.buffer_many(messages)
    end
  end

  def test_buffer_many_with_invalid_messages_does_not_raise
    messages = Array.new(10) { build(:invalid_message) }
    @producer.buffer_many(messages)
  end

  def test_buffer_many_with_invalid_messages_raises_on_flush
    messages = Array.new(10) { build(:invalid_message) }
    @producer.buffer_many(messages)

    assert_raises(@invalid_error) do
      @producer.flush_async
    end
  end

  def test_buffer_many_last_message_invalid_does_not_raise
    messages = [build(:valid_message), build(:invalid_message)]
    @producer.buffer_many(messages)
  end

  def test_buffer_many_last_message_invalid_raises_on_flush
    messages = [build(:valid_message), build(:invalid_message)]
    @producer.buffer_many(messages)

    assert_raises(@invalid_error) do
      @producer.flush_async
    end
  end

  def test_buffer_many_with_valid_messages
    messages = Array.new(10) { build(:valid_message) }
    result = @producer.buffer_many(messages)

    assert_equal messages, result
  end

  def test_buffer_many_middleware_runs_only_once
    message = build(:valid_message)

    middleware = lambda do |msg|
      msg[:payload] += "test "
      msg
    end

    @producer.middleware.append(middleware)
    @producer.buffer_many([message])
    @producer.flush_async

    assert_equal 1, message[:payload].scan("test").size
  end

  # flush_async tests

  def test_flush_async_with_no_messages
    assert_equal [], @producer.flush_async
  end

  def test_flush_async_with_messages
    @producer.buffer(build(:valid_message))
    result = @producer.flush_async

    assert_kind_of Rdkafka::Producer::DeliveryHandle, result[0]
  end

  def test_flush_async_empties_buffer
    @producer.buffer(build(:valid_message))
    @producer.flush_async

    assert_empty @producer.messages
  end

  def test_flush_async_with_error_during_flushing
    error = Rdkafka::RdkafkaError.new(0)

    @producer.client.stub(:produce, ->(*) { raise error }) do
      @producer.buffer(build(:valid_message))

      assert_raises(WaterDrop::Errors::ProduceManyError) do
        @producer.flush_async
      end
    end
  end

  # flush_sync tests

  def test_flush_sync_with_no_messages
    assert_equal [], @producer.flush_sync
  end

  def test_flush_sync_with_messages
    @producer.buffer(build(:valid_message))
    result = @producer.flush_sync

    assert_kind_of Rdkafka::Producer::DeliveryHandle, result[0]
  end

  def test_flush_sync_empties_buffer
    @producer.buffer(build(:valid_message))
    @producer.flush_sync

    assert_empty @producer.messages
  end

  def test_flush_sync_with_error_during_flushing
    error = Rdkafka::RdkafkaError.new(0)

    @producer.client.stub(:produce, ->(*) { raise error }) do
      @producer.buffer(build(:valid_message))

      assert_raises(WaterDrop::Errors::ProduceManyError) do
        @producer.flush_sync
      end
    end
  end

  # disconnect with buffer data test

  def test_disconnect_not_allowed_with_buffered_data
    @producer.buffer(build(:valid_message))

    assert_same false, @producer.disconnect
  end
end
