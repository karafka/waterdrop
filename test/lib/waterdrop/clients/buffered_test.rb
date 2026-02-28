# frozen_string_literal: true

class WaterDropClientsBufferedTest < WaterDropTest::Base
  def setup
    @buffered_message_stub = Struct.new(:topic, :partition, :offset, keyword_init: true)
    @buffered_consumer_stub = Struct.new(:consumer_group_metadata_pointer, keyword_init: true)
    @topic_name = "it-#{SecureRandom.uuid}"

    @producer = WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end

    @client = WaterDrop::Clients::Buffered.new(@producer)

    @producer.stub(:client, @client) do
      @producer.produce_sync(payload: "one", topic: "foo")
      @producer.produce_sync(payload: "one", topic: "bar")
      @producer.produce_sync(payload: "two", topic: "foo")
    end
  end

  def teardown
    @producer.close
    super
  end

  def all_messages
    [
      { payload: "one", topic: "foo" },
      { payload: "one", topic: "bar" },
      { payload: "two", topic: "foo" }
    ]
  end

  def foo_messages
    [
      { payload: "one", topic: "foo" },
      { payload: "two", topic: "foo" }
    ]
  end

  def bar_messages
    [
      { payload: "one", topic: "bar" }
    ]
  end

  def test_messages_returns_all_produced_messages
    assert_equal all_messages, @client.messages
  end

  def test_messages_for_returns_messages_for_foo
    assert_equal foo_messages, @client.messages_for("foo")
  end

  def test_messages_for_returns_messages_for_bar
    assert_equal bar_messages, @client.messages_for("bar")
  end

  def test_messages_for_returns_empty_for_unknown_topic
    assert_empty @client.messages_for("buzz")
  end

  def test_reset_clears_all_messages
    @client.reset

    assert_empty @client.messages
  end

  def test_reset_clears_messages_for_topic
    @client.reset

    assert_empty @client.messages_for("foo")
  end
end

class WaterDropClientsBufferedTransactionTest < WaterDropTest::Base
  def setup
    @buffered_message_stub = Struct.new(:topic, :partition, :offset, keyword_init: true)
    @buffered_consumer_stub = Struct.new(:consumer_group_metadata_pointer, keyword_init: true)
    @topic_name = "it-#{SecureRandom.uuid}"

    @producer = WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "transactional.id": SecureRandom.uuid
      }
    end

    @client = WaterDrop::Clients::Buffered.new(@producer)

    @producer.stub(:client, @client) do
      @producer.produce_sync(payload: "one", topic: "foo")
      @producer.produce_sync(payload: "one", topic: "bar")
      @producer.produce_sync(payload: "two", topic: "foo")
    end
  end

  def teardown
    @producer.close
    super
  end

  def test_transaction_returns_block_value
    @producer.stub(:client, @client) do
      assert_equal 1, @producer.transaction { 1 }
    end
  end

  def test_transaction_adds_messages_to_buffers
    @producer.stub(:client, @client) do
      @producer.transaction do
        @producer.produce_sync(topic: @topic_name, payload: "test")
        @producer.produce_sync(topic: @topic_name, payload: "test")
      end

      assert_equal 5, @client.messages.size
      assert_equal 2, @client.messages_for(@topic_name).size
    end
  end

  def test_nested_transaction_adds_messages_to_buffers
    @producer.stub(:client, @client) do
      @producer.transaction do
        @producer.produce_sync(topic: @topic_name, payload: "test")
        @producer.produce_sync(topic: @topic_name, payload: "test")

        @producer.transaction do
          @producer.produce_sync(topic: @topic_name, payload: "test")
          @producer.produce_sync(topic: @topic_name, payload: "test")
        end
      end

      assert_equal 7, @client.messages.size
      assert_equal 4, @client.messages_for(@topic_name).size
    end
  end

  def test_nested_transaction_abort_discards_messages
    @producer.stub(:client, @client) do
      @producer.transaction do
        @producer.produce_sync(topic: @topic_name, payload: "test")
        @producer.produce_sync(topic: @topic_name, payload: "test")

        @producer.transaction do
          @producer.produce_sync(topic: @topic_name, payload: "test")
          @producer.produce_sync(topic: @topic_name, payload: "test")

          raise WaterDrop::AbortTransaction
        end
      end

      assert_equal 3, @client.messages.size
      assert_equal 0, @client.messages_for("test").size
    end
  end

  def test_abort_transaction_does_not_raise
    @producer.stub(:client, @client) do
      @producer.transaction { raise WaterDrop::AbortTransaction }
    end
  end

  def test_abort_transaction_does_not_contain_aborted_messages
    @producer.stub(:client, @client) do
      @producer.transaction do
        @producer.produce_sync(topic: @topic_name, payload: "test")

        raise WaterDrop::AbortTransaction
      end

      assert_equal 3, @client.messages.size
      assert_empty @client.messages_for("test")
    end
  end

  def test_waterdrop_abort_transaction_does_not_raise
    @producer.stub(:client, @client) do
      @producer.transaction { raise(WaterDrop::AbortTransaction) }
    end
  end

  def test_different_error_raises
    @producer.stub(:client, @client) do
      assert_raises(StandardError) do
        @producer.transaction { raise(StandardError) }
      end
    end
  end

  def test_different_error_does_not_contain_aborted_messages
    @producer.stub(:client, @client) do
      assert_raises(StandardError) do
        @producer.transaction do
          @producer.produce_sync(topic: @topic_name, payload: "test")

          raise StandardError
        end
      end

      assert_equal 3, @client.messages.size
      assert_empty @client.messages_for("test")
    end
  end

  def test_nested_transaction_works
    @producer.stub(:client, @client) do
      result = @producer.transaction do
        @producer.transaction do
          @producer.produce_sync(topic: "1", payload: "2")
          2
        end
      end

      assert_equal 2, result
    end
  end

  def test_store_offset_without_transaction_raises
    topic = "it-#{SecureRandom.uuid}"
    message = @buffered_message_stub.new(topic: topic, partition: 0, offset: 10)

    @producer.stub(:client, @client) do
      assert_raises(WaterDrop::Errors::TransactionRequiredError) do
        @producer.transaction_mark_as_consumed(nil, message)
      end
    end
  end

  def test_store_offset_with_transaction_does_not_raise
    topic = "it-#{SecureRandom.uuid}"
    consumer = @buffered_consumer_stub.new(consumer_group_metadata_pointer: nil)
    message = @buffered_message_stub.new(topic: topic, partition: 0, offset: 10)

    @producer.stub(:client, @client) do
      @producer.transaction do
        @producer.transaction_mark_as_consumed(consumer, message)
      end
    end
  end
end
