# frozen_string_literal: true

class ProducerBuilderTest < WaterDropTest::Base
  def setup
    @producer = WaterDrop::Producer.new
    @deliver = true
    @config = create_config(@deliver)
  end

  def teardown
    @client.close if defined?(@client) && @client
    @producer.close
    super
  end

  def test_client_is_rdkafka_producer
    @client = build_client

    assert_kind_of Rdkafka::Producer, @client
  end

  def test_delivery_callback_is_correct_type
    @client = build_client

    assert_kind_of WaterDrop::Instrumentation::Callbacks::Delivery, @client.delivery_callback
  end

  def test_client_is_dummy_when_delivery_off
    @deliver = false
    @config = create_config(@deliver)
    @client = build_client

    assert_kind_of WaterDrop::Clients::Dummy, @client
  end

  def test_delivery_callback_event_offset
    @client = build_client
    delivery_report = Rdkafka::Producer::DeliveryReport.new(rand, rand)

    callback_event = nil
    @config.monitor.subscribe("message.acknowledged") do |event|
      callback_event = event
    end

    @client.delivery_callback.call(delivery_report)

    assert_equal delivery_report.offset, callback_event[:offset]
  end

  def test_delivery_callback_event_partition
    @client = build_client
    delivery_report = Rdkafka::Producer::DeliveryReport.new(rand, rand)

    callback_event = nil
    @config.monitor.subscribe("message.acknowledged") do |event|
      callback_event = event
    end

    @client.delivery_callback.call(delivery_report)

    assert_equal delivery_report.partition, callback_event[:partition]
  end

  def test_delivery_callback_event_producer_id
    @client = build_client
    delivery_report = Rdkafka::Producer::DeliveryReport.new(rand, rand)

    callback_event = nil
    @config.monitor.subscribe("message.acknowledged") do |event|
      callback_event = event
    end

    @client.delivery_callback.call(delivery_report)

    assert_nil callback_event[:producer_id]
  end

  private

  def create_config(deliver)
    should_deliver = deliver
    producer_config = WaterDrop::Config.new

    producer_config.setup do |config|
      config.deliver = should_deliver
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end

    producer_config.config
  end

  def build_client
    # We need to stub config on producer for the builder
    producer = @producer
    config = @config

    original_config = producer.method(:config) if producer.respond_to?(:config)

    producer.define_singleton_method(:config) { config }

    client = WaterDrop::Producer::Builder.new.call(producer, config)

    if original_config
      producer.singleton_class.remove_method(:config)
    end

    client
  end
end
