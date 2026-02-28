# frozen_string_literal: true

class ProducerClassMonitorTest < WaterDropTest::Base
  def setup
    @producer_class = Class.new do
      include WaterDrop::Producer::ClassMonitor
    end

    @producer = @producer_class.new
  end

  def teardown
    WaterDrop.instance_variable_set(:@instrumentation, nil)
    super
  end

  def test_class_monitor_returns_global_waterdrop_instrumentation
    assert_same WaterDrop.instrumentation, @producer.send(:class_monitor)
  end

  def test_class_monitor_returns_class_monitor_instance
    assert_kind_of WaterDrop::Instrumentation::ClassMonitor, @producer.send(:class_monitor)
  end

  def test_class_monitor_is_private_method
    assert_same true, @producer_class.private_method_defined?(:class_monitor)
  end

  def test_class_monitor_allows_instrumentation
    events_received = []

    WaterDrop.instrumentation.subscribe("producer.created") do |event|
      events_received << event
    end

    @producer.send(:class_monitor).instrument("producer.created", test_data: "value")

    assert_equal 1, events_received.size
    assert_equal "value", events_received.first[:test_data]
  end

  def test_included_in_producer_class
    assert_includes WaterDrop::Producer.included_modules, WaterDrop::Producer::ClassMonitor
  end

  def test_producer_instances_can_use_class_monitor
    producer = WaterDrop::Producer.new

    assert_kind_of WaterDrop::Instrumentation::ClassMonitor, producer.send(:class_monitor)
  end
end
