# frozen_string_literal: true

class WaterDropGemRootTest < WaterDropTest::Base
  def test_gem_root_returns_current_directory
    assert_equal Dir.pwd, WaterDrop.gem_root.to_path
  end
end

class WaterDropMonitorTest < WaterDropTest::Base
  def teardown
    WaterDrop.instance_variable_set(:@instrumentation, nil)
    super
  end

  def test_monitor_returns_class_monitor_instance
    assert_kind_of WaterDrop::Instrumentation::ClassMonitor, WaterDrop.monitor
  end

  def test_monitor_memoizes_the_instance
    first_call = WaterDrop.monitor
    second_call = WaterDrop.monitor

    assert_same first_call, second_call
  end

  def test_monitor_allows_subscribing_to_class_level_events
    events_received = []

    WaterDrop.monitor.subscribe("producer.created") do |event|
      events_received << event
    end

    WaterDrop.monitor.instrument("producer.created", data: "test_data")

    assert_equal 1, events_received.size
    assert_equal "test_data", events_received.first[:data]
  end

  def test_monitor_raises_error_when_subscribing_to_non_class_events
    assert_raises(Karafka::Core::Monitoring::Notifications::EventNotRegistered) do
      WaterDrop.monitor.subscribe("message.produced_async") do |_event|
        # This should not be allowed
      end
    end
  end
end

class WaterDropInstrumentationTest < WaterDropTest::Base
  def teardown
    WaterDrop.instance_variable_set(:@instrumentation, nil)
    super
  end

  def test_instrumentation_returns_class_monitor_instance
    assert_kind_of WaterDrop::Instrumentation::ClassMonitor, WaterDrop.instrumentation
  end

  def test_instrumentation_is_same_instance_as_monitor
    assert_same WaterDrop.monitor, WaterDrop.instrumentation
  end

  def test_instrumentation_maintains_backward_compatibility
    events_received = []

    WaterDrop.instrumentation.subscribe("producer.created") do |event|
      events_received << event
    end

    WaterDrop.instrumentation.instrument("producer.created", data: "legacy_test")

    assert_equal 1, events_received.size
    assert_equal "legacy_test", events_received.first[:data]
  end

  def test_instrumentation_shares_memoized_instance_with_monitor
    instrumentation_instance = WaterDrop.instrumentation
    monitor_instance = WaterDrop.monitor

    assert_same instrumentation_instance, monitor_instance
  end
end
