# frozen_string_literal: true

class WaterDropInstrumentationClassMonitorInitializeTest < WaterDropTest::Base
  def test_creates_a_monitor_instance_without_errors
    monitor = WaterDrop::Instrumentation::ClassMonitor.new

    assert_kind_of WaterDrop::Instrumentation::ClassMonitor, monitor
  end

  def test_accepts_custom_notifications_bus_parameter
    custom_bus = WaterDrop::Instrumentation::ClassNotifications.new
    custom_monitor = WaterDrop::Instrumentation::ClassMonitor.new(custom_bus)

    assert_kind_of WaterDrop::Instrumentation::ClassMonitor, custom_monitor
  end

  def test_accepts_namespace_parameter_without_errors
    WaterDrop::Instrumentation::ClassMonitor.new(nil, "test")
  end
end

class WaterDropInstrumentationClassMonitorSubscribeTest < WaterDropTest::Base
  def setup
    super
    @monitor = WaterDrop::Instrumentation::ClassMonitor.new
  end

  def test_allows_subscribing_to_class_level_events
    events_received = []

    @monitor.subscribe("producer.created") do |event|
      events_received << event
    end

    @monitor.instrument("producer.created", test_data: "value")

    assert_equal 1, events_received.size
    assert_equal "value", events_received.first[:test_data]
  end

  def test_allows_subscribing_to_producer_configured_events
    events_received = []

    @monitor.subscribe("producer.configured") do |event|
      events_received << event
    end

    @monitor.instrument("producer.configured", producer_id: "test-id")

    assert_equal 1, events_received.size
    assert_equal "test-id", events_received.first[:producer_id]
  end

  def test_raises_error_when_subscribing_to_non_class_events
    assert_raises(Karafka::Core::Monitoring::Notifications::EventNotRegistered) do
      @monitor.subscribe("message.produced_async") { |_event| }
    end
  end

  def test_raises_error_when_subscribing_to_instance_level_events
    instance_events = %w[
      producer.connected
      producer.closed
      message.produced_sync
      buffer.flushed_async
      error.occurred
    ]

    instance_events.each do |event_name|
      assert_raises(Karafka::Core::Monitoring::Notifications::EventNotRegistered) do
        @monitor.subscribe(event_name) { |_event| nil }
      end
    end
  end
end

class WaterDropInstrumentationClassMonitorInstrumentTest < WaterDropTest::Base
  def setup
    super
    @monitor = WaterDrop::Instrumentation::ClassMonitor.new
  end

  def test_instruments_class_level_events_successfully
    events_received = []

    @monitor.subscribe("producer.created") do |event|
      events_received << event
    end

    result = @monitor.instrument("producer.created", producer: "test_producer") do
      "instrumented_result"
    end

    assert_equal "instrumented_result", result
    assert_equal 1, events_received.size
    assert_equal "test_producer", events_received.first[:producer]
  end

  def test_supports_instrumenting_without_a_block
    events_received = []

    @monitor.subscribe("producer.configured") do |event|
      events_received << event
    end

    @monitor.instrument("producer.configured", config: "test_config")

    assert_equal 1, events_received.size
    assert_equal "test_config", events_received.first[:config]
  end

  def test_raises_error_when_instrumenting_non_class_events
    assert_raises(Karafka::Core::Monitoring::Notifications::EventNotRegistered) do
      @monitor.instrument("message.produced_sync", message: {})
    end
  end
end

class WaterDropInstrumentationClassMonitorMultipleSubscribersTest < WaterDropTest::Base
  def test_notifies_all_subscribers_for_the_same_event
    monitor = WaterDrop::Instrumentation::ClassMonitor.new
    events_received1 = []
    events_received2 = []

    monitor.subscribe("producer.created") do |event|
      events_received1 << event
    end

    monitor.subscribe("producer.created") do |event|
      events_received2 << event
    end

    monitor.instrument("producer.created", producer_id: "shared-test")

    assert_equal 1, events_received1.size
    assert_equal 1, events_received2.size
    assert_equal "shared-test", events_received1.first[:producer_id]
    assert_equal "shared-test", events_received2.first[:producer_id]
  end
end

class WaterDropInstrumentationClassMonitorInheritanceTest < WaterDropTest::Base
  def test_inherits_from_karafka_core_monitoring_monitor
    assert_operator WaterDrop::Instrumentation::ClassMonitor, :<,
      Karafka::Core::Monitoring::Monitor
  end
end

class WaterDropInstrumentationClassMonitorIntegrationWithoutConfigTest < WaterDropTest::Base
  def setup
    super
    @events_received = []

    WaterDrop.instrumentation.subscribe("producer.created") do |event|
      @events_received << [:producer_created, event]
    end

    WaterDrop.instrumentation.subscribe("producer.configured") do |event|
      @events_received << [:producer_configured, event]
    end

    @producer = WaterDrop::Producer.new
  end

  def teardown
    WaterDrop.instance_variable_set(:@instrumentation, nil)
    super
  end

  def test_instruments_producer_created_event
    created_events = @events_received.select { |event| event.first == :producer_created }

    assert_equal 1, created_events.size

    event_data = created_events.first.last

    assert_same @producer, event_data[:producer]
    assert_nil event_data[:producer_id]
  end

  def test_does_not_instrument_producer_configured_event_yet
    configured_events = @events_received.select { |event| event.first == :producer_configured }

    assert_empty configured_events
  end
end

class WaterDropInstrumentationClassMonitorIntegrationWithConfigTest < WaterDropTest::Base
  def setup
    super
    @events_received = []

    WaterDrop.instrumentation.subscribe("producer.created") do |event|
      @events_received << [:producer_created, event]
    end

    WaterDrop.instrumentation.subscribe("producer.configured") do |event|
      @events_received << [:producer_configured, event]
    end

    @producer = WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    WaterDrop.instance_variable_set(:@instrumentation, nil)
    super
  end

  def test_instruments_both_producer_created_and_producer_configured_events
    created_events = @events_received.select { |event| event.first == :producer_created }
    configured_events = @events_received.select { |event| event.first == :producer_configured }

    assert_equal 1, created_events.size
    assert_equal 1, configured_events.size

    config_event_data = configured_events.first.last

    assert_same @producer, config_event_data[:producer]
    assert_equal @producer.id, config_event_data[:producer_id]
    assert_same @producer.config, config_event_data[:config]
  end
end

class WaterDropInstrumentationClassMonitorIntegrationMultipleProducersTest < WaterDropTest::Base
  def setup
    super
    @events_received = []

    WaterDrop.instrumentation.subscribe("producer.created") do |event|
      @events_received << [:producer_created, event]
    end

    WaterDrop.instrumentation.subscribe("producer.configured") do |event|
      @events_received << [:producer_configured, event]
    end

    @producer1 = WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end

    @producer2 = WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": "localhost:9093" }
    end
  end

  def teardown
    WaterDrop.instance_variable_set(:@instrumentation, nil)
    super
  end

  def test_instruments_events_for_each_producer_separately
    created_events = @events_received.select { |event| event.first == :producer_created }
    configured_events = @events_received.select { |event| event.first == :producer_configured }

    assert_equal 2, created_events.size
    assert_equal 2, configured_events.size

    created_producers = created_events.map { |event| event.last[:producer] }
    configured_producers = configured_events.map { |event| event.last[:producer] }

    assert_includes created_producers, @producer1
    assert_includes created_producers, @producer2
    assert_includes configured_producers, @producer1
    assert_includes configured_producers, @producer2
  end
end

class WaterDropInstrumentationClassMonitorIntegrationExternalLibrariesTest < WaterDropTest::Base
  def setup
    super
    @events_received = []

    WaterDrop.instrumentation.subscribe("producer.created") do |event|
      @events_received << [:producer_created, event]
    end

    WaterDrop.instrumentation.subscribe("producer.configured") do |event|
      @events_received << [:producer_configured, event]
    end
  end

  def teardown
    WaterDrop.instance_variable_set(:@instrumentation, nil)
    super
  end

  def test_allows_external_libraries_to_hook_into_producer_lifecycle
    middleware_applied = []

    WaterDrop.instrumentation.subscribe("producer.configured") do |event|
      producer = event[:producer]
      middleware_applied << producer.id

      producer.config.middleware.append(
        lambda do |message|
          message[:headers] ||= {}
          message[:headers]["x-trace-id"] = "test-trace-id"
          message
        end
      )
    end

    producer = WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end

    assert_includes middleware_applied, producer.id

    test_message = { topic: "test", payload: "test" }
    processed_message = producer.middleware.run(test_message)

    assert_equal "test-trace-id", processed_message[:headers]["x-trace-id"]
  end
end
