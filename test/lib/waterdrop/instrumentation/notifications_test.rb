# frozen_string_literal: true

class WaterDropInstrumentationNotificationsInstrumentTest < WaterDropTest::Base
  def setup
    super
    @monitor = WaterDrop::Instrumentation::Notifications.new
  end

  def test_returns_blocks_execution_value
    result = rand
    instrumentation = @monitor.instrument(
      "message.produced_async",
      call: self,
      error: StandardError
    ) { result }

    assert_equal result, instrumentation
  end
end

class WaterDropInstrumentationNotificationsSubscribeBlockListenerTest < WaterDropTest::Base
  def setup
    super
    @monitor = WaterDrop::Instrumentation::Notifications.new
  end

  def test_raises_error_for_unsupported_event
    expected_error = Karafka::Core::Monitoring::Notifications::EventNotRegistered
    assert_raises(expected_error) do
      @monitor.subscribe("unsupported") { nil }
    end
  end

  def test_subscribing_to_supported_event_does_not_raise
    @monitor.subscribe("message.produced_async") { nil }
  end
end

class WaterDropInstrumentationNotificationsSubscribeObjectListenerTest < WaterDropTest::Base
  def test_subscribing_with_object_listener_does_not_raise
    monitor = WaterDrop::Instrumentation::Notifications.new
    listener = Class.new do
      def on_message_produced_async(_event)
        true
      end
    end

    monitor.subscribe(listener.new)
  end
end

class WaterDropInstrumentationNotificationsLifecycleInitializedTest < WaterDropTest::Base
  def setup
    super
    @producer = WaterDrop::Producer.new
    @events = []
  end

  def teardown
    @producer.close
    super
  end

  def test_status_is_initial
    assert_equal "initial", @producer.status.to_s
  end

  def test_events_are_empty
    assert_empty @events
  end
end

class WaterDropInstrumentationNotificationsLifecycleConfiguredTest < WaterDropTest::Base
  def setup
    super
    @producer = WaterDrop::Producer.new
    @events = []

    @producer.setup { nil }

    %w[producer.connected producer.closing producer.closed].each do |event_name|
      @producer.monitor.subscribe(event_name) do |event|
        @events << event
      end
    end
  end

  def teardown
    @producer.close
    super
  end

  def test_status_is_configured
    assert_equal "configured", @producer.status.to_s
  end

  def test_events_are_empty
    assert_empty @events
  end
end

class WaterDropInstrumentationNotificationsLifecycleConnectedTest < WaterDropTest::Base
  def setup
    super
    @producer = WaterDrop::Producer.new
    @events = []

    @producer.setup { nil }

    %w[producer.connected producer.closing producer.closed].each do |event_name|
      @producer.monitor.subscribe(event_name) do |event|
        @events << event
      end
    end

    @producer.client
  end

  def teardown
    @producer.close
    super
  end

  def test_status_is_connected
    assert_equal "connected", @producer.status.to_s
  end

  def test_one_event_received
    assert_equal 1, @events.size
  end

  def test_event_is_producer_connected
    assert_equal "producer.connected", @events.last.id
  end

  def test_event_has_producer_id
    assert_same true, @events.last.payload.key?(:producer_id)
  end
end

class WaterDropInstrumentationNotificationsLifecycleClosedTest < WaterDropTest::Base
  def setup
    super
    @producer = WaterDrop::Producer.new
    @events = []

    @producer.setup { nil }

    %w[producer.connected producer.closing producer.closed].each do |event_name|
      @producer.monitor.subscribe(event_name) do |event|
        @events << event
      end
    end

    @producer.client
    @producer.close
  end

  def test_status_is_closed
    assert_equal "closed", @producer.status.to_s
  end

  def test_three_events_received
    assert_equal 3, @events.size
  end

  def test_first_event_is_producer_connected
    assert_equal "producer.connected", @events.first.id
  end

  def test_first_event_has_producer_id
    assert_same true, @events.first.payload.key?(:producer_id)
  end

  def test_second_event_is_producer_closing
    assert_equal "producer.closing", @events[1].id
  end

  def test_second_event_has_producer_id
    assert_same true, @events[1].payload.key?(:producer_id)
  end

  def test_last_event_is_producer_closed
    assert_equal "producer.closed", @events.last.id
  end

  def test_last_event_has_producer_id
    assert_same true, @events.last.payload.key?(:producer_id)
  end

  def test_last_event_has_time
    assert_same true, @events.last.payload.key?(:time)
  end
end
