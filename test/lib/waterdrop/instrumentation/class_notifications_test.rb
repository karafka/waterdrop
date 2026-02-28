# frozen_string_literal: true

class WaterDropInstrumentationClassNotificationsEventsTest < WaterDropTest::Base
  def test_contains_only_class_level_events
    expected_events = %w[
      producer.created
      producer.configured
      connection_pool.created
      connection_pool.setup
      connection_pool.shutdown
      connection_pool.reload
      connection_pool.reloaded
    ]

    assert_equal expected_events, WaterDrop::Instrumentation::ClassNotifications::EVENTS
  end

  def test_does_not_contain_instance_level_events
    instance_events = %w[
      producer.connected
      producer.closed
      message.produced_async
      message.produced_sync
      buffer.flushed_async
      error.occurred
    ]

    instance_events.each do |event|
      refute_includes WaterDrop::Instrumentation::ClassNotifications::EVENTS, event
    end
  end
end

class WaterDropInstrumentationClassNotificationsInitializeTest < WaterDropTest::Base
  def test_creates_notifications_instance_without_errors
    notifications = WaterDrop::Instrumentation::ClassNotifications.new

    assert_kind_of WaterDrop::Instrumentation::ClassNotifications, notifications
  end

  def test_calls_register_event_for_all_class_events
    notifications = WaterDrop::Instrumentation::ClassNotifications.new

    WaterDrop::Instrumentation::ClassNotifications::EVENTS.each do |event_name|
      notifications.subscribe(event_name) { |_event| nil }
    end
  end
end

class WaterDropInstrumentationClassNotificationsSubscribeTest < WaterDropTest::Base
  def setup
    super
    @notifications = WaterDrop::Instrumentation::ClassNotifications.new
  end

  def test_allows_subscribing_to_registered_class_events
    @notifications.subscribe("producer.created") { |_event| nil }
  end

  def test_raises_error_when_subscribing_to_unregistered_events
    assert_raises(Karafka::Core::Monitoring::Notifications::EventNotRegistered) do
      @notifications.subscribe("message.produced_sync") { |_event| nil }
    end
  end
end

class WaterDropInstrumentationClassNotificationsInheritanceTest < WaterDropTest::Base
  def test_inherits_from_karafka_core_monitoring_notifications
    assert_operator WaterDrop::Instrumentation::ClassNotifications, :<,
      Karafka::Core::Monitoring::Notifications
  end
end

class WaterDropInstrumentationClassNotificationsSeparationTest < WaterDropTest::Base
  def setup
    super
    @notifications = WaterDrop::Instrumentation::ClassNotifications.new
    @instance_notifications = WaterDrop::Instrumentation::Notifications.new
  end

  def test_has_different_event_sets
    refute_equal WaterDrop::Instrumentation::ClassNotifications::EVENTS,
      WaterDrop::Instrumentation::Notifications::EVENTS
  end

  def test_class_events_are_not_available_in_instance_notifications
    WaterDrop::Instrumentation::ClassNotifications::EVENTS.each do |event_name|
      assert_raises(Karafka::Core::Monitoring::Notifications::EventNotRegistered) do
        @instance_notifications.subscribe(event_name) { |_event| nil }
      end
    end
  end

  def test_instance_events_are_not_available_in_class_notifications
    sample_instance_events = %w[message.produced_async producer.connected error.occurred]

    sample_instance_events.each do |event_name|
      assert_raises(Karafka::Core::Monitoring::Notifications::EventNotRegistered) do
        @notifications.subscribe(event_name) { |_event| nil }
      end
    end
  end
end
