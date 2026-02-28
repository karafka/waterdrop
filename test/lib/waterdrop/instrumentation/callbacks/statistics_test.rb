# frozen_string_literal: true

class WaterDropInstrumentationCallbacksStatisticsCallDifferentProducerTest < WaterDropTest::Base
  def test_does_not_emit_statistics_for_different_producer
    producer_id = SecureRandom.uuid
    client_name = SecureRandom.uuid
    monitor = WaterDrop::Instrumentation::Monitor.new
    changed = []
    statistics = {}

    monitor.subscribe("statistics.emitted") do |event|
      changed << event[:statistics]
    end

    callback = WaterDrop::Instrumentation::Callbacks::Statistics.new(
      producer_id, client_name, monitor
    )
    callback.call(statistics)

    assert_empty changed
  end
end

class WaterDropInstrumentationCallbacksStatisticsCallErrorHandlingTest < WaterDropTest::Base
  def test_contains_notify_and_continue
    producer_id = SecureRandom.uuid
    client_name = SecureRandom.uuid
    monitor = WaterDrop::Instrumentation::Monitor.new
    statistics = { "name" => client_name }
    tracked_errors = []

    monitor.subscribe("statistics.emitted") do
      raise
    end

    local_errors = tracked_errors

    monitor.subscribe("error.occurred") do |event|
      local_errors << event
    end

    callback = WaterDrop::Instrumentation::Callbacks::Statistics.new(
      producer_id, client_name, monitor
    )
    callback.call(statistics)

    assert_equal 1, tracked_errors.size
    assert_equal "callbacks.statistics.error", tracked_errors.first[:type]
  end
end

class WaterDropInstrumentationCallbacksStatisticsCallExpectedProducerTest < WaterDropTest::Base
  def test_emits_statistics_for_expected_producer
    producer_id = SecureRandom.uuid
    client_name = SecureRandom.uuid
    monitor = WaterDrop::Instrumentation::Monitor.new
    statistics = { "name" => client_name }
    changed = []

    monitor.subscribe("statistics.emitted") do |event|
      changed << event[:statistics]
    end

    callback = WaterDrop::Instrumentation::Callbacks::Statistics.new(
      producer_id, client_name, monitor
    )
    callback.call(statistics)

    assert_equal [statistics], changed
  end
end

class WaterDropInstrumentationCallbacksStatisticsMultipleEmissionsTest < WaterDropTest::Base
  def test_emits_multiple_statistics
    producer_id = SecureRandom.uuid
    client_name = SecureRandom.uuid
    monitor = WaterDrop::Instrumentation::Monitor.new
    changed = []

    monitor.subscribe("statistics.emitted") do |event|
      changed << event[:statistics]
    end

    callback = WaterDrop::Instrumentation::Callbacks::Statistics.new(
      producer_id, client_name, monitor
    )

    5.times do |count|
      callback.call("msg_count" => count, "name" => client_name)
    end

    assert_equal 5, changed.size
  end

  def test_decorates_statistics
    producer_id = SecureRandom.uuid
    client_name = SecureRandom.uuid
    monitor = WaterDrop::Instrumentation::Monitor.new
    changed = []

    monitor.subscribe("statistics.emitted") do |event|
      changed << event[:statistics]
    end

    callback = WaterDrop::Instrumentation::Callbacks::Statistics.new(
      producer_id, client_name, monitor
    )

    5.times do |count|
      callback.call("msg_count" => count, "name" => client_name)
    end

    assert_equal 0, changed.first["msg_count_d"]
    assert_equal 1, changed.last["msg_count_d"]
  end
end

class WaterDropInstrumentationCallbacksStatisticsEventDataFormatTest < WaterDropTest::Base
  def setup
    super
    @producer_id = SecureRandom.uuid
    @client_name = SecureRandom.uuid
    @monitor = WaterDrop::Instrumentation::Monitor.new
    @statistics = { "name" => @client_name, "val" => 1, "str" => 1 }
    @events = []

    @monitor.subscribe("statistics.emitted") do |event|
      @events << event
    end

    callback = WaterDrop::Instrumentation::Callbacks::Statistics.new(
      @producer_id, @client_name, @monitor
    )
    callback.call(@statistics)

    @event = @events.first
  end

  def test_event_id
    assert_equal "statistics.emitted", @event.id
  end

  def test_event_producer_id
    assert_equal @producer_id, @event[:producer_id]
  end

  def test_event_statistics
    assert_equal @statistics, @event[:statistics]
  end

  def test_event_statistics_decoration
    assert_equal 0, @event[:statistics]["val_d"]
  end
end

class WaterDropInstrumentationCallbacksStatisticsLateSubscriptionNoListenersTest <
  WaterDropTest::Base
  def test_does_not_emit_statistics_without_listeners
    producer_id = SecureRandom.uuid
    client_name = SecureRandom.uuid
    monitor = WaterDrop::Instrumentation::Monitor.new
    statistics = { "name" => client_name, "msg_count" => 100 }
    events = []

    callback = WaterDrop::Instrumentation::Callbacks::Statistics.new(
      producer_id, client_name, monitor
    )
    callback.call(statistics)

    assert_empty events
  end
end

class WaterDropInstrumentationCallbacksStatisticsLateSubscriptionAddedLaterTest <
  WaterDropTest::Base
  def test_emits_statistics_for_subsequent_calls
    producer_id = SecureRandom.uuid
    client_name = SecureRandom.uuid
    monitor = WaterDrop::Instrumentation::Monitor.new
    statistics = { "name" => client_name, "msg_count" => 100 }
    events = []

    callback = WaterDrop::Instrumentation::Callbacks::Statistics.new(
      producer_id, client_name, monitor
    )

    # First call with no listeners
    callback.call(statistics)

    assert_empty events

    # Subscribe late
    monitor.subscribe("statistics.emitted") do |event|
      events << event
    end

    # Second call should now emit
    callback.call(statistics)

    refute_empty events
    assert_equal 1, events.size
  end
end
