# frozen_string_literal: true

class WaterDropInstrumentationCallbacksErrorCallDifferentProducerTest < WaterDropTest::Base
  def test_does_not_emit_errors_for_different_producer
    producer_id = SecureRandom.uuid
    client_name = SecureRandom.uuid
    monitor = WaterDrop::Instrumentation::Monitor.new
    error = Rdkafka::RdkafkaError.new(1, [])
    changed = []

    monitor.subscribe("error.occurred") do |event|
      changed << event[:error]
    end

    callback = WaterDrop::Instrumentation::Callbacks::Error.new(producer_id, "other", monitor)
    callback.call(client_name, error)

    assert_empty changed
  end
end

class WaterDropInstrumentationCallbacksErrorCallExpectedProducerTest < WaterDropTest::Base
  def test_emits_errors_for_expected_producer
    producer_id = SecureRandom.uuid
    client_name = SecureRandom.uuid
    monitor = WaterDrop::Instrumentation::Monitor.new
    error = Rdkafka::RdkafkaError.new(1, [])
    changed = []

    monitor.subscribe("error.occurred") do |event|
      changed << event[:error]
    end

    callback = WaterDrop::Instrumentation::Callbacks::Error.new(
      producer_id, client_name, monitor
    )
    callback.call(client_name, error)

    assert_equal [error], changed
  end
end

class WaterDropInstrumentationCallbacksErrorEventDataFormatTest < WaterDropTest::Base
  def setup
    super
    @producer_id = SecureRandom.uuid
    @client_name = SecureRandom.uuid
    @monitor = WaterDrop::Instrumentation::Monitor.new
    @error = Rdkafka::RdkafkaError.new(1, [])
    @changed = []

    @monitor.subscribe("error.occurred") do |stat|
      @changed << stat
    end

    callback = WaterDrop::Instrumentation::Callbacks::Error.new(
      @producer_id, @client_name, @monitor
    )
    callback.call(@client_name, @error)

    @event = @changed.first
  end

  def test_event_id
    assert_equal "error.occurred", @event.id
  end

  def test_event_producer_id
    assert_equal @producer_id, @event[:producer_id]
  end

  def test_event_error
    assert_equal @error, @event[:error]
  end

  def test_event_type
    assert_equal "librdkafka.error", @event[:type]
  end
end

class WaterDropInstrumentationCallbacksErrorHandlerContainsErrorTest < WaterDropTest::Base
  def test_contains_notify_and_continue
    producer_id = SecureRandom.uuid
    client_name = SecureRandom.uuid
    monitor = WaterDrop::Instrumentation::Monitor.new
    error = Rdkafka::RdkafkaError.new(1, [])
    tracked_errors = []

    monitor.subscribe("error.occurred") do |event|
      next unless event[:type] == "librdkafka.error"

      raise
    end

    local_errors = tracked_errors

    monitor.subscribe("error.occurred") do |event|
      local_errors << event
    end

    callback = WaterDrop::Instrumentation::Callbacks::Error.new(
      producer_id, client_name, monitor
    )
    callback.call(client_name, error)

    assert_equal 1, tracked_errors.size
    assert_equal "callbacks.error.error", tracked_errors.first[:type]
  end
end
