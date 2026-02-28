# frozen_string_literal: true

class WaterDropInstrumentationIdleDisconnectorListenerTest < WaterDropTest::Base
  def setup
    super
    @producer = build(:producer)
    @timeout_ms = 1000
    @disconnected_events = []
    @topic_name = "it-#{SecureRandom.uuid}"

    @producer.monitor.subscribe("producer.disconnected") do |event|
      @disconnected_events << event
    end

    2.times { @producer.produce_sync(topic: @topic_name, payload: "msg") }

    @listener = WaterDrop::Instrumentation::IdleDisconnectorListener.new(
      @producer,
      disconnect_timeout: @timeout_ms
    )
  end

  def teardown
    @producer.close
    super
  end
end

class WaterDropInstrumentationIdleDisconnectorListenerMessagesTransmittedTest <
  WaterDropInstrumentationIdleDisconnectorListenerTest
  def test_does_not_disconnect_producer
    statistics = { "txmsgs" => 100 }
    event = { statistics: statistics }
    @listener.on_statistics_emitted(event)

    assert_empty @disconnected_events
  end

  def test_does_not_disconnect_with_increasing_message_count
    @listener.on_statistics_emitted({ statistics: { "txmsgs" => 50 } })
    @listener.on_statistics_emitted({ statistics: { "txmsgs" => 100 } })
    @listener.on_statistics_emitted({ statistics: { "txmsgs" => 150 } })

    assert_empty @disconnected_events
  end
end

class WaterDropInstrumentationIdleDisconnectorListenerNoNewMessagesCanDisconnectTest <
  WaterDropInstrumentationIdleDisconnectorListenerTest
  def setup
    super
    old_time = @listener.send(:monotonic_now) - @timeout_ms - 100
    @listener.instance_variable_set(:@last_activity_time, old_time)
    @listener.instance_variable_set(:@last_txmsgs, 100)
  end

  def test_disconnects_the_producer
    statistics = { "txmsgs" => 100 }
    event = { statistics: statistics }
    @listener.on_statistics_emitted(event)
    sleep(0.2)

    assert_equal 1, @disconnected_events.size
  end

  def test_emits_disconnect_event_with_producer_id
    statistics = { "txmsgs" => 100 }
    event = { statistics: statistics }
    @listener.on_statistics_emitted(event)
    sleep(0.2)

    assert_equal @producer.id, @disconnected_events.first[:producer_id]
  end

  def test_does_not_disconnect_again_on_subsequent_calls_with_same_txmsgs
    statistics = { "txmsgs" => 100 }
    event = { statistics: statistics }
    @listener.on_statistics_emitted(event)
    @listener.on_statistics_emitted(event)
    sleep(0.2)

    assert_equal 1, @disconnected_events.size
  end
end

class WaterDropInstrumentationIdleDisconnectorListenerNotDisconnectableTest <
  WaterDropInstrumentationIdleDisconnectorListenerTest
  def setup
    super
    old_time = @listener.send(:monotonic_now) - @timeout_ms - 100
    @listener.instance_variable_set(:@last_activity_time, old_time)
    @listener.instance_variable_set(:@last_txmsgs, 100)
  end

  def test_does_not_attempt_disconnect_when_not_disconnectable
    disconnect_called = false

    @producer.stub(:disconnectable?, false) do
      @producer.stub(:disconnect, -> { disconnect_called = true }) do
        statistics = { "txmsgs" => 100 }
        event = { statistics: statistics }
        @listener.on_statistics_emitted(event)

        assert_empty @disconnected_events
        assert_same false, disconnect_called
      end
    end
  end

  def test_still_resets_activity_time_when_not_disconnectable
    @producer.stub(:disconnectable?, false) do
      @producer.stub(:disconnect, nil) do
        old_activity_time = @listener.instance_variable_get(:@last_activity_time)
        statistics = { "txmsgs" => 100 }
        event = { statistics: statistics }
        @listener.on_statistics_emitted(event)
        new_activity_time = @listener.instance_variable_get(:@last_activity_time)

        assert_operator new_activity_time, :>, old_activity_time
      end
    end
  end
end

class WaterDropInstrumentationIdleDisconnectorListenerDisconnectFailsTest <
  WaterDropInstrumentationIdleDisconnectorListenerTest
  def setup
    super
    old_time = @listener.send(:monotonic_now) - @timeout_ms - 100
    @listener.instance_variable_set(:@last_activity_time, old_time)
    @listener.instance_variable_set(:@last_txmsgs, 100)
    @error_events = []
    @test_error = StandardError.new("disconnect failed")

    @producer.monitor.subscribe("error.occurred") do |event|
      @error_events << event
    end
  end

  def test_handles_error_gracefully_and_instruments_it
    @producer.stub(:disconnectable?, true) do
      @producer.stub(:disconnect, ->(*) { raise @test_error }) do
        statistics = { "txmsgs" => 100 }
        event = { statistics: statistics }
        @listener.on_statistics_emitted(event)
        sleep(0.1)

        assert_empty @disconnected_events
        refute_empty @error_events
        assert_equal "producer.disconnect.error", @error_events.first[:type]
        assert_equal @test_error, @error_events.first[:error]
        assert_equal @producer.id, @error_events.first[:producer_id]
      end
    end
  end

  def test_still_resets_activity_time_even_after_error
    @producer.stub(:disconnectable?, true) do
      @producer.stub(:disconnect, ->(*) { raise @test_error }) do
        old_activity_time = @listener.instance_variable_get(:@last_activity_time)
        statistics = { "txmsgs" => 100 }
        event = { statistics: statistics }
        @listener.on_statistics_emitted(event)
        sleep(0.1)
        new_activity_time = @listener.instance_variable_get(:@last_activity_time)

        assert_operator new_activity_time, :>, old_activity_time
      end
    end
  end
end

class WaterDropInstrumentationIdleDisconnectorListenerTimeoutNotExceededTest <
  WaterDropInstrumentationIdleDisconnectorListenerTest
  def test_does_not_disconnect_producer
    recent_time = @listener.send(:monotonic_now) - (@timeout_ms / 2)
    @listener.instance_variable_set(:@last_activity_time, recent_time)
    @listener.instance_variable_set(:@last_txmsgs, 100)

    statistics = { "txmsgs" => 100 }
    event = { statistics: statistics }
    @listener.on_statistics_emitted(event)

    assert_empty @disconnected_events
  end
end

class WaterDropInstrumentationIdleDisconnectorListenerMissingTxmsgsTest <
  WaterDropInstrumentationIdleDisconnectorListenerTest
  def test_handles_missing_txmsgs_gracefully
    statistics = {}
    event = { statistics: statistics }
    @listener.on_statistics_emitted(event)
  end

  def test_treats_as_zero_messages
    statistics = {}
    event = { statistics: statistics }
    @listener.on_statistics_emitted(event)

    assert_empty @disconnected_events
  end
end

class WaterDropInstrumentationIdleDisconnectorListenerOnStatisticsEmittedTest <
  WaterDropInstrumentationIdleDisconnectorListenerTest
  def test_delegates_to_call_method_with_statistics
    call_args = []

    @listener.stub(:call, ->(stats) { call_args << stats }) do
      event = { statistics: { "txmsgs" => 150 } }
      @listener.on_statistics_emitted(event)

      assert_equal [{ "txmsgs" => 150 }], call_args
    end
  end
end

class WaterDropInstrumentationIdleDisconnectorListenerIntegrationTest <
  WaterDropInstrumentationIdleDisconnectorListenerTest
  def test_responds_to_statistics_events
    assert_respond_to @listener, :on_statistics_emitted
  end

  def test_uses_default_timeout_and_does_not_disconnect_immediately
    long_timeout_listener = WaterDrop::Instrumentation::IdleDisconnectorListener.new(@producer)
    long_timeout_listener.on_statistics_emitted({ statistics: { "txmsgs" => 100 } })
    long_timeout_listener.on_statistics_emitted({ statistics: { "txmsgs" => 100 } })

    assert_empty @disconnected_events
  end
end
