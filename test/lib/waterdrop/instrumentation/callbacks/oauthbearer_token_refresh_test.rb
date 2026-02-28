# frozen_string_literal: true

class WaterDropInstrumentationCallbacksOauthbearerTokenRefreshMatchingBearerTest <
  WaterDropTest::Base
  def test_instruments_oauthbearer_token_refresh_event
    monitor = WaterDrop::Instrumentation::Monitor.new
    instrumented_events = []

    monitor.subscribe("oauthbearer.token_refresh") do |event|
      instrumented_events << event
    end

    bearer = Minitest::Mock.new
    bearer.expect(:name, "test_bearer")

    callback = WaterDrop::Instrumentation::Callbacks::OauthbearerTokenRefresh.new(
      bearer, monitor
    )
    rd_config = Rdkafka::Config.new
    callback.call(rd_config, "test_bearer")

    assert_equal 1, instrumented_events.size
  end
end

class WaterDropInstrumentationCallbacksOauthbearerTokenRefreshNonMatchingBearerTest <
  WaterDropTest::Base
  def test_does_not_instrument_any_event
    monitor = WaterDrop::Instrumentation::Monitor.new
    instrumented_events = []

    monitor.subscribe("oauthbearer.token_refresh") do |event|
      instrumented_events << event
    end

    bearer = Minitest::Mock.new
    bearer.expect(:name, "test_bearer")

    callback = WaterDrop::Instrumentation::Callbacks::OauthbearerTokenRefresh.new(
      bearer, monitor
    )
    rd_config = Rdkafka::Config.new
    callback.call(rd_config, "different_bearer")

    assert_empty instrumented_events
  end
end

class WaterDropInstrumentationCallbacksOauthbearerTokenRefreshErrorHandlingTest <
  WaterDropTest::Base
  def test_contains_notify_and_continue
    monitor = WaterDrop::Instrumentation::Monitor.new
    tracked_errors = []

    monitor.subscribe("oauthbearer.token_refresh") do
      raise
    end

    local_errors = tracked_errors

    monitor.subscribe("error.occurred") do |event|
      local_errors << event
    end

    bearer = Minitest::Mock.new
    bearer.expect(:name, "test_bearer")

    callback = WaterDrop::Instrumentation::Callbacks::OauthbearerTokenRefresh.new(
      bearer, monitor
    )
    rd_config = Rdkafka::Config.new
    callback.call(rd_config, "test_bearer")

    assert_equal 1, tracked_errors.size
    assert_equal "callbacks.oauthbearer_token_refresh.error", tracked_errors.first[:type]
  end
end
