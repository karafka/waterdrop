# frozen_string_literal: true

class ProducerIdempotenceTest < WaterDropTest::Base
  def setup
    @producer = build(:producer)
  end

  def teardown
    @producer.close
    super
  end

  def test_transactional_producer_is_idempotent
    producer = build(:transactional_producer)

    assert_same true, producer.idempotent?
    producer.close
  end

  def test_idempotent_producer_returns_true
    producer = build(:idempotent_producer)

    assert_same true, producer.idempotent?
    producer.close
  end

  def test_non_idempotent_producer_returns_false
    producer = build(
      :producer,
      kafka: { "bootstrap.servers": BOOTSTRAP_SERVERS, "enable.idempotence": false }
    )

    assert_same false, producer.idempotent?
    producer.close
  end

  def test_default_producer_is_not_idempotent
    assert_same false, @producer.idempotent?
  end

  def test_idempotent_result_is_cached
    producer = build(:idempotent_producer)
    first_result = producer.idempotent?

    assert_equal first_result, producer.idempotent?
    assert_same true, producer.instance_variable_defined?(:@idempotent)
    producer.close
  end
end

class ProducerIdempotenceFatalErrorWithReloadTest < WaterDropTest::Base
  def setup
    @producer = build(:idempotent_producer, reload_on_idempotent_fatal_error: true)
    @topic_name = "it-#{SecureRandom.uuid}"
    @message = build(:valid_message, topic: @topic_name)
    @fatal_error = Rdkafka::RdkafkaError.new(-150, fatal: true)
    @reloaded_events = []
    @producer.monitor.subscribe("producer.reloaded") { |event| @reloaded_events << event }
  end

  def teardown
    @producer.close
    super
  end

  def test_instruments_reload_and_reloaded_events_during_reload
    reload_events = []
    @producer.monitor.subscribe("producer.reload") { |event| reload_events << event }

    call_count = 0

    @producer.client.stub(:produce, lambda { |**_args|
      call_count += 1
      raise @fatal_error if call_count == 1

      mock_handle = Minitest::Mock.new
      mock_handle.expect(:wait, nil)
      mock_handle
    }) do
      WaterDrop::Producer::Builder.stub(:new, lambda {
        builder_proxy = Object.new
        builder_proxy.define_singleton_method(:call) do |_prod, _config|
          client_proxy = Object.new
          client_proxy.define_singleton_method(:produce) do |**_args|
            call_count += 1
            handle = Object.new
            handle.define_singleton_method(:wait) { |**_args| nil }
            handle
          end
          client_proxy.define_singleton_method(:flush) { |*_args| nil }
          client_proxy.define_singleton_method(:close) { nil }
          client_proxy.define_singleton_method(:closed?) { false }
          client_proxy.define_singleton_method(:purge) { nil }
          client_proxy
        end
        builder_proxy
      }) do
        @producer.produce_sync(@message)
      end
    end

    assert_equal 1, reload_events.size
    assert_equal @producer.id, reload_events.first[:producer_id]
    assert_equal @fatal_error, reload_events.first[:error]
    assert_equal 1, reload_events.first[:attempt]
    assert_equal @producer, reload_events.first[:caller]

    assert_equal 1, @reloaded_events.size
    assert_equal @producer.id, @reloaded_events.first[:producer_id]
    assert_equal 1, @reloaded_events.first[:attempt]
  end
end

class ProducerIdempotenceFatalErrorDisabledReloadTest < WaterDropTest::Base
  def setup
    @producer = build(:idempotent_producer, reload_on_idempotent_fatal_error: false)
    @topic_name = "it-#{SecureRandom.uuid}"
    @message = build(:valid_message, topic: @topic_name)
    @fatal_error = Rdkafka::RdkafkaError.new(-150, fatal: true)
  end

  def teardown
    @producer.close
    super
  end

  def test_raises_fatal_error_without_reload
    @producer.client.stub(:produce, ->(**_args) { raise @fatal_error }) do
      assert_raises(WaterDrop::Errors::ProduceError) do
        @producer.produce_sync(@message)
      end
    end
  end
end

class ProducerIdempotenceNonReloadableFencedErrorTest < WaterDropTest::Base
  def setup
    @producer = build(:idempotent_producer, reload_on_idempotent_fatal_error: true)
    @topic_name = "it-#{SecureRandom.uuid}"
    @message = build(:valid_message, topic: @topic_name)
    @fenced_error = Rdkafka::RdkafkaError.new(-144, fatal: true)
    @reloaded_events = []
    @producer.monitor.subscribe("producer.reloaded") { |event| @reloaded_events << event }
  end

  def teardown
    @producer.close
    super
  end

  def test_does_not_reload_and_raises_error
    @producer.client.stub(:produce, ->(**_args) { raise @fenced_error }) do
      assert_raises(WaterDrop::Errors::ProduceError) do
        @producer.produce_sync(@message)
      end
    end
  end

  def test_does_not_emit_reloaded_event
    @producer.client.stub(:produce, ->(**_args) { raise @fenced_error }) do
      @producer.produce_sync(@message)
    rescue WaterDrop::Errors::ProduceError
      nil
    end

    assert_empty @reloaded_events
  end
end

class ProducerIdempotenceFatalErrorOnTransactionalProducerTest < WaterDropTest::Base
  def setup
    @producer = build(:transactional_producer, reload_on_idempotent_fatal_error: true)
    @topic_name = "it-#{SecureRandom.uuid}"
    @message = build(:valid_message, topic: @topic_name)
    @fatal_error = Rdkafka::RdkafkaError.new(-150, fatal: true)
  end

  def teardown
    @producer.close
    super
  end

  def test_does_not_use_idempotent_reload_path
    @producer.client.stub(:produce, ->(**_args) { raise @fatal_error }) do
      assert_raises(WaterDrop::Errors::ProduceError) do
        @producer.produce_sync(@message)
      end
    end
  end
end

class ProducerIdempotenceFatalErrorOnNonIdempotentProducerTest < WaterDropTest::Base
  def setup
    @producer = build(:producer, reload_on_idempotent_fatal_error: true)
    @topic_name = "it-#{SecureRandom.uuid}"
    @message = build(:valid_message, topic: @topic_name)
    @fatal_error = Rdkafka::RdkafkaError.new(-150, fatal: true)
  end

  def teardown
    @producer.close
    super
  end

  def test_does_not_reload_as_not_idempotent
    @producer.client.stub(:produce, ->(**_args) { raise @fatal_error }) do
      assert_raises(WaterDrop::Errors::ProduceError) do
        @producer.produce_sync(@message)
      end
    end
  end
end

class ProducerIdempotenceNonFatalErrorTest < WaterDropTest::Base
  def setup
    @producer = build(:idempotent_producer, reload_on_idempotent_fatal_error: true)
    @topic_name = "it-#{SecureRandom.uuid}"
    @message = build(:valid_message, topic: @topic_name)
    @queue_full_error = Rdkafka::RdkafkaError.new(-184, fatal: false)
  end

  def teardown
    @producer.close
    super
  end

  def test_does_not_reload_for_non_fatal_errors
    @producer.client.stub(:produce, ->(**_args) { raise @queue_full_error }) do
      assert_raises(WaterDrop::Errors::ProduceError) do
        @producer.produce_sync(@message)
      end
    end
  end
end

class ProducerIdempotenceTestingHelperTest < WaterDropTest::Base
  def setup
    @producer = build(:idempotent_producer, reload_on_idempotent_fatal_error: true)
    @topic_name = "it-#{SecureRandom.uuid}"
    @message = build(:valid_message, topic: @topic_name)
    @producer.singleton_class.include(WaterDrop::Producer::Testing)
  end

  def teardown
    @producer.close
    super
  end

  def test_trigger_fatal_error_and_query
    result = @producer.trigger_test_fatal_error(47, "Test producer epoch error")

    assert_equal 0, result

    fatal_error = @producer.fatal_error

    refute_nil fatal_error
    assert_equal 47, fatal_error[:error_code]
    assert_includes fatal_error[:error_string], "test_fatal_error"
  end

  def test_detect_fatal_error_state_after_triggering
    @producer.trigger_test_fatal_error(47, "Injected test error")

    fatal_error = @producer.fatal_error

    refute_nil fatal_error
    assert_equal 47, fatal_error[:error_code]
  end
end

class ProducerIdempotenceRealFatalErrorInjectionTest < WaterDropTest::Base
  def setup
    @producer = build(:idempotent_producer, reload_on_idempotent_fatal_error: true)
    @topic_name = "it-#{SecureRandom.uuid}"
    @message = build(:valid_message, topic: @topic_name)
    @producer.singleton_class.include(WaterDrop::Producer::Testing)
  end

  def teardown
    @producer.close
    super
  end

  def test_inject_fatal_errors_with_various_error_codes
    result = @producer.trigger_test_fatal_error(47, "Simulated producer fencing")

    assert_equal 0, result

    fatal_error = @producer.fatal_error

    refute_nil fatal_error
    assert_equal 47, fatal_error[:error_code]
    assert_kind_of String, fatal_error[:error_string]
    refute_empty fatal_error[:error_string]
  end

  def test_persists_fatal_error_state_across_multiple_queries
    @producer.trigger_test_fatal_error(47, "Test error persistence")

    first_query = @producer.fatal_error
    second_query = @producer.fatal_error
    third_query = @producer.fatal_error

    assert_equal first_query, second_query
    assert_equal second_query, third_query
    assert_equal 47, first_query[:error_code]
  end

  def test_maintains_fatal_error_state_after_triggering
    @producer.trigger_test_fatal_error(47, "Test fatal error state")

    refute_nil @producer.fatal_error

    sleep 0.1

    refute_nil @producer.fatal_error
    assert_equal 47, @producer.fatal_error[:error_code]
  end

  def test_detects_real_fatal_errors_correctly
    report = @producer.produce_sync(@message)

    assert_kind_of Rdkafka::Producer::DeliveryReport, report
    assert_nil report.error

    @producer.trigger_test_fatal_error(47, "Test reload trigger")

    fatal_error = @producer.fatal_error

    refute_nil fatal_error
    assert_equal 47, fatal_error[:error_code]
  end

  def test_can_create_new_producer_after_fatal_error
    @producer.trigger_test_fatal_error(47, "First producer fatal")

    refute_nil @producer.fatal_error

    @producer.close

    new_producer = build(:idempotent_producer)
    new_producer.singleton_class.include(WaterDrop::Producer::Testing)

    assert_nil new_producer.fatal_error

    new_producer.produce_sync(@message)

    new_producer.close
  end
end

class ProducerIdempotenceRealFatalErrorReloadInstrumentationTest < WaterDropTest::Base
  def setup
    @producer = build(
      :idempotent_producer,
      reload_on_idempotent_fatal_error: true,
      max_attempts_on_idempotent_fatal_error: 3,
      wait_backoff_on_idempotent_fatal_error: 100
    )
    @topic_name = "it-#{SecureRandom.uuid}"
    @message = build(:valid_message, topic: @topic_name)
    @reload_events = []
    @reloaded_events = []
    @error_events = []

    @producer.singleton_class.include(WaterDrop::Producer::Testing)
    @producer.monitor.subscribe("producer.reload") { |event| @reload_events << event }
    @producer.monitor.subscribe("producer.reloaded") { |event| @reloaded_events << event }
    @producer.monitor.subscribe("error.occurred") { |event| @error_events << event }
  end

  def teardown
    @producer.close
    super
  end

  def test_emits_reload_and_reloaded_events_when_fatal_error_triggers_reload
    @producer.trigger_test_fatal_error(47, "Test reload with real fatal error")

    refute_nil @producer.fatal_error
    assert_equal 47, @producer.fatal_error[:error_code]

    report = @producer.produce_sync(@message)

    assert_kind_of Rdkafka::Producer::DeliveryReport, report

    assert_operator @reload_events.size, :>=, 1
    assert_equal @producer.id, @reload_events.first[:producer_id]
    assert_kind_of Rdkafka::RdkafkaError, @reload_events.first[:error]
    assert_same true, @reload_events.first[:error].fatal?
    assert_equal 1, @reload_events.first[:attempt]
    assert_equal @producer, @reload_events.first[:caller]

    assert_operator @reloaded_events.size, :>=, 1
    assert_equal @producer.id, @reloaded_events.first[:producer_id]
    assert_equal 1, @reloaded_events.first[:attempt]
  end

  def test_emits_reload_events_when_producer_recovers
    @producer.trigger_test_fatal_error(47, "Test reload event emission")

    refute_nil @producer.fatal_error
    assert_equal 47, @producer.fatal_error[:error_code]

    report = @producer.produce_sync(@message)

    assert_kind_of Rdkafka::Producer::DeliveryReport, report

    assert_operator @reload_events.size, :>=, 1
    assert_equal 1, @reload_events.first[:attempt]
    assert_equal @producer.id, @reload_events.first[:producer_id]

    assert_operator @reloaded_events.size, :>=, 1
    assert_equal 1, @reloaded_events.first[:attempt]
    assert_equal @producer.id, @reloaded_events.first[:producer_id]
  end

  def test_includes_error_occurred_event_with_fatal_error_details
    @producer.trigger_test_fatal_error(47, "Test error.occurred event")

    report = @producer.produce_sync(@message)

    assert_kind_of Rdkafka::Producer::DeliveryReport, report

    assert_operator @error_events.size, :>=, 1

    fatal_error_events = @error_events.select do |e|
      e[:type] == "librdkafka.idempotent_fatal_error"
    end

    refute_empty fatal_error_events
    assert_equal @producer.id, fatal_error_events.first[:producer_id]
    assert_kind_of Rdkafka::RdkafkaError, fatal_error_events.first[:error]
    assert_equal 1, fatal_error_events.first[:attempt]
  end

  def test_allows_config_modification_in_reload_event_with_real_fatal_errors
    config_modified = false
    modified_config = nil

    @producer.monitor.subscribe("producer.reload") do |event|
      event[:caller].config.kafka[:"enable.idempotence"] = false
      config_modified = true
      modified_config = event[:caller].config
    end

    @producer.trigger_test_fatal_error(47, "Test config modification")

    report = @producer.produce_sync(@message)

    assert_kind_of Rdkafka::Producer::DeliveryReport, report

    assert_same true, config_modified
    refute_nil modified_config
    assert_same false, modified_config.kafka[:"enable.idempotence"]
    assert_operator @reload_events.size, :>=, 1
    assert_operator @reloaded_events.size, :>=, 1
  end
end

class ProducerIdempotenceReloadDisabledTest < WaterDropTest::Base
  def setup
    @producer = build(
      :idempotent_producer,
      reload_on_idempotent_fatal_error: false,
      max_attempts_on_idempotent_fatal_error: 3,
      wait_backoff_on_idempotent_fatal_error: 100
    )
    @topic_name = "it-#{SecureRandom.uuid}"
    @message = build(:valid_message, topic: @topic_name)
    @reload_events = []
    @reloaded_events = []

    @producer.singleton_class.include(WaterDrop::Producer::Testing)
    @producer.monitor.subscribe("producer.reload") { |event| @reload_events << event }
    @producer.monitor.subscribe("producer.reloaded") { |event| @reloaded_events << event }
  end

  def teardown
    @producer.close
    super
  end

  def test_keeps_throwing_errors_without_reload
    initial_client = @producer.client

    @producer.trigger_test_fatal_error(47, "Fatal error with reload disabled")

    initial_client.stub(:produce, lambda { |**_args|
      raise Rdkafka::RdkafkaError.new(-150, fatal: true)
    }) do
      assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_sync(@message) }
      assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_sync(@message) }
      assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_sync(@message) }
    end

    assert_empty @reload_events
    assert_empty @reloaded_events
  end

  def test_does_not_attempt_reload_with_multiple_produce_attempts
    initial_client = @producer.client
    produce_attempts = 0

    @producer.trigger_test_fatal_error(47, "Fatal error without reload")

    initial_client.stub(:produce, lambda { |**_args|
      produce_attempts += 1
      raise Rdkafka::RdkafkaError.new(-150, fatal: true)
    }) do
      5.times do
        @producer.produce_sync(@message)
      rescue WaterDrop::Errors::ProduceError
        # Expected to fail
      end
    end

    assert_equal 5, produce_attempts
    assert_empty @reload_events
    assert_empty @reloaded_events

    refute_nil @producer.fatal_error
    assert_equal 47, @producer.fatal_error[:error_code]
  end
end

class ProducerIdempotenceProduceAfterFatalStateTest < WaterDropTest::Base
  def setup
    @producer = build(
      :idempotent_producer,
      reload_on_idempotent_fatal_error: false
    )
    @topic_name = "it-#{SecureRandom.uuid}"
    @message = build(:valid_message, topic: @topic_name)
    @messages = Array.new(3) { build(:valid_message, topic: @topic_name) }

    @producer.singleton_class.include(WaterDrop::Producer::Testing)
    @producer.trigger_test_fatal_error(47, "Producer in fatal state")
  end

  def teardown
    @producer.close
    super
  end

  def test_produce_sync_raises_error_in_fatal_state
    error = nil

    begin
      @producer.produce_sync(@message)
    rescue WaterDrop::Errors::ProduceError => e
      error = e
    end

    refute_nil error
    assert_kind_of Rdkafka::RdkafkaError, error.cause
    assert_same true, error.cause.fatal?

    refute_nil @producer.fatal_error
    assert_equal 47, @producer.fatal_error[:error_code]
  end

  def test_produce_async_raises_error_in_fatal_state
    error = nil

    begin
      @producer.produce_async(@message)
    rescue WaterDrop::Errors::ProduceError => e
      error = e
    end

    refute_nil error
    assert_kind_of Rdkafka::RdkafkaError, error.cause
    assert_same true, error.cause.fatal?

    refute_nil @producer.fatal_error
  end

  def test_produce_many_sync_raises_error_in_fatal_state
    error = nil

    begin
      @producer.produce_many_sync(@messages)
    rescue WaterDrop::Errors::ProduceManyError => e
      error = e
    end

    refute_nil error
    assert_kind_of Rdkafka::RdkafkaError, error.cause
    assert_same true, error.cause.fatal?

    refute_nil @producer.fatal_error
  end

  def test_produce_many_async_raises_error_in_fatal_state
    error = nil

    begin
      @producer.produce_many_async(@messages)
    rescue WaterDrop::Errors::ProduceManyError => e
      error = e
    end

    refute_nil error
    assert_kind_of Rdkafka::RdkafkaError, error.cause
    assert_same true, error.cause.fatal?

    refute_nil @producer.fatal_error
  end

  def test_all_produce_methods_fail_consistently_in_fatal_state
    3.times do
      error = assert_raises(WaterDrop::Errors::ProduceError) do
        @producer.produce_sync(@message)
      end
      assert_kind_of Rdkafka::RdkafkaError, error.cause
      assert_same true, error.cause.fatal?

      error = assert_raises(WaterDrop::Errors::ProduceError) do
        @producer.produce_async(@message)
      end
      assert_kind_of Rdkafka::RdkafkaError, error.cause
      assert_same true, error.cause.fatal?

      error = assert_raises(WaterDrop::Errors::ProduceManyError) do
        @producer.produce_many_sync(@messages)
      end
      assert_kind_of Rdkafka::RdkafkaError, error.cause
      assert_same true, error.cause.fatal?

      error = assert_raises(WaterDrop::Errors::ProduceManyError) do
        @producer.produce_many_async(@messages)
      end
      assert_kind_of Rdkafka::RdkafkaError, error.cause
      assert_same true, error.cause.fatal?
    end

    refute_nil @producer.fatal_error
    assert_equal 47, @producer.fatal_error[:error_code]
  end

  def test_producer_remains_unusable_after_fatal_error_without_reload
    assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_sync(@message) }

    refute_nil @producer.fatal_error

    assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_sync(@message) }
    assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_async(@message) }

    assert_equal 47, @producer.fatal_error[:error_code]
  end
end
