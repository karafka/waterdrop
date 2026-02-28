# frozen_string_literal: true

class ProducerTestingTest < WaterDropTest::Base
  def setup
    @producer = build(:idempotent_producer)
    @producer.singleton_class.include(WaterDrop::Producer::Testing)
  end

  def teardown
    @producer.close
    super
  end

  def test_trigger_test_fatal_error
    result = @producer.trigger_test_fatal_error(47, "Test producer fencing")

    assert_equal 0, result

    fatal_error = @producer.fatal_error

    refute_nil fatal_error
    assert_equal 47, fatal_error[:error_code]
    assert_includes fatal_error[:error_string], "test_fatal_error"
  end

  def test_trigger_different_error_codes
    @producer.trigger_test_fatal_error(64, "Test invalid producer ID")

    fatal_error = @producer.fatal_error

    refute_nil fatal_error
    assert_equal 64, fatal_error[:error_code]
  end

  def test_includes_reason_in_error_context
    custom_reason = "Custom test scenario for fencing"
    @producer.trigger_test_fatal_error(47, custom_reason)

    refute_nil @producer.fatal_error
  end

  def test_automatically_includes_rdkafka_testing_on_client
    @producer.trigger_test_fatal_error(47, "test")
  end

  def test_fatal_error_returns_nil_when_no_error
    assert_nil @producer.fatal_error
  end

  def test_fatal_error_returns_hash_with_error_details
    @producer.trigger_test_fatal_error(47, "Test error")

    error = @producer.fatal_error

    assert_kind_of Hash, error
    assert_includes error.keys, :error_code
    assert_includes error.keys, :error_string
  end

  def test_fatal_error_returns_correct_error_code
    @producer.trigger_test_fatal_error(47, "Test error")

    error = @producer.fatal_error

    assert_equal 47, error[:error_code]
  end

  def test_fatal_error_returns_human_readable_error_string
    @producer.trigger_test_fatal_error(47, "Test error")

    error = @producer.fatal_error

    assert_kind_of String, error[:error_string]
    refute_empty error[:error_string]
  end

  def test_fatal_error_returns_consistent_results
    @producer.trigger_test_fatal_error(47, "Test error")

    first_call = @producer.fatal_error
    second_call = @producer.fatal_error

    assert_equal first_call, second_call
  end
end

class ProducerTestingIntegrationWithReloadTest < WaterDropTest::Base
  def setup
    @producer = build(
      :idempotent_producer,
      reload_on_idempotent_fatal_error: true,
      max_attempts_on_idempotent_fatal_error: 3,
      wait_backoff_on_idempotent_fatal_error: 100
    )
    @topic_name = "testing-#{SecureRandom.uuid}"
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

  def test_can_trigger_fatal_errors_that_affect_produce_operations
    @producer.trigger_test_fatal_error(47, "Test producer fencing for reload")

    refute_nil @producer.fatal_error
    assert_equal 47, @producer.fatal_error[:error_code]
  end
end

class ProducerTestingIntegrationWithTransactionalTest < WaterDropTest::Base
  def setup
    @producer = build(
      :transactional_producer,
      reload_on_transaction_fatal_error: true
    )
    @producer.singleton_class.include(WaterDrop::Producer::Testing)
  end

  def teardown
    @producer.close
    super
  end

  def test_can_trigger_fatal_errors_on_transactional_producers
    result = @producer.trigger_test_fatal_error(47, "Test transactional error")

    assert_equal 0, result

    fatal_error = @producer.fatal_error

    refute_nil fatal_error
    assert_equal 47, fatal_error[:error_code]
  end
end

class ProducerTestingWithStandardProducerTest < WaterDropTest::Base
  def setup
    @producer = build(:producer)
    @producer.singleton_class.include(WaterDrop::Producer::Testing)
  end

  def teardown
    @producer.close
    super
  end

  def test_can_trigger_and_query_fatal_errors
    @producer.trigger_test_fatal_error(47, "Test on non-idempotent")

    refute_nil @producer.fatal_error
  end
end

class ProducerTestingModuleWideInclusionTest < WaterDropTest::Base
  def setup
    unless WaterDrop::Producer.included_modules.include?(WaterDrop::Producer::Testing)
      WaterDrop::Producer.include(WaterDrop::Producer::Testing)
    end
  end

  def test_makes_testing_methods_available_to_all_instances
    new_producer = build(:idempotent_producer)

    assert_respond_to new_producer, :trigger_test_fatal_error
    assert_respond_to new_producer, :fatal_error

    new_producer.close
  end
end

class ProducerTestingErrorCodesTest < WaterDropTest::Base
  def setup
    @producer = build(:idempotent_producer)
    @producer.singleton_class.include(WaterDrop::Producer::Testing)
  end

  def teardown
    @producer.close
    super
  end

  {
    47 => "INVALID_PRODUCER_EPOCH (producer fencing)",
    64 => "INVALID_PRODUCER_ID_MAPPING (invalid producer ID)"
  }.each do |code, description|
    define_method("test_works_with_error_code_#{code}_#{description.gsub(/[^a-z0-9]/i, "_")}") do
      @producer.trigger_test_fatal_error(code, "Testing #{description}")
      error = @producer.fatal_error

      refute_nil error
      assert_equal code, error[:error_code]
    end
  end
end
