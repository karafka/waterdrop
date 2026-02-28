# frozen_string_literal: true

class WaterDropConfigSetupWithErrorsTest < WaterDropTest::Base
  def test_raises_configuration_invalid_error_for_invalid_config
    error = assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
      WaterDrop::Config.new.setup { |config| config.kafka = { "a" => true } }
    end

    assert_kind_of WaterDrop::Errors::ConfigurationInvalidError, error
  end
end

class WaterDropConfigSetupWithValidConfigTest < WaterDropTest::Base
  def test_does_not_raise_for_valid_config
    kafka_config = { :"bootstrap.servers" => BOOTSTRAP_SERVERS, rand.to_s.to_sym => rand }

    WaterDrop::Config.new.setup { |config| config.kafka = kafka_config }
  end
end

class WaterDropConfigSetupTransactionalWithoutIdempotenceTest < WaterDropTest::Base
  def test_does_not_allow_transactional_producer_without_idempotence
    producer = build(:transactional_producer, idempotent: false)
    topic_name = "it-#{SecureRandom.uuid}"

    assert_raises(Rdkafka::Config::ClientCreationError) do
      producer.produce_sync(topic: topic_name, payload: "test")
    end
  end
end

class WaterDropConfigSetupFrozenKafkaConfigTest < WaterDropTest::Base
  def test_does_not_raise_for_frozen_kafka_config
    frozen_kafka_config = {
      "bootstrap.servers": BOOTSTRAP_SERVERS,
      "client.id": "test-client"
    }.freeze

    WaterDrop::Config.new.setup { |config| config.kafka = frozen_kafka_config }
  end
end

class WaterDropConfigReloadOnIdempotentFatalErrorTest < WaterDropTest::Base
  def test_allows_setting_to_true
    WaterDrop::Config.new.setup do |config|
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.reload_on_idempotent_fatal_error = true
    end
  end

  def test_allows_setting_to_false
    WaterDrop::Config.new.setup do |config|
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.reload_on_idempotent_fatal_error = false
    end
  end

  def test_defaults_to_false
    config = WaterDrop::Config.new
    config.setup { |c| c.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }

    assert_same false, config.config.reload_on_idempotent_fatal_error
  end
end

class WaterDropConfigWaitBackoffOnIdempotentFatalErrorTest < WaterDropTest::Base
  def test_allows_setting_a_positive_value
    WaterDrop::Config.new.setup do |config|
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.wait_backoff_on_idempotent_fatal_error = 10_000
    end
  end

  def test_defaults_to_5000
    config = WaterDrop::Config.new
    config.setup { |c| c.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }

    assert_equal 5_000, config.config.wait_backoff_on_idempotent_fatal_error
  end
end

class WaterDropConfigMaxAttemptsOnIdempotentFatalErrorTest < WaterDropTest::Base
  def test_allows_setting_a_positive_value
    WaterDrop::Config.new.setup do |config|
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.max_attempts_on_idempotent_fatal_error = 10
    end
  end

  def test_defaults_to_5
    config = WaterDrop::Config.new
    config.setup { |c| c.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }

    assert_equal 5, config.config.max_attempts_on_idempotent_fatal_error
  end
end

class WaterDropConfigWaitBackoffOnTransactionFatalErrorTest < WaterDropTest::Base
  def test_allows_setting_a_positive_value
    WaterDrop::Config.new.setup do |config|
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.wait_backoff_on_transaction_fatal_error = 2_000
    end
  end

  def test_defaults_to_1000
    config = WaterDrop::Config.new
    config.setup { |c| c.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }

    assert_equal 1_000, config.config.wait_backoff_on_transaction_fatal_error
  end
end

class WaterDropConfigMaxAttemptsOnTransactionFatalErrorTest < WaterDropTest::Base
  def test_allows_setting_a_positive_value
    WaterDrop::Config.new.setup do |config|
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.max_attempts_on_transaction_fatal_error = 10
    end
  end

  def test_defaults_to_10
    config = WaterDrop::Config.new
    config.setup { |c| c.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }

    assert_equal 10, config.config.max_attempts_on_transaction_fatal_error
  end
end

class WaterDropConfigPollingModeTest < WaterDropTest::Base
  def test_allows_setting_to_fd
    WaterDrop::Config.new.setup do |config|
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.polling.mode = :fd
    end
  end

  def test_allows_setting_to_thread
    WaterDrop::Config.new.setup do |config|
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.polling.mode = :thread
    end
  end

  def test_defaults_to_thread
    config = WaterDrop::Config.new
    config.setup { |c| c.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }

    assert_equal :thread, config.config.polling.mode
  end
end

class WaterDropConfigPollingFdMaxTimeTest < WaterDropTest::Base
  def test_allows_setting_a_positive_value
    WaterDrop::Config.new.setup do |config|
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.polling.fd.max_time = 200
    end
  end

  def test_defaults_to_100
    config = WaterDrop::Config.new
    config.setup { |c| c.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }

    assert_equal 100, config.config.polling.fd.max_time
  end

  def test_validates_max_time_must_be_greater_than_zero
    contract = WaterDrop::Contracts::Config.new
    invalid_config = {
      id: "test",
      logger: Logger.new(File::NULL),
      monitor: WaterDrop::Instrumentation::Monitor.new,
      deliver: true,
      client_class: WaterDrop::Clients::Rdkafka,
      max_payload_size: 1024,
      max_wait_timeout: 1000,
      wait_on_queue_full: true,
      wait_backoff_on_queue_full: 1,
      wait_timeout_on_queue_full: 10,
      wait_backoff_on_transaction_command: 15,
      max_attempts_on_transaction_command: 5,
      instrument_on_wait_queue_full: true,
      idle_disconnect_timeout: 0,
      reload_on_idempotent_fatal_error: false,
      wait_backoff_on_idempotent_fatal_error: 5000,
      max_attempts_on_idempotent_fatal_error: 5,
      reload_on_transaction_fatal_error: true,
      wait_backoff_on_transaction_fatal_error: 1000,
      max_attempts_on_transaction_fatal_error: 10,
      non_reloadable_errors: [:fenced],
      oauth: { token_provider_listener: false },
      polling: { mode: :thread, fd: { max_time: 0 } },
      kafka: { "bootstrap.servers": BOOTSTRAP_SERVERS },
      middleware: WaterDrop::Middleware.new
    }

    assert_same false, contract.call(invalid_config).success?
  end
end
