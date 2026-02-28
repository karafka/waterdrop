# frozen_string_literal: true

class WaterDropContractsConfigTest < WaterDropTest::Base
  def setup
    @config = {
      id: SecureRandom.uuid,
      logger: Logger.new(File::NULL),
      monitor: WaterDrop::Instrumentation::Monitor.new,
      deliver: false,
      client_class: WaterDrop::Clients::Rdkafka,
      max_payload_size: 1024 * 1024,
      max_wait_timeout: 1,
      wait_on_queue_full: true,
      wait_backoff_on_queue_full: 1,
      wait_timeout_on_queue_full: 10,
      wait_backoff_on_transaction_command: 15,
      max_attempts_on_transaction_command_format: 5,
      instrument_on_wait_queue_full: true,
      max_attempts_on_transaction_command: 1,
      idle_disconnect_timeout: 0,
      reload_on_idempotent_fatal_error: false,
      wait_backoff_on_idempotent_fatal_error: 5_000,
      max_attempts_on_idempotent_fatal_error: 5,
      reload_on_transaction_fatal_error: true,
      wait_backoff_on_transaction_fatal_error: 1_000,
      max_attempts_on_transaction_fatal_error: 10,
      non_reloadable_errors: %i[fenced],
      oauth: {
        token_provider_listener: false
      },
      polling: {
        mode: :thread,
        poller: nil,
        fd: {
          max_time: 100,
          periodic_poll_interval: 1000
        }
      },
      kafka: {
        "bootstrap.servers": "#{BOOTSTRAP_SERVERS},#{BOOTSTRAP_SERVERS}"
      }
    }
  end

  def contract_result
    WaterDrop::Contracts::Config.new.call(@config)
  end

  def contract_errors
    contract_result.errors.to_h
  end

  def test_valid_config
    assert_predicate contract_result, :success?
  end

  def test_id_missing
    @config.delete(:id)

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:id]
  end

  def test_id_nil
    @config[:id] = nil

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:id]
  end

  def test_id_not_a_string
    @config[:id] = rand

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:id]
  end

  def test_monitor_missing
    @config.delete(:monitor)

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:monitor]
  end

  def test_monitor_nil
    @config[:monitor] = nil

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:monitor]
  end

  def test_client_class_missing
    @config.delete(:client_class)

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:client_class]
  end

  def test_client_class_nil
    @config[:client_class] = nil

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:client_class]
  end

  def test_logger_missing
    @config.delete(:logger)

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:logger]
  end

  def test_logger_nil
    @config[:logger] = nil

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:logger]
  end

  def test_deliver_missing
    @config.delete(:deliver)

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:deliver]
  end

  def test_deliver_nil
    @config[:deliver] = nil

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:deliver]
  end

  def test_deliver_not_boolean
    @config[:deliver] = rand

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:deliver]
  end

  def test_kafka_missing
    @config.delete(:kafka)

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:kafka]
  end

  def test_kafka_empty_hash
    @config[:kafka] = {}

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:kafka]
  end

  def test_kafka_non_symbol_key
    @config[:kafka] = { "not_a_symbol" => true }

    refute_predicate contract_result, :success?
  end

  def test_max_payload_size_nil
    @config[:max_payload_size] = nil

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:max_payload_size]
  end

  def test_max_payload_size_negative_int
    @config[:max_payload_size] = -1

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:max_payload_size]
  end

  def test_max_payload_size_negative_float
    @config[:max_payload_size] = -0.1

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:max_payload_size]
  end

  def test_max_payload_size_zero
    @config[:max_payload_size] = 0

    refute_predicate contract_result, :success?
  end

  def test_max_payload_size_positive_int
    @config[:max_payload_size] = 1

    assert_predicate contract_result, :success?
  end

  def test_max_payload_size_positive_float
    @config[:max_payload_size] = 1.1

    refute_predicate contract_result, :success?
  end

  def test_max_attempts_on_transaction_command_nil
    @config[:max_attempts_on_transaction_command] = nil

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:max_attempts_on_transaction_command]
  end

  def test_max_attempts_on_transaction_command_negative_int
    @config[:max_attempts_on_transaction_command] = -1

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:max_attempts_on_transaction_command]
  end

  def test_max_attempts_on_transaction_command_negative_float
    @config[:max_attempts_on_transaction_command] = -0.1

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:max_attempts_on_transaction_command]
  end

  def test_max_attempts_on_transaction_command_zero
    @config[:max_attempts_on_transaction_command] = 0

    refute_predicate contract_result, :success?
  end

  def test_max_attempts_on_transaction_command_positive_int
    @config[:max_attempts_on_transaction_command] = 1

    assert_predicate contract_result, :success?
  end

  def test_max_attempts_on_transaction_command_positive_float
    @config[:max_attempts_on_transaction_command] = 1.1

    refute_predicate contract_result, :success?
  end

  def test_max_wait_timeout_missing
    @config.delete(:max_wait_timeout)

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:max_wait_timeout]
  end

  def test_max_wait_timeout_nil
    @config[:max_wait_timeout] = nil

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:max_wait_timeout]
  end

  def test_max_wait_timeout_negative_int
    @config[:max_wait_timeout] = -1

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:max_wait_timeout]
  end

  def test_max_wait_timeout_negative_float
    @config[:max_wait_timeout] = -0.1

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:max_wait_timeout]
  end

  def test_max_wait_timeout_zero
    @config[:max_wait_timeout] = 0

    assert_predicate contract_result, :success?
  end

  def test_max_wait_timeout_positive_int
    @config[:max_wait_timeout] = 1

    assert_predicate contract_result, :success?
  end

  def test_max_wait_timeout_positive_float
    @config[:max_wait_timeout] = 1.1

    assert_predicate contract_result, :success?
  end

  def test_wait_on_queue_full_not_boolean
    @config[:wait_on_queue_full] = 0

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:wait_on_queue_full]
  end

  def test_reload_on_transaction_fatal_error_not_boolean
    @config[:reload_on_transaction_fatal_error] = 0

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:reload_on_transaction_fatal_error]
  end

  def test_instrument_on_wait_queue_full_not_boolean
    @config[:instrument_on_wait_queue_full] = 0

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:instrument_on_wait_queue_full]
  end

  def test_wait_backoff_on_queue_full_not_numeric
    @config[:wait_backoff_on_queue_full] = "na"

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:wait_backoff_on_queue_full]
  end

  def test_wait_backoff_on_queue_full_less_than_zero
    @config[:wait_backoff_on_queue_full] = -1

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:wait_backoff_on_queue_full]
  end

  def test_wait_backoff_on_transaction_command_not_numeric
    @config[:wait_backoff_on_transaction_command] = "na"

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:wait_backoff_on_transaction_command]
  end

  def test_wait_backoff_on_transaction_command_less_than_zero
    @config[:wait_backoff_on_transaction_command] = -1

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:wait_backoff_on_transaction_command]
  end

  def test_wait_timeout_on_queue_full_not_numeric
    @config[:wait_timeout_on_queue_full] = "na"

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:wait_timeout_on_queue_full]
  end

  def test_wait_timeout_on_queue_full_less_than_zero
    @config[:wait_timeout_on_queue_full] = -1

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:wait_timeout_on_queue_full]
  end

  def test_oauth_token_provider_listener_does_not_respond_to_callback
    @config[:oauth][:token_provider_listener] = true

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:"oauth.token_provider_listener"]
  end

  def test_oauth_token_provider_listener_responds_to_callback
    listener_class = Class.new do
      def on_oauthbearer_token_refresh(_)
      end
    end
    @config[:oauth][:token_provider_listener] = listener_class.new

    assert_predicate contract_result, :success?
  end

  def test_idle_disconnect_timeout_missing
    @config.delete(:idle_disconnect_timeout)

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:idle_disconnect_timeout]
  end

  def test_idle_disconnect_timeout_nil
    @config[:idle_disconnect_timeout] = nil

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:idle_disconnect_timeout]
  end

  def test_idle_disconnect_timeout_not_integer
    @config[:idle_disconnect_timeout] = 30.5

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:idle_disconnect_timeout]
  end

  def test_idle_disconnect_timeout_negative
    @config[:idle_disconnect_timeout] = -1000

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:idle_disconnect_timeout]
  end

  def test_idle_disconnect_timeout_below_minimum
    @config[:idle_disconnect_timeout] = 29_999

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:idle_disconnect_timeout]
  end

  def test_idle_disconnect_timeout_zero_disabled
    @config[:idle_disconnect_timeout] = 0

    assert_predicate contract_result, :success?
  end

  def test_idle_disconnect_timeout_exactly_minimum
    @config[:idle_disconnect_timeout] = 30_000

    assert_predicate contract_result, :success?
  end

  def test_idle_disconnect_timeout_above_minimum
    @config[:idle_disconnect_timeout] = 60_000

    assert_predicate contract_result, :success?
  end

  def test_non_reloadable_errors_missing
    @config.delete(:non_reloadable_errors)

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:non_reloadable_errors]
  end

  def test_non_reloadable_errors_nil
    @config[:non_reloadable_errors] = nil

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:non_reloadable_errors]
  end

  def test_non_reloadable_errors_not_array
    @config[:non_reloadable_errors] = :fenced

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:non_reloadable_errors]
  end

  def test_non_reloadable_errors_contains_non_symbol_values
    @config[:non_reloadable_errors] = [:fenced, "string_value", 123]

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:non_reloadable_errors]
  end

  def test_non_reloadable_errors_empty_array
    @config[:non_reloadable_errors] = []

    assert_predicate contract_result, :success?
  end

  def test_non_reloadable_errors_array_of_symbols
    @config[:non_reloadable_errors] = %i[fenced some_other_error]

    assert_predicate contract_result, :success?
  end

  def test_non_reloadable_errors_default_value
    @config[:non_reloadable_errors] = %i[fenced]

    assert_predicate contract_result, :success?
  end

  def test_polling_mode_missing
    @config[:polling].delete(:mode)

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:"polling.mode"]
  end

  def test_polling_mode_invalid_symbol
    @config[:polling][:mode] = :invalid

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:"polling.mode"]
  end

  def test_polling_mode_thread
    @config[:polling][:mode] = :thread

    assert_predicate contract_result, :success?
  end

  def test_polling_mode_fd
    @config[:polling][:mode] = :fd

    assert_predicate contract_result, :success?
  end

  def test_polling_poller_nil
    @config[:polling][:poller] = nil

    assert_predicate contract_result, :success?
  end

  def test_polling_poller_valid_with_fd_mode
    @config[:polling][:mode] = :fd
    @config[:polling][:poller] = WaterDrop::Polling::Poller.new

    assert_predicate contract_result, :success?
  end

  def test_polling_poller_set_with_thread_mode
    @config[:polling][:mode] = :thread
    @config[:polling][:poller] = WaterDrop::Polling::Poller.new

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:"polling.poller"]
  end

  def test_polling_poller_not_a_poller_instance
    @config[:polling][:mode] = :fd
    @config[:polling][:poller] = "invalid"

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:"polling.poller"]
  end

  def test_polling_fd_max_time_missing
    @config[:polling][:fd].delete(:max_time)

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:"polling.fd.max_time"]
  end

  def test_polling_fd_max_time_not_integer
    @config[:polling][:fd][:max_time] = "100"

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:"polling.fd.max_time"]
  end

  def test_polling_fd_max_time_zero
    @config[:polling][:fd][:max_time] = 0

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:"polling.fd.max_time"]
  end

  def test_polling_fd_max_time_negative
    @config[:polling][:fd][:max_time] = -1

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:"polling.fd.max_time"]
  end

  def test_polling_fd_max_time_positive
    @config[:polling][:fd][:max_time] = 100

    assert_predicate contract_result, :success?
  end
end
