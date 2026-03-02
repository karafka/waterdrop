# frozen_string_literal: true

describe_current do
  before do
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
    @contract_result = described_class.new.call(@config)
    @contract_errors = @contract_result.errors.to_h
  end

  describe "when config is valid" do
    it { assert_predicate @contract_result, :success? }
  end

  describe "when id is missing" do
    before {
      @config.delete(:id)
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:id] }
  end

  describe "when id is nil" do
    before {
      @config[:id] = nil
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:id] }
  end

  describe "when id is not a string" do
    before {
      @config[:id] = rand
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:id] }
  end

  describe "when monitor is missing" do
    before {
      @config.delete(:monitor)
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:monitor] }
  end

  describe "when monitor is nil" do
    before {
      @config[:monitor] = nil
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:monitor] }
  end

  describe "when client_class is missing" do
    before {
      @config.delete(:client_class)
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:client_class] }
  end

  describe "when client_class is nil" do
    before {
      @config[:client_class] = nil
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:client_class] }
  end

  describe "when logger is missing" do
    before {
      @config.delete(:logger)
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:logger] }
  end

  describe "when logger is nil" do
    before {
      @config[:logger] = nil
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:logger] }
  end

  describe "when deliver is missing" do
    before {
      @config.delete(:deliver)
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:deliver] }
  end

  describe "when deliver is nil" do
    before {
      @config[:deliver] = nil
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:deliver] }
  end

  describe "when deliver is not a string" do
    before {
      @config[:deliver] = rand
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:deliver] }
  end

  describe "when kafka is missing" do
    before {
      @config.delete(:kafka)
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:kafka] }
  end

  describe "when kafka is an empty hash" do
    before {
      @config[:kafka] = {}
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:kafka] }
  end

  describe "when kafka hash is present" do
    describe "when there is a non-symbol key setting" do
      before {
        @config[:kafka] = { "not_a_symbol" => true }
        @contract_result = described_class.new.call(@config)
        @contract_errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
    end
  end

  describe "when max_payload_size is nil" do
    before {
      @config[:max_payload_size] = nil
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:max_payload_size] }
  end

  describe "when max_payload_size is a negative int" do
    before {
      @config[:max_payload_size] = -1
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:max_payload_size] }
  end

  describe "when max_payload_size is a negative float" do
    before {
      @config[:max_payload_size] = -0.1
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:max_payload_size] }
  end

  describe "when max_payload_size is 0" do
    before {
      @config[:max_payload_size] = 0
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
  end

  describe "when max_payload_size is positive int" do
    before {
      @config[:max_payload_size] = 1
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { assert_predicate @contract_result, :success? }
  end

  describe "when max_payload_size is positive float" do
    before {
      @config[:max_payload_size] = 1.1
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
  end

  describe "when max_attempts_on_transaction_command is nil" do
    before {
      @config[:max_attempts_on_transaction_command] = nil
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:max_attempts_on_transaction_command] }
  end

  describe "when max_attempts_on_transaction_command is a negative int" do
    before {
      @config[:max_attempts_on_transaction_command] = -1
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:max_attempts_on_transaction_command] }
  end

  describe "when max_attempts_on_transaction_command is a negative float" do
    before {
      @config[:max_attempts_on_transaction_command] = -0.1
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:max_attempts_on_transaction_command] }
  end

  describe "when max_attempts_on_transaction_command is 0" do
    before {
      @config[:max_attempts_on_transaction_command] = 0
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
  end

  describe "when max_attempts_on_transaction_command is positive int" do
    before {
      @config[:max_attempts_on_transaction_command] = 1
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { assert_predicate @contract_result, :success? }
  end

  describe "when max_attempts_on_transaction_command is positive float" do
    before {
      @config[:max_attempts_on_transaction_command] = 1.1
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
  end

  describe "when max_wait_timeout is missing" do
    before {
      @config.delete(:max_wait_timeout)
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:max_wait_timeout] }
  end

  describe "when max_wait_timeout is nil" do
    before {
      @config[:max_wait_timeout] = nil
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:max_wait_timeout] }
  end

  describe "when max_wait_timeout is a negative int" do
    before {
      @config[:max_wait_timeout] = -1
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:max_wait_timeout] }
  end

  describe "when max_wait_timeout is a negative float" do
    before {
      @config[:max_wait_timeout] = -0.1
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:max_wait_timeout] }
  end

  describe "when max_wait_timeout is 0" do
    before {
      @config[:max_wait_timeout] = 0
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { assert_predicate @contract_result, :success? }
  end

  describe "when max_wait_timeout is positive int" do
    before {
      @config[:max_wait_timeout] = 1
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { assert_predicate @contract_result, :success? }
  end

  describe "when max_wait_timeout is positive float" do
    before {
      @config[:max_wait_timeout] = 1.1
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { assert_predicate @contract_result, :success? }
  end

  describe "when wait_on_queue_full is not a boolean" do
    before {
      @config[:wait_on_queue_full] = 0
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:wait_on_queue_full] }
  end

  describe "when reload_on_transaction_fatal_error is not a boolean" do
    before {
      @config[:reload_on_transaction_fatal_error] = 0
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:reload_on_transaction_fatal_error] }
  end

  describe "when instrument_on_wait_queue_full is not a boolean" do
    before {
      @config[:instrument_on_wait_queue_full] = 0
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:instrument_on_wait_queue_full] }
  end

  describe "when wait_backoff_on_queue_full is not a numeric" do
    before {
      @config[:wait_backoff_on_queue_full] = "na"
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:wait_backoff_on_queue_full] }
  end

  describe "when wait_backoff_on_queue_full is less than 0" do
    before {
      @config[:wait_backoff_on_queue_full] = -1
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:wait_backoff_on_queue_full] }
  end

  describe "when wait_backoff_on_transaction_command is not a numeric" do
    before {
      @config[:wait_backoff_on_transaction_command] = "na"
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:wait_backoff_on_transaction_command] }
  end

  describe "when wait_backoff_on_transaction_command is less than 0" do
    before {
      @config[:wait_backoff_on_transaction_command] = -1
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:wait_backoff_on_transaction_command] }
  end

  describe "when wait_timeout_on_queue_full is not a numeric" do
    before {
      @config[:wait_timeout_on_queue_full] = "na"
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:wait_timeout_on_queue_full] }
  end

  describe "when wait_timeout_on_queue_full is less than 0" do
    before {
      @config[:wait_timeout_on_queue_full] = -1
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:wait_timeout_on_queue_full] }
  end

  describe "when oauth token_provider_listener does not respond to on_oauthbearer_token_refresh" do
    before {
      @config[:oauth][:token_provider_listener] = true
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:"oauth.token_provider_listener"] }
  end

  describe "when oauth token_provider_listener responds to on_oauthbearer_token_refresh" do
    before do
      listener = Class.new do
        def on_oauthbearer_token_refresh(_)
        end
      end
      @config[:oauth][:token_provider_listener] = listener.new
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    end

    it { assert_predicate @contract_result, :success? }
  end

  describe "when idle_disconnect_timeout is missing" do
    before {
      @config.delete(:idle_disconnect_timeout)
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:idle_disconnect_timeout] }
  end

  describe "when idle_disconnect_timeout is nil" do
    before {
      @config[:idle_disconnect_timeout] = nil
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:idle_disconnect_timeout] }
  end

  describe "when idle_disconnect_timeout is not an integer" do
    before {
      @config[:idle_disconnect_timeout] = 30.5
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:idle_disconnect_timeout] }
  end

  describe "when idle_disconnect_timeout is a negative integer" do
    before {
      @config[:idle_disconnect_timeout] = -1000
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:idle_disconnect_timeout] }
  end

  describe "when idle_disconnect_timeout is below minimum (30 seconds)" do
    before {
      @config[:idle_disconnect_timeout] = 29_999
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:idle_disconnect_timeout] }
  end

  describe "when idle_disconnect_timeout is zero (disabled)" do
    before {
      @config[:idle_disconnect_timeout] = 0
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { assert_predicate @contract_result, :success? }
  end

  describe "when idle_disconnect_timeout is exactly minimum (30 seconds)" do
    before {
      @config[:idle_disconnect_timeout] = 30_000
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { assert_predicate @contract_result, :success? }
  end

  describe "when idle_disconnect_timeout is above minimum" do
    before {
      @config[:idle_disconnect_timeout] = 60_000
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { assert_predicate @contract_result, :success? }
  end

  describe "when non_reloadable_errors is missing" do
    before {
      @config.delete(:non_reloadable_errors)
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:non_reloadable_errors] }
  end

  describe "when non_reloadable_errors is nil" do
    before {
      @config[:non_reloadable_errors] = nil
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:non_reloadable_errors] }
  end

  describe "when non_reloadable_errors is not an array" do
    before {
      @config[:non_reloadable_errors] = :fenced
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:non_reloadable_errors] }
  end

  describe "when non_reloadable_errors contains non-symbol values" do
    before {
      @config[:non_reloadable_errors] = [:fenced, "string_value", 123]
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:non_reloadable_errors] }
  end

  describe "when non_reloadable_errors is an empty array" do
    before {
      @config[:non_reloadable_errors] = []
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { assert_predicate @contract_result, :success? }
  end

  describe "when non_reloadable_errors is an array of symbols" do
    before {
      @config[:non_reloadable_errors] = %i[fenced some_other_error]
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { assert_predicate @contract_result, :success? }
  end

  describe "when non_reloadable_errors is default value" do
    before {
      @config[:non_reloadable_errors] = %i[fenced]
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { assert_predicate @contract_result, :success? }
  end

  describe "when polling.mode is missing" do
    before {
      @config[:polling].delete(:mode)
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:"polling.mode"] }
  end

  describe "when polling.mode is not a valid symbol" do
    before {
      @config[:polling][:mode] = :invalid
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:"polling.mode"] }
  end

  describe "when polling.mode is :thread" do
    before {
      @config[:polling][:mode] = :thread
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { assert_predicate @contract_result, :success? }
  end

  describe "when polling.mode is :fd" do
    before {
      @config[:polling][:mode] = :fd
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { assert_predicate @contract_result, :success? }
  end

  describe "when polling.poller is nil" do
    before {
      @config[:polling][:poller] = nil
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { assert_predicate @contract_result, :success? }
  end

  describe "when polling.poller is a valid Poller instance with :fd mode" do
    before do
      @config[:polling][:mode] = :fd
      @config[:polling][:poller] = WaterDrop::Polling::Poller.new
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    end

    it { assert_predicate @contract_result, :success? }
  end

  describe "when polling.poller is set with :thread mode" do
    before do
      @config[:polling][:mode] = :thread
      @config[:polling][:poller] = WaterDrop::Polling::Poller.new
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    end

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:"polling.poller"] }
  end

  describe "when polling.poller is not a Poller instance" do
    before do
      @config[:polling][:mode] = :fd
      @config[:polling][:poller] = "invalid"
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    end

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:"polling.poller"] }
  end

  describe "when polling.fd.max_time is missing" do
    before {
      @config[:polling][:fd].delete(:max_time)
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:"polling.fd.max_time"] }
  end

  describe "when polling.fd.max_time is not an integer" do
    before {
      @config[:polling][:fd][:max_time] = "100"
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:"polling.fd.max_time"] }
  end

  describe "when polling.fd.max_time is zero" do
    before {
      @config[:polling][:fd][:max_time] = 0
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:"polling.fd.max_time"] }
  end

  describe "when polling.fd.max_time is negative" do
    before {
      @config[:polling][:fd][:max_time] = -1
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:"polling.fd.max_time"] }
  end

  describe "when polling.fd.max_time is positive" do
    before {
      @config[:polling][:fd][:max_time] = 100
      @contract_result = described_class.new.call(@config)
      @contract_errors = @contract_result.errors.to_h
    }

    it { assert_predicate @contract_result, :success? }
  end
end
