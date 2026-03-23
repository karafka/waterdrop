# frozen_string_literal: true

describe_current do
  before do
    @config = described_class.new
    @topic_name = "it-#{SPEC_HASH}-#{SecureRandom.hex(6)}"
  end

  describe "#setup" do
    context "when configuration has errors" do
      before do
        @error_class = WaterDrop::Errors::ConfigurationInvalidError
      end

      it "raise ConfigurationInvalidError exception" do
        error = assert_raises(@error_class) do
          described_class.new.setup { |config| config.kafka = { "a" => true } }
        end
        assert_kind_of(@error_class, error)
      end
    end

    context "when configuration is valid" do
      before do
        @kafka_config = { :"bootstrap.servers" => BOOTSTRAP_SERVERS, rand.to_s.to_sym => rand }
      end

      it "not raise ConfigurationInvalidError exception" do
        @config.setup { |config| config.kafka = @kafka_config }
      end
    end

    context "when we try to create and use transactional producer without idempotence" do
      before do
        @producer = build(:transactional_producer, idempotent: false)
      end

      it "expect not to allow it" do
        assert_raises(Rdkafka::Config::ClientCreationError) do
          @producer.produce_sync(topic: @topic_name, payload: "test")
        end
      end
    end

    context "when kafka configuration is frozen" do
      before do
        @frozen_kafka_config = {
          "bootstrap.servers": BOOTSTRAP_SERVERS,
          "client.id": "test-client"
        }.freeze
      end

      it "not raise FrozenError when setting frozen kafka config" do
        @config.setup { |config| config.kafka = @frozen_kafka_config }
      end
    end

    context "when reload_on_idempotent_fatal_error is configured" do
      it "allows setting to true" do
        @config.setup do |config|
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          config.reload_on_idempotent_fatal_error = true
        end
      end

      it "allows setting to false" do
        @config.setup do |config|
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          config.reload_on_idempotent_fatal_error = false
        end
      end

      it "defaults to false" do
        @config.setup { |config| config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }

        refute(@config.config.reload_on_idempotent_fatal_error)
      end
    end

    context "when wait_backoff_on_idempotent_fatal_error is configured" do
      it "allows setting a positive value" do
        @config.setup do |config|
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          config.wait_backoff_on_idempotent_fatal_error = 10_000
        end
      end

      it "defaults to 5000" do
        @config.setup { |config| config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }

        assert_equal(5_000, @config.config.wait_backoff_on_idempotent_fatal_error)
      end
    end

    context "when max_attempts_on_idempotent_fatal_error is configured" do
      it "allows setting a positive value" do
        @config.setup do |config|
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          config.max_attempts_on_idempotent_fatal_error = 10
        end
      end

      it "defaults to 5" do
        @config.setup { |config| config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }

        assert_equal(5, @config.config.max_attempts_on_idempotent_fatal_error)
      end
    end

    context "when wait_backoff_on_transaction_fatal_error is configured" do
      it "allows setting a positive value" do
        @config.setup do |config|
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          config.wait_backoff_on_transaction_fatal_error = 2_000
        end
      end

      it "defaults to 1000" do
        @config.setup { |config| config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }

        assert_equal(1_000, @config.config.wait_backoff_on_transaction_fatal_error)
      end
    end

    context "when max_attempts_on_transaction_fatal_error is configured" do
      it "allows setting a positive value" do
        @config.setup do |config|
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          config.max_attempts_on_transaction_fatal_error = 10
        end
      end

      it "defaults to 10" do
        @config.setup { |config| config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }

        assert_equal(10, @config.config.max_attempts_on_transaction_fatal_error)
      end
    end

    context "when polling.mode is configured" do
      it "allows setting to :fd" do
        @config.setup do |config|
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          config.polling.mode = :fd
        end
      end

      it "allows setting to :thread" do
        @config.setup do |config|
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          config.polling.mode = :thread
        end
      end

      it "defaults to :thread" do
        @config.setup { |config| config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }

        assert_equal(:thread, @config.config.polling.mode)
      end
    end

    context "when polling.fd.max_time is configured" do
      it "allows setting a positive value" do
        @config.setup do |config|
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          config.polling.fd.max_time = 200
        end
      end

      it "defaults to 100" do
        @config.setup { |config| config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }

        assert_equal(100, @config.config.polling.fd.max_time)
      end

      it "validates that max_time must be greater than 0" do
        contract = WaterDrop::Contracts::Config.new
        # Build a minimal valid config with invalid max_time
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

        refute_predicate(contract.call(invalid_config), :success?)
      end
    end
  end
end
