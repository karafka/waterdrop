# frozen_string_literal: true

RSpec.describe_current do
  subject(:config) { described_class.new }

  let(:topic_name) { "it-#{SecureRandom.uuid}" }

  describe "#setup" do
    context "when configuration has errors" do
      let(:error_class) { WaterDrop::Errors::ConfigurationInvalidError }
      let(:setup) { described_class.new.setup { |config| config.kafka = { "a" => true } } }

      it "raise ConfigurationInvalidError exception" do
        expect { setup }.to raise_error do |error|
          expect(error).to be_a(error_class)
        end
      end
    end

    context "when configuration is valid" do
      let(:kafka_config) do
        { :"bootstrap.servers" => BOOTSTRAP_SERVERS, rand.to_s.to_sym => rand }
      end

      it "not raise ConfigurationInvalidError exception" do
        expect { config.setup { |config| config.kafka = kafka_config } }
          .not_to raise_error
      end
    end

    context "when we try to create and use transactional producer without idempotence" do
      subject(:producer) { build(:transactional_producer, idempotent: false) }

      it "expect not to allow it" do
        expect do
          producer.produce_sync(topic: topic_name, payload: "test")
        end.to raise_error(Rdkafka::Config::ClientCreationError)
      end
    end

    context "when kafka configuration is frozen" do
      let(:frozen_kafka_config) do
        {
          "bootstrap.servers": BOOTSTRAP_SERVERS,
          "client.id": "test-client"
        }.freeze
      end

      it "not raise FrozenError when setting frozen kafka config" do
        expect { config.setup { |config| config.kafka = frozen_kafka_config } }
          .not_to raise_error
      end
    end

    context "when reload_on_idempotent_fatal_error is configured" do
      it "allows setting to true" do
        expect do
          config.setup do |config|
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
            config.reload_on_idempotent_fatal_error = true
          end
        end.not_to raise_error
      end

      it "allows setting to false" do
        expect do
          config.setup do |config|
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
            config.reload_on_idempotent_fatal_error = false
          end
        end.not_to raise_error
      end

      it "defaults to false" do
        config.setup { |config| config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }
        expect(config.config.reload_on_idempotent_fatal_error).to be(false)
      end
    end

    context "when wait_backoff_on_idempotent_fatal_error is configured" do
      it "allows setting a positive value" do
        expect do
          config.setup do |config|
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
            config.wait_backoff_on_idempotent_fatal_error = 10_000
          end
        end.not_to raise_error
      end

      it "defaults to 5000" do
        config.setup { |config| config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }
        expect(config.config.wait_backoff_on_idempotent_fatal_error).to eq(5_000)
      end
    end

    context "when max_attempts_on_idempotent_fatal_error is configured" do
      it "allows setting a positive value" do
        expect do
          config.setup do |config|
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
            config.max_attempts_on_idempotent_fatal_error = 10
          end
        end.not_to raise_error
      end

      it "defaults to 5" do
        config.setup { |config| config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }
        expect(config.config.max_attempts_on_idempotent_fatal_error).to eq(5)
      end
    end

    context "when wait_backoff_on_transaction_fatal_error is configured" do
      it "allows setting a positive value" do
        expect do
          config.setup do |config|
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
            config.wait_backoff_on_transaction_fatal_error = 2_000
          end
        end.not_to raise_error
      end

      it "defaults to 1000" do
        config.setup { |config| config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }
        expect(config.config.wait_backoff_on_transaction_fatal_error).to eq(1_000)
      end
    end

    context "when max_attempts_on_transaction_fatal_error is configured" do
      it "allows setting a positive value" do
        expect do
          config.setup do |config|
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
            config.max_attempts_on_transaction_fatal_error = 10
          end
        end.not_to raise_error
      end

      it "defaults to 10" do
        config.setup { |config| config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }
        expect(config.config.max_attempts_on_transaction_fatal_error).to eq(10)
      end
    end

    context "when polling.mode is configured" do
      it "allows setting to :fd" do
        expect do
          config.setup do |config|
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
            config.polling.mode = :fd
          end
        end.not_to raise_error
      end

      it "allows setting to :thread" do
        expect do
          config.setup do |config|
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
            config.polling.mode = :thread
          end
        end.not_to raise_error
      end

      it "defaults to :thread" do
        config.setup { |config| config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }
        expect(config.config.polling.mode).to eq(:thread)
      end
    end

    context "when polling.fd.max_time is configured" do
      it "allows setting a positive value" do
        expect do
          config.setup do |config|
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
            config.polling.fd.max_time = 200
          end
        end.not_to raise_error
      end

      it "defaults to 100" do
        config.setup { |config| config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS } }
        expect(config.config.polling.fd.max_time).to eq(100)
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
        expect(contract.call(invalid_config).success?).to be(false)
      end
    end
  end
end
