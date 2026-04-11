# frozen_string_literal: true

describe_current do
  before do
    @producer = WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
    @client = described_class.new(@producer)
  end

  after do
    @client.close
    @producer.close
  end

  it { assert_kind_of(Rdkafka::Producer, @client) }

  describe "statistics on-demand behavior" do
    # Builds a producer without going through the lazy client so we can control listeners
    # on the monitor before the underlying rdkafka client gets constructed. We use the
    # `:thread` polling mode so that we don't need to register with the FD poller, which
    # would require a real native rdkafka client.
    def build_producer(kafka_overrides = {})
      WaterDrop::Producer.new do |config|
        config.deliver = true
        config.polling.mode = :thread
        config.kafka = {
          "bootstrap.servers": BOOTSTRAP_SERVERS
        }.merge(kafka_overrides)
      end
    end

    # Captures the kafka config hash passed into Rdkafka::Config.new when the underlying
    # client is built. We stub the constructor so we never actually hit a broker. Also
    # captures whether the statistics callback was registered (before we clean it up).
    # @return [Hash{Symbol => Object}] with :kafka_config and :stats_callback_registered
    def capture_rdkafka_config(producer)
      captured = {}
      fake_producer_double = Object.new

      # Minimal surface used by Clients::Rdkafka.new after config creation
      fake_producer_double.define_singleton_method(:name) { "rdkafka-producer-fake" }
      fake_producer_double.define_singleton_method(:start) {}
      fake_producer_double.define_singleton_method(:close) {}
      fake_producer_double.define_singleton_method(:delivery_callback=) { |_| }

      fake_config = Object.new
      fake_config.define_singleton_method(:producer) do |**_opts|
        fake_producer_double
      end

      ::Rdkafka::Config
        .stubs(:new)
        .with do |hash|
          captured.replace(hash)
          true
        end
        .returns(fake_config)

      described_class.new(producer)

      {
        kafka_config: captured,
        stats_callback_registered: statistics_callback_registered?(producer.id)
      }
    ensure
      ::Karafka::Core::Instrumentation.statistics_callbacks.delete(producer.id)
      ::Karafka::Core::Instrumentation.error_callbacks.delete(producer.id)
      ::Karafka::Core::Instrumentation.oauthbearer_token_refresh_callbacks.delete(producer.id)
    end

    # Reads whether a statistics callback is currently registered for the given producer id
    def statistics_callback_registered?(producer_id)
      callbacks = ::Karafka::Core::Instrumentation
        .statistics_callbacks
        .instance_variable_get(:@callbacks)

      callbacks.key?(producer_id)
    end

    describe "when there is no listener for statistics.emitted at build time" do
      before do
        @producer_on_demand = build_producer("statistics.interval.ms": 5_000)
        @captured = capture_rdkafka_config(@producer_on_demand)
      end

      after { @producer_on_demand.close }

      it "expect to force statistics.interval.ms to 0" do
        assert_equal(0, @captured[:kafka_config][:"statistics.interval.ms"])
      end

      it "expect not to register the statistics callback" do
        refute(@captured[:stats_callback_registered])
      end

      it "expect not to mutate the original kafka config on the producer" do
        assert_equal(5_000, @producer_on_demand.config.kafka[:"statistics.interval.ms"])
      end

      it "expect to freeze the statistics listeners on the monitor" do
        assert_predicate(@producer_on_demand.monitor, :statistics_listeners_frozen?)
      end

      it "expect a late subscription to statistics.emitted to raise" do
        assert_raises(WaterDrop::Errors::StatisticsNotEnabledError) do
          @producer_on_demand.monitor.subscribe("statistics.emitted") { |_event| }
        end
      end
    end

    describe "when a listener for statistics.emitted is subscribed before build" do
      before do
        @producer_on_demand = build_producer("statistics.interval.ms": 5_000)
        @producer_on_demand.monitor.subscribe("statistics.emitted") { |_event| }

        @captured = capture_rdkafka_config(@producer_on_demand)
      end

      after { @producer_on_demand.close }

      it "expect to keep the user-configured statistics.interval.ms" do
        assert_equal(5_000, @captured[:kafka_config][:"statistics.interval.ms"])
      end

      it "expect to register the statistics callback" do
        assert(@captured[:stats_callback_registered])
      end

      it "expect not to freeze the statistics listeners on the monitor" do
        refute_predicate(@producer_on_demand.monitor, :statistics_listeners_frozen?)
      end

      it "expect a second late subscription to statistics.emitted to still work" do
        @producer_on_demand.monitor.subscribe("statistics.emitted") { |_event| }

        # Two subscribers now: the initial one plus the late one
        assert_equal(2, @producer_on_demand.monitor.listeners["statistics.emitted"].size)
      end
    end

    describe "when user explicitly disables statistics with interval.ms = 0" do
      before do
        @producer_on_demand = build_producer("statistics.interval.ms": 0)
        @captured = capture_rdkafka_config(@producer_on_demand)
      end

      after { @producer_on_demand.close }

      it "expect to keep statistics disabled" do
        assert_equal(0, @captured[:kafka_config][:"statistics.interval.ms"])
      end

      it "expect not to register the statistics callback" do
        refute(@captured[:stats_callback_registered])
      end

      it "expect to freeze the statistics listeners on the monitor" do
        assert_predicate(@producer_on_demand.monitor, :statistics_listeners_frozen?)
      end
    end

    describe "when user explicitly disables statistics but subscribes a listener" do
      before do
        @producer_on_demand = build_producer("statistics.interval.ms": 0)
        @producer_on_demand.monitor.subscribe("statistics.emitted") { |_event| }
        @captured = capture_rdkafka_config(@producer_on_demand)
      end

      after { @producer_on_demand.close }

      it "expect to leave interval.ms as 0 since the user explicitly disabled it" do
        assert_equal(0, @captured[:kafka_config][:"statistics.interval.ms"])
      end

      it "expect not to register the statistics callback because interval is 0" do
        refute(@captured[:stats_callback_registered])
      end

      it "expect to freeze the statistics listeners on the monitor" do
        assert_predicate(@producer_on_demand.monitor, :statistics_listeners_frozen?)
      end
    end
  end
end
