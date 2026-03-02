# frozen_string_literal: true

describe_current do
  before do
    @monitor = described_class.new
  end

  describe "#initialize" do
    it "creates a monitor instance without errors" do
      assert_kind_of(described_class, @monitor)
    end

    it "accepts custom notifications bus parameter" do
      custom_bus = WaterDrop::Instrumentation::ClassNotifications.new
      custom_monitor = described_class.new(custom_bus)

      assert_kind_of(described_class, custom_monitor)
    end

    it "accepts namespace parameter without errors" do
      described_class.new(nil, "test")
    end
  end

  describe "#subscribe" do
    it "allows subscribing to class-level events" do
      events_received = []

      @monitor.subscribe("producer.created") do |event|
        events_received << event
      end

      @monitor.instrument("producer.created", test_data: "value")

      assert_equal(1, events_received.size)
      assert_equal("value", events_received.first[:test_data])
    end

    it "allows subscribing to producer.configured events" do
      events_received = []

      @monitor.subscribe("producer.configured") do |event|
        events_received << event
      end

      @monitor.instrument("producer.configured", producer_id: "test-id")

      assert_equal(1, events_received.size)
      assert_equal("test-id", events_received.first[:producer_id])
    end

    it "raises error when subscribing to non-class events" do
      assert_raises(Karafka::Core::Monitoring::Notifications::EventNotRegistered) do
        @monitor.subscribe("message.produced_async") do |event|
          # This should not be allowed
        end
      end
    end

    it "raises error when subscribing to instance-level events" do
      instance_events = %w[
        producer.connected
        producer.closed
        message.produced_sync
        buffer.flushed_async
        error.occurred
      ]

      instance_events.each do |event_name|
        assert_raises(Karafka::Core::Monitoring::Notifications::EventNotRegistered) do
          @monitor.subscribe(event_name) { |_event| nil }
        end
      end
    end
  end

  describe "#instrument" do
    it "instruments class-level events successfully" do
      events_received = []

      @monitor.subscribe("producer.created") do |event|
        events_received << event
      end

      result = @monitor.instrument("producer.created", producer: "test_producer") do
        "instrumented_result"
      end

      assert_equal("instrumented_result", result)
      assert_equal(1, events_received.size)
      assert_equal("test_producer", events_received.first[:producer])
    end

    it "supports instrumenting without a block" do
      events_received = []

      @monitor.subscribe("producer.configured") do |event|
        events_received << event
      end

      @monitor.instrument("producer.configured", config: "test_config")

      assert_equal(1, events_received.size)
      assert_equal("test_config", events_received.first[:config])
    end

    it "raises error when instrumenting non-class events" do
      assert_raises(Karafka::Core::Monitoring::Notifications::EventNotRegistered) do
        @monitor.instrument("message.produced_sync", message: {})
      end
    end
  end

  describe "multiple subscribers" do
    it "notifies all subscribers for the same event" do
      events_received1 = []
      events_received2 = []

      @monitor.subscribe("producer.created") do |event|
        events_received1 << event
      end

      @monitor.subscribe("producer.created") do |event|
        events_received2 << event
      end

      @monitor.instrument("producer.created", producer_id: "shared-test")

      assert_equal(1, events_received1.size)
      assert_equal(1, events_received2.size)
      assert_equal("shared-test", events_received1.first[:producer_id])
      assert_equal("shared-test", events_received2.first[:producer_id])
    end
  end

  describe "inheritance" do
    it "inherits from Karafka::Core::Monitoring::Monitor" do
      assert_operator(described_class, :<, Karafka::Core::Monitoring::Monitor)
    end
  end

  describe "integration with WaterDrop.instrumentation" do
    before do
      @events_received = []

      WaterDrop.instrumentation.subscribe("producer.created") do |event|
        @events_received << [:producer_created, event]
      end

      WaterDrop.instrumentation.subscribe("producer.configured") do |event|
        @events_received << [:producer_configured, event]
      end
    end

    after do
      # Reset the memoized instance after each test to avoid interference
      WaterDrop.instance_variable_set(:@instrumentation, nil)
    end

    describe "when creating a producer without configuration" do
      before do
        @producer = WaterDrop::Producer.new
      end

      after { @producer.close }

      it "instruments producer.created event" do
        created_events = @events_received.select { |event| event.first == :producer_created }

        assert_equal(1, created_events.size)

        event_data = created_events.first.last

        assert_equal(@producer, event_data[:producer])
        assert_nil(event_data[:producer_id]) # Not configured yet
      end

      it "does not instrument producer.configured event yet" do
        configured_events = @events_received.select { |event| event.first == :producer_configured }

        assert_empty(configured_events)
      end
    end

    describe "when creating a producer with configuration" do
      before do
        @producer = WaterDrop::Producer.new do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end
      end

      after { @producer.close }

      it "instruments both producer.created and producer.configured events" do
        created_events = @events_received.select { |event| event.first == :producer_created }
        configured_events = @events_received.select { |event| event.first == :producer_configured }

        assert_equal(1, created_events.size)
        assert_equal(1, configured_events.size)

        # Check producer.configured event data
        config_event_data = configured_events.first.last

        assert_equal(@producer, config_event_data[:producer])
        assert_equal(@producer.id, config_event_data[:producer_id])
        assert_equal(@producer.config, config_event_data[:config])
      end
    end

    describe "when creating multiple producers" do
      before do
        @producer1 = WaterDrop::Producer.new do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end

        @producer2 = WaterDrop::Producer.new do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": "localhost:9093" }
        end
      end

      after do
        @producer1.close
        @producer2.close
      end

      it "instruments events for each producer separately" do
        created_events = @events_received.select { |event| event.first == :producer_created }
        configured_events = @events_received.select { |event| event.first == :producer_configured }

        assert_equal(2, created_events.size)
        assert_equal(2, configured_events.size)

        # Verify each producer is tracked separately
        created_producers = created_events.map { |event| event.last[:producer] }
        configured_producers = configured_events.map { |event| event.last[:producer] }

        assert_includes(created_producers, @producer1)
        assert_includes(created_producers, @producer2)
        assert_includes(configured_producers, @producer1)
        assert_includes(configured_producers, @producer2)
      end
    end

    describe "integration with external libraries" do
      it "allows external libraries to hook into producer lifecycle" do
        middleware_applied = []

        # Simulate Datadog integration subscribing to events
        WaterDrop.instrumentation.subscribe("producer.configured") do |event|
          producer = event[:producer]

          # Simulate adding tracing middleware
          middleware_applied << producer.id

          # Simulate middleware addition
          producer.config.middleware.append(
            lambda do |message|
              message[:headers] ||= {}
              message[:headers]["x-trace-id"] = "test-trace-id"
              message
            end
          )
        end

        # Create a producer
        producer = WaterDrop::Producer.new do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end

        # Verify middleware was applied
        assert_includes(middleware_applied, producer.id)

        # Test that middleware actually works
        test_message = { topic: "test", payload: "test" }
        processed_message = producer.middleware.run(test_message)

        assert_equal("test-trace-id", processed_message[:headers]["x-trace-id"])
      ensure
        producer&.close
      end
    end
  end
end
