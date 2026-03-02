# frozen_string_literal: true

require "test_helper"

describe_current do
  # Since Poller is a singleton, we need to be careful with testing
  # We'll test the instance methods through the singleton
  before do
    @producer_id = "test-producer-#{SecureRandom.hex(6)}"

    # Build config using real WaterDrop config structure
    @producer_config = WaterDrop::Config.new.tap do |cfg|
      cfg.configure do |c|
        c.kafka = { "bootstrap.servers": "localhost:9092" }
        c.polling.mode = :fd
        c.polling.fd.max_time = 100
        c.polling.fd.periodic_poll_interval = 1000
      end
    end

    @instrument_calls = []
    @monitor = OpenStruct.new
    @monitor.define_singleton_method(:instrument) do |*args, **kwargs|
      # no-op for testing
    end

    @client = OpenStruct.new(queue_size: 0)
    @client.define_singleton_method(:enable_queue_io_events) { |*| nil }
    @client.define_singleton_method(:events_poll_nb_each) { |*| nil }

    @producer = OpenStruct.new(
      id: @producer_id,
      config: @producer_config.config,
      monitor: @monitor
    )

    @poller = described_class.instance

    # Reset singleton state before each test to avoid mock leaking
    @poller.shutdown!
  end

  after { @poller.shutdown! }

  describe "#register" do
    it "adds the producer to the registry" do
      @poller.register(@producer, @client)
      assert_equal(1, @poller.count)
    end

    it "starts the polling thread" do
      @poller.register(@producer, @client)
      assert_equal(true, @poller.alive?)
    end

    it "instruments producer registration" do
      instrument_calls = []
      @monitor.define_singleton_method(:instrument) do |*args, **kwargs|
        instrument_calls << [args, kwargs]
      end

      @poller.register(@producer, @client)

      registered_call = instrument_calls.find { |call| call[0].include?("poller.producer_registered") }
      refute_nil(registered_call)
      assert_equal(@producer_id, registered_call[1][:producer_id])
    end
  end

  describe "#unregister" do
    before do
      @poller.register(@producer, @client)
    end

    it "instruments producer unregistration" do
      instrument_calls = []
      @monitor.define_singleton_method(:instrument) do |*args, **kwargs|
        instrument_calls << [args, kwargs]
      end

      @poller.unregister(@producer)

      unregistered_call = instrument_calls.find { |call| call[0].include?("poller.producer_unregistered") }
      refute_nil(unregistered_call)
      assert_equal(@producer_id, unregistered_call[1][:producer_id])
    end
  end

  describe "thread lifecycle" do
    before do
      @poller.register(@producer, @client)
    end

    it "stops the polling thread when last producer unregisters" do
      assert_equal(true, @poller.alive?)

      @poller.unregister(@producer)

      assert_equal(false, @poller.alive?)
    end

    it "sets thread priority from Config" do
      original_priority = WaterDrop::Polling::Config.config.thread_priority

      begin
        WaterDrop::Polling::Config.setup do |config|
          config.thread_priority = -1
        end

        @poller.shutdown!
        @poller.register(@producer, @client)

        # Access thread via instance variable (acceptable for testing internals)
        thread = @poller.instance_variable_get(:@thread)
        assert_equal(-1, thread.priority)
      ensure
        WaterDrop::Polling::Config.setup do |config|
          config.thread_priority = original_priority
        end
      end
    end
  end

  describe "#alive?" do
    describe "when no producers are registered" do
      it "returns false" do
        assert_equal(false, @poller.alive?)
      end
    end

    describe "when a producer is registered" do
      before { @poller.register(@producer, @client) }

      it "returns true" do
        assert_equal(true, @poller.alive?)
      end
    end
  end

  describe "#count" do
    describe "when no producers are registered" do
      it "returns 0" do
        assert_equal(0, @poller.count)
      end
    end

    describe "when producers are registered" do
      before do
        @producer2 = OpenStruct.new(
          id: "test-producer-2",
          config: @producer_config.config,
          monitor: @monitor
        )

        @poller.register(@producer, @client)
        @poller.register(@producer2, @client)
      end

      it "returns the number of registered producers" do
        assert_equal(2, @poller.count)
      end
    end
  end

  describe "#shutdown!" do
    before { @poller.register(@producer, @client) }

    it "stops the polling thread" do
      assert_equal(true, @poller.alive?)
      @poller.shutdown!
      assert_equal(false, @poller.alive?)
    end

    it "clears all producers" do
      assert_equal(1, @poller.count)
      @poller.shutdown!
      assert_equal(0, @poller.count)
    end

    it "can be called multiple times safely" do
      3.times { @poller.shutdown! }
    end
  end

  describe "#in_poller_thread?" do
    describe "when called from main thread" do
      it "returns false" do
        assert_equal(false, @poller.in_poller_thread?)
      end
    end
  end

  describe "#id" do
    it "returns a unique incremental integer" do
      assert_kind_of(Integer, @poller.id)
    end
  end

  describe ".new" do
    it "creates a new poller instance (not the singleton)" do
      custom_poller = described_class.new
      refute_equal(@poller, custom_poller)
    end

    it "assigns incremental IDs to new instances" do
      poller1 = described_class.new
      poller2 = described_class.new
      assert_operator(poller2.id, :>, poller1.id)
    end

    it "includes the ID in the thread name" do
      custom_poller = described_class.new
      custom_poller.register(@producer, @client)

      thread = custom_poller.instance_variable_get(:@thread)
      assert_equal("waterdrop.poller##{custom_poller.id}", thread.name)

      custom_poller.shutdown!
    end
  end
end
