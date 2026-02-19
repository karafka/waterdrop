# frozen_string_literal: true

RSpec.describe_current do
  # Since Poller is a singleton, we need to be careful with testing
  # We'll test the instance methods through the singleton
  subject(:poller) { described_class.instance }

  let(:producer_id) { "test-producer-#{SecureRandom.hex(6)}" }

  # Build config using real WaterDrop config structure
  let(:producer_config) do
    WaterDrop::Config.new.tap do |cfg|
      cfg.configure do |c|
        c.kafka = { "bootstrap.servers": "localhost:9092" }
        c.polling.mode = :fd
        c.polling.fd.max_time = 100
        c.polling.fd.periodic_poll_interval = 1000
      end
    end
  end

  let(:producer) do
    instance_double(
      WaterDrop::Producer,
      id: producer_id,
      config: producer_config.config,
      monitor: monitor
    )
  end

  let(:monitor) do
    instance_double(WaterDrop::Instrumentation::Monitor).tap do |m|
      allow(m).to receive(:instrument)
    end
  end

  let(:client) do
    instance_double(
      Rdkafka::Producer,
      enable_queue_io_events: nil,
      queue_size: 0
    ).tap do |c|
      allow(c).to receive(:events_poll_nb_each)
    end
  end

  # Reset singleton state before each test to avoid mock leaking
  before { poller.shutdown! }

  after { poller.shutdown! }

  describe "#register" do
    it "adds the producer to the registry" do
      poller.register(producer, client)
      expect(poller.count).to eq(1)
    end

    it "starts the polling thread" do
      poller.register(producer, client)
      expect(poller.alive?).to be(true)
    end

    it "instruments producer registration" do
      poller.register(producer, client)

      expect(monitor).to have_received(:instrument).with(
        "poller.producer_registered",
        producer_id: producer_id
      )
    end
  end

  describe "#unregister" do
    before do
      poller.register(producer, client)
    end

    it "instruments producer unregistration" do
      poller.unregister(producer)

      expect(monitor).to have_received(:instrument).with(
        "poller.producer_unregistered",
        producer_id: producer_id
      )
    end
  end

  describe "thread lifecycle" do
    before do
      poller.register(producer, client)
    end

    it "stops the polling thread when last producer unregisters" do
      expect(poller.alive?).to be(true)

      poller.unregister(producer)

      expect(poller.alive?).to be(false)
    end

    it "sets thread priority from Config" do
      original_priority = WaterDrop::Polling::Config.config.thread_priority

      begin
        WaterDrop::Polling::Config.setup do |config|
          config.thread_priority = -1
        end

        poller.shutdown!
        poller.register(producer, client)

        # Access thread via instance variable (acceptable for testing internals)
        thread = poller.instance_variable_get(:@thread)
        expect(thread.priority).to eq(-1)
      ensure
        WaterDrop::Polling::Config.setup do |config|
          config.thread_priority = original_priority
        end
      end
    end
  end

  describe "#alive?" do
    context "when no producers are registered" do
      it "returns false" do
        expect(poller.alive?).to be(false)
      end
    end

    context "when a producer is registered" do
      before { poller.register(producer, client) }

      it "returns true" do
        expect(poller.alive?).to be(true)
      end
    end
  end

  describe "#count" do
    context "when no producers are registered" do
      it "returns 0" do
        expect(poller.count).to eq(0)
      end
    end

    context "when producers are registered" do
      let(:producer2) do
        instance_double(
          WaterDrop::Producer,
          id: "test-producer-2",
          config: producer_config.config,
          monitor: monitor
        )
      end

      before do
        poller.register(producer, client)
        poller.register(producer2, client)
      end

      it "returns the number of registered producers" do
        expect(poller.count).to eq(2)
      end
    end
  end

  describe "#shutdown!" do
    before { poller.register(producer, client) }

    it "stops the polling thread" do
      expect(poller.alive?).to be(true)
      poller.shutdown!
      expect(poller.alive?).to be(false)
    end

    it "clears all producers" do
      expect(poller.count).to eq(1)
      poller.shutdown!
      expect(poller.count).to eq(0)
    end

    it "can be called multiple times safely" do
      expect { 3.times { poller.shutdown! } }.not_to raise_error
    end
  end

  describe "#in_poller_thread?" do
    context "when called from main thread" do
      it "returns false" do
        expect(poller.in_poller_thread?).to be(false)
      end
    end
  end

  describe "#id" do
    it "returns a unique incremental integer" do
      expect(poller.id).to be_a(Integer)
    end
  end

  describe ".new" do
    it "creates a new poller instance (not the singleton)" do
      custom_poller = described_class.new
      expect(custom_poller).not_to eq(poller)
    end

    it "assigns incremental IDs to new instances" do
      poller1 = described_class.new
      poller2 = described_class.new
      expect(poller2.id).to be > poller1.id
    end

    it "includes the ID in the thread name" do
      custom_poller = described_class.new
      custom_poller.register(producer, client)

      thread = custom_poller.instance_variable_get(:@thread)
      expect(thread.name).to eq("waterdrop.poller##{custom_poller.id}")

      custom_poller.shutdown!
    end
  end
end
