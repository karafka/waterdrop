# frozen_string_literal: true

# rubocop:disable RSpec/VerifiedDoubles, RSpec/MessageSpies
# We use unverified doubles because the FD APIs (background_queue_fd, enable_queue_io_events)
# may not exist in the current karafka-rdkafka version
RSpec.describe_current do
  # Since Poller is a singleton, we need to be careful with testing
  # We'll test the instance methods through the singleton
  subject(:poller) { described_class.instance }

  let(:producer_id) { "test-producer-#{SecureRandom.hex(6)}" }

  # Use doubles instead of instance_doubles because the FD APIs
  # may not exist in the current karafka-rdkafka version
  let(:producer) do
    double(
      :waterdrop_producer,
      id: producer_id,
      config: config,
      monitor: monitor
    )
  end

  let(:config) do
    double(
      :waterdrop_config,
      polling: polling_config
    )
  end

  let(:polling_config) do
    double(:polling_config, fd: fd_config)
  end

  let(:fd_config) do
    double(:fd_config, max_time: 100, periodic_poll_interval: 1000)
  end

  let(:monitor) do
    double(:waterdrop_monitor).tap do |m|
      allow(m).to receive(:instrument)
    end
  end

  let(:client) do
    double(
      :rdkafka_producer,
      background_queue_fd: nil,
      enable_queue_io_events: nil,
      poll: 0,
      poll_nb: 0,
      poll_drain_nb: false,
      queue_size: 0
    )
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
      expect(monitor).to receive(:instrument).with(
        "poller.producer_registered",
        producer_id: producer_id
      )

      poller.register(producer, client)
    end
  end

  describe "#unregister" do
    before do
      poller.register(producer, client)
    end

    it "instruments producer unregistration" do
      expect(monitor).to receive(:instrument).with(
        "poller.producer_unregistered",
        producer_id: producer_id
      )

      poller.unregister(producer)
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
        double(
          :waterdrop_producer,
          id: "test-producer-2",
          config: config,
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
end
# rubocop:enable RSpec/VerifiedDoubles, RSpec/MessageSpies
