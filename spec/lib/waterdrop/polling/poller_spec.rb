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
    double(:fd_config, max_time: 100)
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
  before do
    # Properly shutdown any running thread
    poller.instance_variable_set(:@shutdown, true)
    thread = poller.instance_variable_get(:@thread)
    if thread&.alive?
      # Signal wakeup to unblock IO.select
      begin
        poller.instance_variable_get(:@wakeup_write)&.write_nonblock("W")
      rescue IOError, Errno::EPIPE, Errno::EAGAIN
        nil
      end
      thread.join(1)
      thread.kill if thread.alive?
    end

    # Reset all state
    poller.instance_variable_set(:@thread, nil)
    poller.instance_variable_set(:@producers, {})
    poller.instance_variable_set(:@shutdown, false)
    poller.instance_variable_set(:@ios_dirty, true)
    poller.instance_variable_set(:@cached_ios, [])
    poller.instance_variable_set(:@cached_io_to_state, {})

    # Recreate wakeup pipe to avoid stale data
    old_read = poller.instance_variable_get(:@wakeup_read)
    old_write = poller.instance_variable_get(:@wakeup_write)

    begin
      old_read&.close
    rescue IOError
      nil
    end

    begin
      old_write&.close
    rescue IOError
      nil
    end

    new_read, new_write = IO.pipe
    poller.instance_variable_set(:@wakeup_read, new_read)
    poller.instance_variable_set(:@wakeup_write, new_write)
  end

  after do
    # Ensure thread is stopped after each test
    poller.instance_variable_set(:@shutdown, true)
    thread = poller.instance_variable_get(:@thread)
    if thread&.alive?
      begin
        poller.instance_variable_get(:@wakeup_write)&.write_nonblock("W")
      rescue IOError, Errno::EPIPE, Errno::EAGAIN
        nil
      end
      thread.join(1)
      thread.kill if thread.alive?
    end
  end

  describe "#register" do
    it "adds the producer to the registry" do
      poller.register(producer, client)
      producers = poller.instance_variable_get(:@producers)
      expect(producers).to have_key(producer_id)
    end

    it "starts the polling thread" do
      poller.register(producer, client)
      thread = poller.instance_variable_get(:@thread)
      expect(thread).to be_alive
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

  describe "#shutdown" do
    before do
      poller.register(producer, client)
    end

    it "stops the polling thread" do
      thread = poller.instance_variable_get(:@thread)
      expect(thread).to be_alive
      poller.shutdown
      expect(thread).not_to be_alive
    end
  end
end
# rubocop:enable RSpec/VerifiedDoubles, RSpec/MessageSpies
