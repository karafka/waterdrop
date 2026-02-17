# frozen_string_literal: true

# rubocop:disable RSpec/VerifiedDoubles
# We use unverified doubles because the FD APIs (enable_queue_io_events)
# may not exist in the current karafka-rdkafka version
RSpec.describe_current do
  subject(:state) { described_class.new(producer_id, client, monitor, max_poll_time, periodic_poll_interval) }

  let(:producer_id) { "test-producer-#{SecureRandom.hex(6)}" }
  let(:max_poll_time) { 100 }
  let(:periodic_poll_interval) { 1000 }

  let(:client) do
    double(:rdkafka_producer, enable_queue_io_events: nil, poll_drain_nb: false, queue_size: 0)
  end

  let(:monitor) do
    double(:waterdrop_monitor).tap do |m|
      allow(m).to receive(:instrument)
    end
  end

  after { state.close }

  describe "#initialize" do
    it "stores the producer_id" do
      expect(state.producer_id).to eq(producer_id)
    end

    it "stores the monitor" do
      expect(state.monitor).to eq(monitor)
    end

    it "creates an IO for monitoring" do
      expect(state.io).to be_an(IO)
    end

    it "is not closed by default" do
      expect(state.closed?).to be(false)
    end

    it "is not closing by default" do
      expect(state.closing?).to be(false)
    end

    context "when enable_queue_io_events raises an error" do
      let(:failing_client) do
        double(:rdkafka_producer, poll_drain_nb: false, queue_size: 0).tap do |c|
          allow(c).to receive(:enable_queue_io_events).and_raise(StandardError, "test error")
        end
      end

      it "raises the error (FD mode requires working queue pipe)" do
        expect do
          described_class.new(producer_id, failing_client, monitor, max_poll_time, periodic_poll_interval)
        end.to raise_error(StandardError, "test error")
      end
    end
  end

  describe "#io" do
    it "returns the queue pipe reader" do
      expect(state.io).to respond_to(:read_nonblock)
    end
  end

  describe "#poll" do
    it "calls poll_drain_nb on the client" do
      allow(client).to receive(:poll_drain_nb).and_return(false)
      state.poll
      expect(client).to have_received(:poll_drain_nb).with(max_poll_time)
    end

    it "returns the result from poll_drain_nb" do
      allow(client).to receive(:poll_drain_nb).and_return(true)
      expect(state.poll).to be(true)
    end
  end

  describe "#queue_empty?" do
    it "returns true when queue_size is zero" do
      allow(client).to receive(:queue_size).and_return(0)
      expect(state.queue_empty?).to be(true)
    end

    it "returns false when queue_size is non-zero" do
      allow(client).to receive(:queue_size).and_return(5)
      expect(state.queue_empty?).to be(false)
    end
  end

  describe "#mark_polled! and #needs_periodic_poll?" do
    # Use larger intervals to account for CI/macOS timing variability
    let(:periodic_poll_interval) { 100 }

    it "needs periodic poll when never polled" do
      expect(state.needs_periodic_poll?).to be(true)
    end

    it "does not need periodic poll immediately after polling" do
      state.mark_polled!
      expect(state.needs_periodic_poll?).to be(false)
    end

    it "needs periodic poll after interval passes" do
      state.mark_polled!
      sleep(0.15)
      expect(state.needs_periodic_poll?).to be(true)
    end

    context "with large interval to avoid timing issues" do
      let(:periodic_poll_interval) { 10_000 }

      it "does not need periodic poll if interval has not passed" do
        state.mark_polled!
        sleep(0.01)
        expect(state.needs_periodic_poll?).to be(false)
      end
    end
  end

  describe "#signal_close" do
    it "sets closing flag to true" do
      state.signal_close
      expect(state.closing?).to be(true)
    end

    it "makes the IO readable" do
      state.signal_close
      ready = IO.select([state.io], nil, nil, 0)
      expect(ready).not_to be_nil
    end

    it "does not raise when called multiple times" do
      expect do
        3.times { state.signal_close }
      end.not_to raise_error
    end
  end

  describe "#signal_continue" do
    it "makes the IO readable" do
      state.signal_continue
      ready = IO.select([state.io], nil, nil, 0)
      expect(ready).not_to be_nil
    end

    it "does not raise when called multiple times" do
      expect do
        3.times { state.signal_continue }
      end.not_to raise_error
    end
  end

  describe "#closing?" do
    it "returns false initially" do
      expect(state.closing?).to be(false)
    end

    it "returns true after signal_close" do
      state.signal_close
      expect(state.closing?).to be(true)
    end
  end

  describe "#drain" do
    it "drains all data from the queue pipe" do
      3.times { state.signal_continue }
      state.drain
      # Pipe should be empty now
      ready = IO.select([state.io], nil, nil, 0)
      expect(ready).to be_nil
    end

    it "does not raise when pipe is empty" do
      expect { state.drain }.not_to raise_error
    end
  end

  describe "#close" do
    it "marks the state as closed" do
      state.close
      expect(state.closed?).to be(true)
    end

    it "closes the IO" do
      io = state.io
      state.close
      expect(io.closed?).to be(true)
    end

    it "does not raise when called multiple times" do
      expect do
        3.times { state.close }
      end.not_to raise_error
    end
  end

  describe "#wait_for_close" do
    it "returns immediately if already closed" do
      state.close
      expect { state.wait_for_close }.not_to raise_error
    end

    it "waits until close is called from another thread" do
      closed = false

      Thread.new do
        sleep(0.05)
        state.close
        closed = true
      end

      state.wait_for_close
      expect(closed).to be(true)
    end
  end

  describe "#closed?" do
    context "when not closed" do
      it "returns false" do
        expect(state.closed?).to be(false)
      end
    end

    context "when closed" do
      before { state.close }

      it "returns true" do
        expect(state.closed?).to be(true)
      end
    end
  end
end
# rubocop:enable RSpec/VerifiedDoubles
