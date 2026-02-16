# frozen_string_literal: true

# rubocop:disable RSpec/VerifiedDoubles
# We use unverified doubles because the FD APIs (enable_queue_io_events)
# may not exist in the current karafka-rdkafka version
RSpec.describe_current do
  subject(:state) { described_class.new(producer_id, client, monitor, max_poll_time) }

  let(:producer_id) { "test-producer-#{SecureRandom.hex(6)}" }
  let(:max_poll_time) { 100 }

  let(:client) do
    double(:rdkafka_producer, enable_queue_io_events: nil)
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

    it "stores the client" do
      expect(state.client).to eq(client)
    end

    it "stores the max_poll_time" do
      expect(state.max_poll_time).to eq(max_poll_time)
    end

    it "creates a control pipe" do
      expect(state.control_read).to be_a(IO)
      expect(state.control_write).to be_a(IO)
    end

    it "creates a continue pipe" do
      expect(state.continue_read).to be_a(IO)
    end

    it "is not closed by default" do
      expect(state.closed?).to be(false)
    end

    context "when enable_queue_io_events raises an error" do
      let(:client) do
        double(:rdkafka_producer).tap do |c|
          allow(c).to receive(:enable_queue_io_events).and_raise(StandardError, "test error")
        end
      end

      it "reports error via monitor" do
        state

        expect(monitor).to have_received(:instrument).with(
          "error.occurred",
          producer_id: producer_id,
          type: "poller.queue_pipe_setup",
          error: an_instance_of(StandardError)
        )
      end

      it "does not set queue_read" do
        expect(state.queue_read).to be_nil
      end
    end

    context "when client supports enable_queue_io_events" do
      let(:client) do
        double(:rdkafka_producer, enable_queue_io_events: nil)
      end

      it "sets up queue_read pipe" do
        expect(state.queue_read).to be_a(IO)
      end
    end
  end

  describe "#signal_close" do
    it "writes to the control pipe" do
      state.signal_close
      expect(state.control_read.read_nonblock(1)).to eq("X")
    end

    it "does not raise when called multiple times" do
      expect do
        3.times { state.signal_close }
      end.not_to raise_error
    end

    it "does nothing if already closed" do
      state.close
      expect { state.signal_close }.not_to raise_error
    end
  end

  describe "#signal_continue" do
    it "writes to the continue pipe" do
      state.signal_continue
      expect(state.continue_read.read_nonblock(1)).to eq("C")
    end

    it "does not raise when called multiple times" do
      expect do
        3.times { state.signal_continue }
      end.not_to raise_error
    end

    it "does nothing if already closed" do
      state.close
      expect { state.signal_continue }.not_to raise_error
    end
  end

  describe "#drain_continue_pipe" do
    it "drains all data from the continue pipe" do
      3.times { state.signal_continue }
      state.drain_continue_pipe
      # Pipe should be empty now - read_nonblock should raise
      expect { state.continue_read.read_nonblock(1) }.to raise_error(IO::WaitReadable)
    end

    it "does not raise when pipe is empty" do
      expect { state.drain_continue_pipe }.not_to raise_error
    end
  end

  describe "#close" do
    it "marks the state as closed" do
      state.close
      expect(state.closed?).to be(true)
    end

    it "closes the control pipes" do
      control_read = state.control_read
      control_write = state.control_write

      state.close

      expect(control_read.closed?).to be(true)
      expect(control_write.closed?).to be(true)
    end

    it "closes the continue pipes" do
      continue_read = state.continue_read

      state.close

      expect(continue_read.closed?).to be(true)
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
      expect { state.wait_for_close(0.1) }.not_to raise_error
    end

    it "waits until close is called from another thread" do
      closed = false

      Thread.new do
        sleep(0.05)
        state.close
        closed = true
      end

      state.wait_for_close(1)
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

  describe "#ios" do
    it "includes the control_read IO" do
      expect(state.ios).to include(state.control_read)
    end

    context "when queue_read is available" do
      let(:client) do
        double(:rdkafka_producer, enable_queue_io_events: nil)
      end

      it "includes control_read, continue_read, and queue_read" do
        expect(state.ios).to include(state.control_read)
        expect(state.ios).to include(state.continue_read)
        expect(state.ios).to include(state.queue_read)
        expect(state.ios.size).to eq(3)
      end
    end
  end
end
# rubocop:enable RSpec/VerifiedDoubles
