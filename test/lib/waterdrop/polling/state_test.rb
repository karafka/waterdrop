# frozen_string_literal: true

describe_current do
  before do
    @producer_id = "test-producer-#{SecureRandom.hex(6)}"
    @max_poll_time = 100
    @periodic_poll_interval = 1000

    @poll_calls = []
    @client = OpenStruct.new(queue_size: 0)
    @client.define_singleton_method(:enable_queue_io_events) { |*| nil }
    @client.define_singleton_method(:events_poll_nb_each) { |*| nil }

    @monitor = OpenStruct.new
    @monitor.define_singleton_method(:instrument) { |*args, **kwargs| nil }

    @state = described_class.new(@producer_id, @client, @monitor, @max_poll_time, @periodic_poll_interval)
  end

  after { @state.close }

  describe "#initialize" do
    it "stores the producer_id" do
      assert_equal(@producer_id, @state.producer_id)
    end

    it "stores the monitor" do
      assert_equal(@monitor, @state.monitor)
    end

    it "creates an IO for monitoring" do
      assert_kind_of(IO, @state.io)
    end

    it "is not closed by default" do
      refute_predicate(@state, :closed?)
    end

    it "is not closing by default" do
      refute_predicate(@state, :closing?)
    end

    describe "when enable_queue_io_events raises an error" do
      it "raises the error (FD mode requires working queue pipe)" do
        failing_client = OpenStruct.new(queue_size: 0)
        failing_client.define_singleton_method(:events_poll_nb_each) { |*| nil }
        failing_client.define_singleton_method(:enable_queue_io_events) { |*| raise StandardError, "test error" }

        error = assert_raises(StandardError) do
          described_class.new(@producer_id, failing_client, @monitor, @max_poll_time, @periodic_poll_interval)
        end
        assert_equal("test error", error.message)
      end
    end
  end

  describe "#io" do
    it "returns the queue pipe reader" do
      assert_respond_to(@state.io, :read_nonblock)
    end
  end

  describe "#poll" do
    it "calls events_poll_nb_each on the client" do
      poll_called = false
      @client.define_singleton_method(:events_poll_nb_each) { |*| poll_called = true }

      @state.poll

      assert(poll_called)
    end

    it "returns true when queue drains before timeout" do
      @client.define_singleton_method(:events_poll_nb_each) { |*| nil }

      assert(@state.poll)
    end

    it "returns false when timeout is reached" do
      # Simulate a long-running poll that exceeds the deadline
      @client.define_singleton_method(:events_poll_nb_each) do |&block|
        # Yield multiple times to simulate processing events
        loop do
          result = block.call(1)
          break if result == :stop
        end
      end

      # Use a very short max_poll_time to trigger timeout
      short_state = described_class.new(@producer_id, @client, @monitor, 0, @periodic_poll_interval)

      refute(short_state.poll)
      short_state.close
    end
  end

  describe "#queue_empty?" do
    it "returns true when queue_size is zero" do
      @client.define_singleton_method(:queue_size) { 0 }

      assert_predicate(@state, :queue_empty?)
    end

    it "returns false when queue_size is non-zero" do
      @client.define_singleton_method(:queue_size) { 5 }

      refute_predicate(@state, :queue_empty?)
    end
  end

  describe "#mark_polled! and #needs_periodic_poll?" do
    # Use larger intervals to account for CI/macOS timing variability
    before do
      @state.close
      @periodic_poll_interval = 100
      @state = described_class.new(@producer_id, @client, @monitor, @max_poll_time, @periodic_poll_interval)
    end

    it "needs periodic poll when never polled" do
      assert_predicate(@state, :needs_periodic_poll?)
    end

    it "does not need periodic poll immediately after polling" do
      @state.mark_polled!

      refute_predicate(@state, :needs_periodic_poll?)
    end

    it "needs periodic poll after interval passes" do
      @state.mark_polled!
      sleep(0.15)

      assert_predicate(@state, :needs_periodic_poll?)
    end

    describe "with large interval to avoid timing issues" do
      before do
        @state.close
        @periodic_poll_interval = 10_000
        @state = described_class.new(@producer_id, @client, @monitor, @max_poll_time, @periodic_poll_interval)
      end

      it "does not need periodic poll if interval has not passed" do
        @state.mark_polled!
        sleep(0.01)

        refute_predicate(@state, :needs_periodic_poll?)
      end
    end
  end

  describe "#signal_close" do
    it "sets closing flag to true" do
      @state.signal_close

      assert_predicate(@state, :closing?)
    end

    it "makes the IO readable" do
      @state.signal_close
      ready = IO.select([@state.io], nil, nil, 0)

      refute_nil(ready)
    end

    it "does not raise when called multiple times" do
      3.times { @state.signal_close }
    end
  end

  describe "#signal_continue" do
    it "makes the IO readable" do
      @state.signal_continue
      ready = IO.select([@state.io], nil, nil, 0)

      refute_nil(ready)
    end

    it "does not raise when called multiple times" do
      3.times { @state.signal_continue }
    end
  end

  describe "#closing?" do
    it "returns false initially" do
      refute_predicate(@state, :closing?)
    end

    it "returns true after signal_close" do
      @state.signal_close

      assert_predicate(@state, :closing?)
    end
  end

  describe "#drain" do
    it "drains all data from the queue pipe" do
      3.times { @state.signal_continue }
      @state.drain
      # Pipe should be empty now
      ready = IO.select([@state.io], nil, nil, 0)

      assert_nil(ready)
    end

    it "does not raise when pipe is empty" do
      @state.drain
    end
  end

  describe "#close" do
    it "marks the state as closed" do
      @state.close

      assert_predicate(@state, :closed?)
    end

    it "closes the IO" do
      io = @state.io
      @state.close

      assert_predicate(io, :closed?)
    end

    it "does not raise when called multiple times" do
      3.times { @state.close }
    end
  end

  describe "#wait_for_close" do
    it "returns immediately if already closed" do
      @state.close
      @state.wait_for_close
    end

    it "waits until close is called from another thread" do
      closed = false

      Thread.new do
        sleep(0.05)
        @state.close
        closed = true
      end

      @state.wait_for_close

      assert(closed)
    end
  end

  describe "#closed?" do
    describe "when not closed" do
      it "returns false" do
        refute_predicate(@state, :closed?)
      end
    end

    describe "when closed" do
      before { @state.close }

      it "returns true" do
        assert_predicate(@state, :closed?)
      end
    end
  end
end
