# frozen_string_literal: true

class WaterDropPollingStateTest < WaterDropTest::Base
  def setup
    @producer_id = "test-producer-#{SecureRandom.hex(6)}"
    @max_poll_time = 100
    @periodic_poll_interval = 1000

    @client = Minitest::Mock.new
    @client.expect(:enable_queue_io_events, nil, [Integer])
    @client.expect(:queue_size, 0)

    @monitor = OpenStruct.new

    @state = WaterDrop::Polling::State.new(
      @producer_id, @client, @monitor, @max_poll_time, @periodic_poll_interval
    )
  end

  def teardown
    @state.close
    super
  end

  def test_stores_producer_id
    assert_equal @producer_id, @state.producer_id
  end

  def test_stores_monitor
    assert_equal @monitor, @state.monitor
  end

  def test_creates_io_for_monitoring
    assert_kind_of IO, @state.io
  end

  def test_not_closed_by_default
    assert_same false, @state.closed?
  end

  def test_not_closing_by_default
    assert_same false, @state.closing?
  end

  def test_io_returns_queue_pipe_reader
    assert_respond_to @state.io, :read_nonblock
  end

  def test_signal_close_sets_closing_flag
    @state.signal_close

    assert_same true, @state.closing?
  end

  def test_signal_close_makes_io_readable
    @state.signal_close
    ready = IO.select([@state.io], nil, nil, 0)

    refute_nil ready
  end

  def test_signal_close_multiple_times_does_not_raise
    3.times { @state.signal_close }
  end

  def test_signal_continue_makes_io_readable
    @state.signal_continue
    ready = IO.select([@state.io], nil, nil, 0)

    refute_nil ready
  end

  def test_signal_continue_multiple_times_does_not_raise
    3.times { @state.signal_continue }
  end

  def test_closing_returns_false_initially
    assert_same false, @state.closing?
  end

  def test_closing_returns_true_after_signal_close
    @state.signal_close

    assert_same true, @state.closing?
  end

  def test_drain_clears_queue_pipe
    3.times { @state.signal_continue }
    @state.drain
    ready = IO.select([@state.io], nil, nil, 0)

    assert_nil ready
  end

  def test_drain_does_not_raise_when_empty
    @state.drain
  end

  def test_close_marks_state_as_closed
    @state.close

    assert_same true, @state.closed?
  end

  def test_close_closes_io
    io = @state.io
    @state.close

    assert_same true, io.closed?
  end

  def test_close_multiple_times_does_not_raise
    3.times { @state.close }
  end

  def test_wait_for_close_returns_immediately_if_closed
    @state.close
    @state.wait_for_close
  end

  def test_wait_for_close_waits_until_close_from_another_thread
    closed = false

    Thread.new do
      sleep(0.05)
      @state.close
      closed = true
    end

    @state.wait_for_close

    assert_same true, closed
  end

  def test_closed_returns_false_when_not_closed
    assert_same false, @state.closed?
  end

  def test_closed_returns_true_when_closed
    @state.close

    assert_same true, @state.closed?
  end
end

class WaterDropPollingStateEnableQueueEventsErrorTest < WaterDropTest::Base
  def test_raises_error_when_enable_queue_io_events_fails
    client = Minitest::Mock.new
    client.expect(:enable_queue_io_events, nil) { raise StandardError, "test error" }
    client.expect(:queue_size, 0)

    monitor = Minitest::Mock.new

    assert_raises(StandardError) do
      WaterDrop::Polling::State.new("producer-id", client, monitor, 100, 1000)
    end
  end
end

class WaterDropPollingStatePollTest < WaterDropTest::Base
  # A minimal client stub that supports enable_queue_io_events and events_poll_nb_each
  # Using a real class so Minitest's stub can override methods
  class StubClient
    def enable_queue_io_events(_fd)
    end

    def queue_size
      0
    end

    def events_poll_nb_each
    end
  end

  def setup
    @producer_id = "test-producer-#{SecureRandom.hex(6)}"
    @max_poll_time = 100
    @periodic_poll_interval = 1000

    @client = StubClient.new
    @monitor = OpenStruct.new

    @state = WaterDrop::Polling::State.new(
      @producer_id, @client, @monitor, @max_poll_time, @periodic_poll_interval
    )
  end

  def teardown
    @state.close
    super
  end

  def test_poll_calls_events_poll_nb_each
    called = false
    @client.stub(:events_poll_nb_each, ->(&_block) { called = true }) do
      @state.poll
    end

    assert called
  end

  def test_poll_returns_true_when_queue_drains
    @client.stub(:events_poll_nb_each, ->(&_block) {}) do
      assert_same true, @state.poll
    end
  end

  def test_poll_returns_false_when_timeout_reached
    long_poll = lambda { |&block|
      loop do
        result = block.call(1)
        break if result == :stop
      end
    }

    short_state = WaterDrop::Polling::State.new(
      @producer_id, @client, @monitor, 0, @periodic_poll_interval
    )

    @client.stub(:events_poll_nb_each, long_poll) do
      assert_same false, short_state.poll
    end

    short_state.close
  end
end

class WaterDropPollingStateQueueEmptyTest < WaterDropTest::Base
  # A minimal client stub that supports enable_queue_io_events and queue_size
  # Using a real class so Minitest's stub can override methods
  class StubClient
    def enable_queue_io_events(_fd)
    end

    def queue_size
      0
    end
  end

  def setup
    @producer_id = "test-producer-#{SecureRandom.hex(6)}"

    @client = StubClient.new
    @monitor = OpenStruct.new

    @state = WaterDrop::Polling::State.new(
      @producer_id, @client, @monitor, 100, 1000
    )
  end

  def teardown
    @state.close
    super
  end

  def test_queue_empty_when_size_zero
    @client.stub(:queue_size, 0) do
      assert_same true, @state.queue_empty?
    end
  end

  def test_queue_not_empty_when_size_nonzero
    @client.stub(:queue_size, 5) do
      assert_same false, @state.queue_empty?
    end
  end
end

class WaterDropPollingStatePeriodicPollTest < WaterDropTest::Base
  def setup
    @producer_id = "test-producer-#{SecureRandom.hex(6)}"
    @periodic_poll_interval = 100

    @client = Minitest::Mock.new
    @client.expect(:enable_queue_io_events, nil, [Integer])
    @client.expect(:queue_size, 0)

    @monitor = Minitest::Mock.new

    @state = WaterDrop::Polling::State.new(
      @producer_id, @client, @monitor, 100, @periodic_poll_interval
    )
  end

  def teardown
    @state.close
    super
  end

  def test_needs_periodic_poll_when_never_polled
    assert_same true, @state.needs_periodic_poll?
  end

  def test_does_not_need_periodic_poll_immediately_after_polling
    @state.mark_polled!

    assert_same false, @state.needs_periodic_poll?
  end

  def test_needs_periodic_poll_after_interval_passes
    @state.mark_polled!
    sleep(0.15)

    assert_same true, @state.needs_periodic_poll?
  end
end

class WaterDropPollingStatePeriodicPollLargeIntervalTest < WaterDropTest::Base
  def setup
    @producer_id = "test-producer-#{SecureRandom.hex(6)}"

    @client = Minitest::Mock.new
    @client.expect(:enable_queue_io_events, nil, [Integer])
    @client.expect(:queue_size, 0)

    @monitor = Minitest::Mock.new

    @state = WaterDrop::Polling::State.new(
      @producer_id, @client, @monitor, 100, 10_000
    )
  end

  def teardown
    @state.close
    super
  end

  def test_does_not_need_periodic_poll_if_interval_not_passed
    @state.mark_polled!
    sleep(0.01)

    assert_same false, @state.needs_periodic_poll?
  end
end
