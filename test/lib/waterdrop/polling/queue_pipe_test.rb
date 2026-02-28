# frozen_string_literal: true

class WaterDropPollingQueuePipeTest < WaterDropTest::Base
  def setup
    @client = Minitest::Mock.new
    @client.expect(:enable_queue_io_events, nil, [Integer])
    @pipe = WaterDrop::Polling::QueuePipe.new(@client)
  end

  def teardown
    @pipe.close
  rescue
    # Ignore if pipe was never created or already closed
  ensure
    super
  end

  def test_creates_readable_io
    assert_kind_of IO, @pipe.reader
  end

  def test_signal_makes_pipe_readable
    @pipe.signal
    ready = IO.select([@pipe.reader], nil, nil, 0)

    refute_nil ready
  end

  def test_signal_multiple_times_does_not_raise
    3.times { @pipe.signal }
  end

  def test_signal_after_close_does_not_raise
    @pipe.close
    @pipe.signal
  end

  def test_drain_does_not_raise_when_empty
    @pipe.drain
  end

  def test_drain_clears_pipe_after_signals
    3.times { @pipe.signal }
    @pipe.drain
    ready = IO.select([@pipe.reader], nil, nil, 0)

    assert_nil ready
  end

  def test_drain_after_close_does_not_raise
    @pipe.close
    @pipe.drain
  end

  def test_close_closes_reader_io
    reader = @pipe.reader
    @pipe.close

    assert_same true, reader.closed?
  end

  def test_close_can_be_called_multiple_times
    3.times { @pipe.close }
  end

  def test_reader_can_be_used_with_io_select
    assert_respond_to @pipe.reader, :read_nonblock
  end
end

class WaterDropPollingQueuePipeEnableQueueEventsErrorTest < WaterDropTest::Base
  def test_propagates_error_from_enable_queue_io_events
    client = Minitest::Mock.new
    client.expect(:enable_queue_io_events, nil) { raise StandardError, "test error" }

    assert_raises(StandardError) do
      WaterDrop::Polling::QueuePipe.new(client)
    end
  end
end
