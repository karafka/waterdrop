# frozen_string_literal: true

class WaterDropPollingLatchTest < WaterDropTest::Base
  def setup
    @latch = WaterDrop::Polling::Latch.new
  end

  def test_not_released_by_default
    assert_same false, @latch.released?
  end

  def test_release_marks_as_released
    @latch.release!

    assert_same true, @latch.released?
  end

  def test_release_can_be_called_multiple_times
    3.times { @latch.release! }
  end

  def test_wait_returns_immediately_if_released
    @latch.release!
    @latch.wait
  end

  def test_wait_until_release_from_another_thread
    released = false

    thread = Thread.new do
      sleep(0.05)
      @latch.release!
      released = true
    end

    @latch.wait
    thread.join

    assert_same true, released
  end

  def test_released_returns_false_when_not_released
    assert_same false, @latch.released?
  end

  def test_released_returns_true_when_released
    @latch.release!

    assert_same true, @latch.released?
  end
end
