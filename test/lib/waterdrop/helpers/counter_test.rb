# frozen_string_literal: true

class WaterDropHelpersCounterTest < WaterDropTest::Base
  def setup
    @counter = WaterDrop::Helpers::Counter.new
  end

  def test_initialize_starts_at_zero
    assert_equal 0, @counter.value
  end

  def test_increment_increases_value_by_one
    @counter.increment

    assert_equal 1, @counter.value
  end

  def test_increment_concurrent
    threads = Array.new(10) do
      Thread.new { @counter.increment }
    end
    threads.each(&:join)

    assert_equal 10, @counter.value
  end

  def test_decrement_decreases_value_by_one
    @counter.decrement

    assert_equal(-1, @counter.value)
  end

  def test_decrement_concurrent
    threads = Array.new(10) do
      Thread.new { @counter.decrement }
    end
    threads.each(&:join)

    assert_equal(-10, @counter.value)
  end

  def test_concurrent_increment_and_decrement
    increment_threads = Array.new(5) { Thread.new { @counter.increment } }
    decrement_threads = Array.new(5) { Thread.new { @counter.decrement } }
    (increment_threads + decrement_threads).each(&:join)

    assert_equal 0, @counter.value
  end
end
