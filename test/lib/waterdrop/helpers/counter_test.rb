# frozen_string_literal: true

require "test_helper"

describe_current do
  before do
    @counter = described_class.new
  end

  describe "#initialize" do
    it "starts with a value of 0" do
      assert_equal(0, @counter.value)
    end
  end

  describe "#increment" do
    it "increases the value by 1" do
      @counter.increment
      assert_equal(1, @counter.value)
    end

    it "correctly increments the value concurrently" do
      threads = Array.new(10) do
        Thread.new { @counter.increment }
      end
      threads.each(&:join)
      assert_equal(10, @counter.value)
    end
  end

  describe "#decrement" do
    it "decreases the value by 1" do
      @counter.decrement
      assert_equal(-1, @counter.value)
    end

    it "correctly decrements the value concurrently" do
      threads = Array.new(10) do
        Thread.new { @counter.decrement }
      end
      threads.each(&:join)
      assert_equal(-10, @counter.value)
    end
  end

  describe "concurrent increment and decrement" do
    it "correctly updates the value with concurrent increments and decrements" do
      increment_threads = Array.new(5) { Thread.new { @counter.increment } }
      decrement_threads = Array.new(5) { Thread.new { @counter.decrement } }
      (increment_threads + decrement_threads).each(&:join)
      assert_equal(0, @counter.value)
    end
  end
end
