# frozen_string_literal: true

require "test_helper"

describe_current do
  before do
    @latch = described_class.new
  end

  describe "#initialize" do
    it "is not released by default" do
      assert_equal(false, @latch.released?)
    end
  end

  describe "#release!" do
    it "marks the latch as released" do
      @latch.release!
      assert_equal(true, @latch.released?)
    end

    it "can be called multiple times safely" do
      3.times { @latch.release! }
    end
  end

  describe "#wait" do
    it "returns immediately if already released" do
      @latch.release!
      @latch.wait
    end

    it "waits until release is called from another thread" do
      released = false

      Thread.new do
        sleep(0.05)
        @latch.release!
        released = true
      end

      @latch.wait
      assert_equal(true, released)
    end
  end

  describe "#released?" do
    it "returns false when not released" do
      assert_equal(false, @latch.released?)
    end

    it "returns true when released" do
      @latch.release!
      assert_equal(true, @latch.released?)
    end
  end
end
