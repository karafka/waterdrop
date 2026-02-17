# frozen_string_literal: true

RSpec.describe_current do
  subject(:latch) { described_class.new }

  describe "#initialize" do
    it "is not released by default" do
      expect(latch.released?).to be(false)
    end
  end

  describe "#release!" do
    it "marks the latch as released" do
      latch.release!
      expect(latch.released?).to be(true)
    end

    it "can be called multiple times safely" do
      expect do
        3.times { latch.release! }
      end.not_to raise_error
    end
  end

  describe "#wait" do
    it "returns immediately if already released" do
      latch.release!
      expect { latch.wait }.not_to raise_error
    end

    it "waits until release is called from another thread" do
      released = false

      Thread.new do
        sleep(0.05)
        latch.release!
        released = true
      end

      latch.wait
      expect(released).to be(true)
    end
  end

  describe "#released?" do
    it "returns false when not released" do
      expect(latch.released?).to be(false)
    end

    it "returns true when released" do
      latch.release!
      expect(latch.released?).to be(true)
    end
  end
end
