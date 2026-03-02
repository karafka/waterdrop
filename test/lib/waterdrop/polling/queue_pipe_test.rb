# frozen_string_literal: true

require "test_helper"

describe_current do
  before do
    @client = OpenStruct.new
    @client.define_singleton_method(:enable_queue_io_events) { |*| nil }

    @pipe = described_class.new(@client)
  end

  after do
    @pipe.close
  rescue
    # Ignore if pipe was never created
  end

  describe "#initialize" do
    it "creates a readable IO" do
      assert_kind_of(IO, @pipe.reader)
    end

    it "calls enable_queue_io_events on the client" do
      called_with = nil
      client = OpenStruct.new
      client.define_singleton_method(:enable_queue_io_events) { |fd| called_with = fd }

      pipe = described_class.new(client)

      refute_nil(called_with)
      assert_kind_of(Integer, called_with)
      pipe.close
    end

    describe "when enable_queue_io_events raises" do
      it "propagates the error" do
        failing_client = OpenStruct.new
        failing_client.define_singleton_method(:enable_queue_io_events) { |*| raise StandardError, "test error" }

        error = assert_raises(StandardError) { described_class.new(failing_client) }
        assert_equal("test error", error.message)
      end
    end
  end

  describe "#signal" do
    it "makes the pipe readable" do
      @pipe.signal
      ready = IO.select([@pipe.reader], nil, nil, 0)

      refute_nil(ready)
    end

    it "does not raise when called multiple times" do
      3.times { @pipe.signal }
    end

    it "does not raise after pipe is closed" do
      @pipe.close
      @pipe.signal
    end
  end

  describe "#drain" do
    it "does not raise when pipe is empty" do
      @pipe.drain
    end

    it "clears the pipe after signals" do
      3.times { @pipe.signal }
      @pipe.drain
      ready = IO.select([@pipe.reader], nil, nil, 0)

      assert_nil(ready)
    end

    it "does not raise after pipe is closed" do
      @pipe.close
      @pipe.drain
    end
  end

  describe "#close" do
    it "closes the reader IO" do
      reader = @pipe.reader
      @pipe.close

      assert_predicate(reader, :closed?)
    end

    it "can be called multiple times safely" do
      3.times { @pipe.close }
    end
  end

  describe "#reader" do
    it "can be used with IO.select" do
      assert_respond_to(@pipe.reader, :read_nonblock)
    end
  end
end
