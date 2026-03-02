# frozen_string_literal: true

require "test_helper"

describe_current do
  before do
    @middleware = described_class.new
    @message = build(:valid_message)
  end

  context "when no middlewares" do
    it do
      message_before = @message.dup
      @middleware.run(@message)
      assert_equal(message_before, @message)
    end
  end

  context "when morphing middleware" do
    before do
      mid1 = lambda do |msg|
        msg[:test] = 1
        msg
      end

      @middleware.prepend mid1
    end

    it do
      assert_nil(@message[:test])
      @middleware.run(@message)
      assert_equal(1, @message[:test])
    end
  end

  context "when morphing middlewares" do
    before do
      mid1 = lambda do |msg|
        msg[:test] = 1
        msg
      end

      mid2 = lambda do |msg|
        msg[:test2] = 2
        msg
      end

      @middleware.prepend mid1
      @middleware.prepend mid2
    end

    it do
      assert_nil(@message[:test])
      @middleware.run(@message)
      assert_equal(1, @message[:test])
    end

    it do
      assert_nil(@message[:test2])
      @middleware.run(@message)
      assert_equal(2, @message[:test2])
    end
  end

  context "when non-morphing middleware" do
    before do
      mid1 = lambda do |msg|
        msg = msg.dup
        msg[:test] = 1
        msg
      end

      @middleware.prepend mid1
    end

    it do
      message_before = @message.dup
      @middleware.run(@message)
      assert_equal(message_before, @message)
    end

    it { assert_equal(1, @middleware.run(@message)[:test]) }
  end

  context "when non-morphing middlewares" do
    before do
      mid1 = lambda do |msg|
        msg = msg.dup

        msg[:test] = 1
        msg
      end

      mid2 = lambda do |msg|
        msg = msg.dup

        msg[:test2] = 2
        msg
      end

      @middleware.prepend mid1
      @middleware.prepend mid2
    end

    it do
      message_before = @message.dup
      @middleware.run(@message)
      assert_equal(message_before, @message)
    end

    it { assert_equal(1, @middleware.run(@message)[:test]) }
    it { assert_equal(2, @middleware.run(@message)[:test2]) }
  end

  context "when morphing middleware on many" do
    before do
      mid1 = lambda do |msg|
        msg[:test] = 1
        msg
      end

      @middleware.append mid1
    end

    it do
      assert_nil(@message[:test])
      @middleware.run_many([@message])
      assert_equal(1, @message[:test])
    end
  end

  context "when morphing middlewares on many" do
    before do
      mid1 = lambda do |msg|
        msg[:test] = 1
        msg
      end

      mid2 = lambda do |msg|
        msg[:test2] = 2
        msg
      end

      @middleware.append mid1
      @middleware.append mid2
    end

    it do
      assert_nil(@message[:test])
      @middleware.run_many([@message])
      assert_equal(1, @message[:test])
    end

    it do
      assert_nil(@message[:test2])
      @middleware.run_many([@message])
      assert_equal(2, @message[:test2])
    end
  end

  context "when non-morphing middleware on many" do
    before do
      mid1 = lambda do |msg|
        msg = msg.dup
        msg[:test] = 1
        msg
      end

      @middleware.append mid1
    end

    it do
      message_before = @message.dup
      @middleware.run_many([@message])
      assert_equal(message_before, @message)
    end

    it { assert_equal(1, @middleware.run_many([@message]).first[:test]) }
  end
end
