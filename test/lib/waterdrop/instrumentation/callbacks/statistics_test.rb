# frozen_string_literal: true

require "test_helper"

describe_current do
  before do
    @producer_id = SecureRandom.uuid
    @client_name = SecureRandom.uuid
    @monitor = WaterDrop::Instrumentation::Monitor.new
    @callback = described_class.new(@producer_id, @client_name, @monitor)
  end

  describe "#call" do
    describe "when emitted statistics refer different producer" do
      it "expect not to emit them" do
        changed = []
        @monitor.subscribe("statistics.emitted") { |event| changed << event[:statistics] }
        @callback.call({})

        assert_empty(changed)
      end
    end

    describe "when emitted statistics handler code contains an error" do
      it "expect to contain in, notify and continue as we do not want to crash rdkafka" do
        tracked_errors = []

        @monitor.subscribe("statistics.emitted") { raise }
        @monitor.subscribe("error.occurred") { |event| tracked_errors << event }

        @callback.call({ "name" => @client_name })

        assert_equal(1, tracked_errors.size)
        assert_equal("callbacks.statistics.error", tracked_errors.first[:type])
      end
    end

    describe "when emitted statistics refer to expected producer" do
      before do
        @monitor = WaterDrop::Instrumentation::Monitor.new
        @callback = described_class.new(@producer_id, @client_name, @monitor)
        @changed = []
        @statistics = { "name" => @client_name }

        @monitor.subscribe("statistics.emitted") do |event|
          @changed << event[:statistics]
        end

        @callback.call(@statistics)
      end

      it "expects to emit them" do
        assert_equal([@statistics], @changed)
      end
    end

    describe "when we emit more statistics" do
      before do
        @monitor = WaterDrop::Instrumentation::Monitor.new
        @callback = described_class.new(@producer_id, @client_name, @monitor)
        @changed = []

        @monitor.subscribe("statistics.emitted") do |event|
          @changed << event[:statistics]
        end

        5.times do |count|
          @callback.call("msg_count" => count, "name" => @client_name)
        end
      end

      it { assert_equal(5, @changed.size) }

      it "expect to decorate them" do
        # First is also decorated but with no change
        assert_equal(0, @changed.first["msg_count_d"])
        assert_equal(1, @changed.last["msg_count_d"])
      end
    end
  end

  describe "emitted event data format" do
    before do
      @events = []
      @statistics = { "name" => @client_name, "val" => 1, "str" => 1 }

      @monitor.subscribe("statistics.emitted") do |event|
        @events << event
      end

      @callback.call(@statistics)
      @event = @events.first
    end

    it { assert_equal("statistics.emitted", @event.id) }
    it { assert_equal(@producer_id, @event[:producer_id]) }
    it { assert_equal(@statistics, @event[:statistics]) }
    it { assert_equal(0, @event[:statistics]["val_d"]) }
  end

  describe "late subscription support" do
    before do
      @events = []
      @statistics = { "name" => @client_name, "msg_count" => 100 }
    end

    describe "when no one is listening initially" do
      it "expect not to emit statistics" do
        @callback.call(@statistics)

        assert_empty(@events)
      end
    end

    describe "when subscriber is added after initialization" do
      it "expect to emit statistics for subsequent calls" do
        # First call with no listeners
        @callback.call(@statistics)

        assert_empty(@events)

        # Subscribe late
        @monitor.subscribe("statistics.emitted") do |event|
          @events << event
        end

        # Second call should now emit
        @callback.call(@statistics)

        refute_empty(@events)
        assert_equal(1, @events.size)
      end
    end
  end
end
