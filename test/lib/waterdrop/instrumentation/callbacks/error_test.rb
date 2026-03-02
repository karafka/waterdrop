# frozen_string_literal: true

require "test_helper"

describe_current do
  before do
    @producer_id = SecureRandom.uuid
    @client_name = SecureRandom.uuid
    @monitor = WaterDrop::Instrumentation::Monitor.new
    @callback = described_class.new(@producer_id, @client_name, @monitor)
    @error = Rdkafka::RdkafkaError.new(1, [])
  end

  describe "#call" do
    before do
      @changed = []

      @monitor.subscribe("error.occurred") do |event|
        @changed << event[:error]
      end

      @callback.call(@client_name, @error)
    end

    describe "when occurred error refer different producer" do
      before do
        @callback = described_class.new(@producer_id, "other", @monitor)
        @changed = []

        @monitor.subscribe("error.occurred") do |event|
          @changed << event[:error]
        end

        @callback.call(@client_name, @error)
      end

      it "expect not to emit them" do
        assert_empty(@changed)
      end
    end

    describe "when occurred error refer to expected producer" do
      it "expects to emit them" do
        assert_equal([@error], @changed)
      end
    end
  end

  describe "occurred event data format" do
    before do
      @changed = []

      @monitor.subscribe("error.occurred") do |stat|
        @changed << stat
      end

      @callback.call(@client_name, @error)
      @event = @changed.first
    end

    it { assert_equal("error.occurred", @event.id) }
    it { assert_equal(@producer_id, @event[:producer_id]) }
    it { assert_equal(@error, @event[:error]) }
    it { assert_equal("librdkafka.error", @event[:type]) }
  end

  describe "when librdkafka error handling handler contains error" do
    before do
      @tracked_errors = []

      @monitor.subscribe("error.occurred") do |event|
        next unless event[:type] == "librdkafka.error"

        raise
      end

      local_errors = @tracked_errors

      @monitor.subscribe("error.occurred") do |event|
        local_errors << event
      end
    end

    it "expect to contain in, notify and continue as we do not want to crash rdkafka" do
      @callback.call(@client_name, @error)

      assert_equal(1, @tracked_errors.size)
      assert_equal("callbacks.error.error", @tracked_errors.first[:type])
    end
  end
end
