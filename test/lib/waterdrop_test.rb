# frozen_string_literal: true

require "test_helper"

describe_current do
  before do
    @waterdrop = described_class
  end

  # Reset the memoized instance after each test to avoid interference
  after do
    @waterdrop.instance_variable_set(:@instrumentation, nil)
  end

  describe "#gem_root" do
    context "when we want to get gem root path" do
      before do
        @path = Dir.pwd
      end

      it { assert_equal(@path, @waterdrop.gem_root.to_path) }
    end
  end

  describe "#monitor" do
    it "returns a class monitor instance" do
      assert_kind_of(WaterDrop::Instrumentation::ClassMonitor, @waterdrop.monitor)
    end

    it "memoizes the monitor instance" do
      first_call = @waterdrop.monitor
      second_call = @waterdrop.monitor

      assert_same(first_call, second_call)
    end

    it "allows subscribing to class-level events" do
      events_received = []

      @waterdrop.monitor.subscribe("producer.created") do |event|
        events_received << event
      end

      @waterdrop.monitor.instrument("producer.created", data: "test_data")

      assert_equal(1, events_received.size)
      assert_equal("test_data", events_received.first[:data])
    end

    it "raises error when subscribing to non-class events" do
      assert_raises(Karafka::Core::Monitoring::Notifications::EventNotRegistered) do
        @waterdrop.monitor.subscribe("message.produced_async") do |event|
          # This should not be allowed
        end
      end
    end
  end

  describe "#instrumentation" do
    it "returns the same instance as #monitor (alias)" do
      assert_kind_of(WaterDrop::Instrumentation::ClassMonitor, @waterdrop.instrumentation)
      assert_same(@waterdrop.instrumentation, @waterdrop.monitor)
    end

    it "maintains backward compatibility - existing code should work unchanged" do
      # Test that existing code using WaterDrop.instrumentation continues to work
      events_received = []

      # This is how existing code would use WaterDrop.instrumentation
      @waterdrop.instrumentation.subscribe("producer.created") do |event|
        events_received << event
      end

      @waterdrop.instrumentation.instrument("producer.created", data: "legacy_test")

      assert_equal(1, events_received.size)
      assert_equal("legacy_test", events_received.first[:data])
    end

    it "shares the same memoized instance with monitor" do
      # Call instrumentation first
      instrumentation_instance = @waterdrop.instrumentation
      monitor_instance = @waterdrop.monitor

      # Should be the exact same object
      assert_equal(instrumentation_instance.object_id, monitor_instance.object_id)
    end
  end
end
