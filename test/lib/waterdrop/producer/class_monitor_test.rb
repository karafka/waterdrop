# frozen_string_literal: true

require "test_helper"

describe_current do
  before do
    @producer_class = Class.new do
      include WaterDrop::Producer::ClassMonitor
    end

    @producer = @producer_class.new
  end

  # Reset instrumentation after each test to avoid interference
  after { WaterDrop.instance_variable_set(:@instrumentation, nil) }

  describe "#class_monitor" do
    it "returns the global WaterDrop instrumentation monitor" do
      assert_same(WaterDrop.instrumentation, @producer.send(:class_monitor))
    end

    it "returns a ClassMonitor instance" do
      assert_kind_of(WaterDrop::Instrumentation::ClassMonitor, @producer.send(:class_monitor))
    end

    it "is a private method" do
      assert_equal(true, @producer_class.private_method_defined?(:class_monitor))
    end

    it "allows instrumentation through the returned monitor" do
      events_received = []

      WaterDrop.instrumentation.subscribe("producer.created") do |event|
        events_received << event
      end

      @producer.send(:class_monitor).instrument("producer.created", test_data: "value")

      assert_equal(1, events_received.size)
      assert_equal("value", events_received.first[:test_data])
    end
  end

  describe "integration with Producer" do
    it "is included in the Producer class" do
      assert_includes(WaterDrop::Producer.included_modules, described_class)
    end

    it "allows Producer instances to use class_monitor" do
      producer = WaterDrop::Producer.new

      assert_kind_of(WaterDrop::Instrumentation::ClassMonitor, producer.send(:class_monitor))
    ensure
      producer&.close
    end
  end
end
