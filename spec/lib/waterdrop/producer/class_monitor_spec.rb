# frozen_string_literal: true

RSpec.describe_current do
  let(:producer_class) do
    Class.new do
      include WaterDrop::Producer::ClassMonitor
    end
  end

  let(:producer) { producer_class.new }

  # Reset instrumentation after each test to avoid interference
  after { WaterDrop.instance_variable_set(:@instrumentation, nil) }

  describe "#class_monitor" do
    it "returns the global WaterDrop instrumentation monitor" do
      expect(producer.send(:class_monitor)).to be(WaterDrop.instrumentation)
    end

    it "returns a ClassMonitor instance" do
      expect(producer.send(:class_monitor)).to be_a(WaterDrop::Instrumentation::ClassMonitor)
    end

    it "is a private method" do
      expect(producer_class.private_method_defined?(:class_monitor)).to be(true)
    end

    it "allows instrumentation through the returned monitor" do
      events_received = []

      WaterDrop.instrumentation.subscribe("producer.created") do |event|
        events_received << event
      end

      producer.send(:class_monitor).instrument("producer.created", test_data: "value")

      expect(events_received.size).to eq(1)
      expect(events_received.first[:test_data]).to eq("value")
    end
  end

  describe "integration with Producer" do
    it "is included in the Producer class" do
      expect(WaterDrop::Producer.included_modules).to include(described_class)
    end

    it "allows Producer instances to use class_monitor" do
      producer = WaterDrop::Producer.new

      expect(producer.send(:class_monitor)).to be_a(WaterDrop::Instrumentation::ClassMonitor)
    end
  end
end
