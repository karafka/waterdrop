# frozen_string_literal: true

describe_current do
  before do
    @producer_class = Class.new do
      include WaterDrop::Producer::ClassMonitor

      def monitor
        class_monitor
      end
    end

    @producer = @producer_class.new
  end

  # Reset instrumentation after each test to avoid interference
  after { WaterDrop.instance_variable_set(:@instrumentation, nil) }

  describe "#class_monitor" do
    it "returns the global WaterDrop instrumentation monitor" do
      assert_same(WaterDrop.instrumentation, @producer.monitor)
    end

    it "returns a ClassMonitor instance" do
      assert_kind_of(WaterDrop::Instrumentation::ClassMonitor, @producer.monitor)
    end

    it "is a private method" do
      assert(@producer_class.private_method_defined?(:class_monitor))
    end

    it "allows instrumentation through the returned monitor" do
      events_received = []

      WaterDrop.instrumentation.subscribe("producer.created") do |event|
        events_received << event
      end

      @producer.monitor.instrument("producer.created", test_data: "value")

      assert_equal(1, events_received.size)
      assert_equal("value", events_received.first[:test_data])
    end
  end

  describe "integration with Producer" do
    it "is included in the Producer class" do
      assert_includes(WaterDrop::Producer.included_modules, described_class)
    end

    it "instruments Producer creation through the global monitor" do
      created = []
      WaterDrop.instrumentation.subscribe("producer.created") { |event| created << event }

      producer = WaterDrop::Producer.new

      assert_equal([producer], created.map { |event| event[:producer] })
    ensure
      producer&.close
    end
  end
end
