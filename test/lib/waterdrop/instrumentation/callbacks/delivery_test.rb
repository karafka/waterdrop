# frozen_string_literal: true

describe_current do
  before do
    @delivery_report_stub = Struct.new(:offset, :partition, :topic_name, :error, :label, keyword_init: true)
    @producer = build(:producer)
    @producer_id = SecureRandom.uuid
    @transactional = @producer.transactional?
    @monitor = WaterDrop::Instrumentation::Monitor.new
    @callback = described_class.new(@producer_id, @transactional, @monitor)
    @delivery_report = @delivery_report_stub.new(
      offset: rand(100),
      partition: rand(100),
      topic_name: rand(100).to_s,
      error: 0,
      label: nil
    )
  end

  after { @producer.close }

  describe "#call" do
    before do
      @changed = []

      @monitor.subscribe("message.acknowledged") do |event|
        @changed << event
      end

      @callback.call(@delivery_report)
      @event = @changed.first
    end

    it { assert_equal("message.acknowledged", @event.id) }
    it { assert_equal(@producer_id, @event[:producer_id]) }
    it { assert_equal(@delivery_report.offset, @event[:offset]) }
    it { assert_equal(@delivery_report.partition, @event[:partition]) }
    it { assert_equal(@delivery_report.topic_name, @event[:topic]) }

    describe "when delivery handler code contains an error" do
      before do
        @tracked_errors = []

        @monitor.subscribe("message.acknowledged") do
          raise
        end

        local_errors = @tracked_errors

        @monitor.subscribe("error.occurred") do |event|
          local_errors << event
        end
      end

      it "expect to contain in, notify and continue as we do not want to crash rdkafka" do
        @callback.call(@delivery_report)

        assert_equal(1, @tracked_errors.size)
        assert_equal("callbacks.delivery.error", @tracked_errors.first[:type])
      end
    end
  end

  describe "#when we do an end-to-end delivery report check" do
    describe "when there is a message that was successfully delivered" do
      before do
        @changed = []
        @message = build(:valid_message)

        @producer.monitor.subscribe("message.acknowledged") do |event|
          @changed << event
        end

        @producer.produce_sync(@message)

        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
        sleep(0.01) until @changed.size.positive? || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
        @event = @changed.first
      end

      it { assert_equal(0, @event.payload[:partition]) }
      it { assert_equal(0, @event.payload[:offset]) }
      it { assert_equal(@message[:topic], @event[:topic]) }
    end

    describe "when there is a message that was not successfully delivered async" do
      before do
        @changed = []

        @producer.monitor.subscribe("error.occurred") do |event|
          @changed << event
        end

        100.times do
          # We force it to bypass the validations, so we trigger an error on delivery
          # otherwise we would be stopped by WaterDrop itself
          @producer.send(:client).produce(topic: "$%^&*", payload: "1")
        rescue Rdkafka::RdkafkaError
          nil
        end

        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
        sleep(0.01) until @changed.size.positive? || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
        @event = @changed.last
      end

      it { assert_kind_of(Rdkafka::RdkafkaError, @event.payload[:error]) }
      it { assert_equal(-1, @event.payload[:partition]) }
      it { assert_equal(-1001, @event.payload[:offset]) }
      it { assert_equal("$%^&*", @event.payload[:topic]) }
    end

    describe "when there is a message that was not successfully delivered sync" do
      before do
        @changed = []

        @producer.monitor.subscribe("error.occurred") do |event|
          @changed << event
        end

        # Intercept the error so it won't bubble up as we want to check the notifications pipeline
        begin
          @producer.send(:client).produce(topic: "$%^&*", payload: "1").wait
        rescue Rdkafka::RdkafkaError
          nil
        end

        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
        sleep(0.01) until @changed.size.positive? || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
        @event = @changed.first
      end

      it { assert_kind_of(Rdkafka::RdkafkaError, @event.payload[:error]) }
      it { assert_equal(-1, @event.payload[:partition]) }
      it { assert_equal(-1001, @event.payload[:offset]) }
    end

    describe "when there is an inline thrown erorrs" do
      before do
        @changed = []
        @errors = []
        @producer = build(:limited_producer)

        @producer.monitor.subscribe("error.occurred") do |event|
          @changed << event
        end

        # Intercept the error so it won't bubble up as we want to check the notifications pipeline
        begin
          msg = build(:valid_message)
          100.times { @producer.produce_async(msg) }
        rescue WaterDrop::Errors::ProduceError => e
          @errors << e
        end

        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
        sleep(0.01) until @changed.size.positive? || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
        @event = @changed.first
      end

      it { assert_kind_of(WaterDrop::Errors::ProduceError, @errors.first) }
      it { assert_kind_of(Rdkafka::RdkafkaError, @errors.first.cause) }
      it { assert_kind_of(WaterDrop::Errors::ProduceError, @event[:error]) }
      it { assert_kind_of(Rdkafka::RdkafkaError, @event[:error].cause) }
    end

    describe "when there is a producer with non-transactional purge" do
      before do
        @producer = build(:slow_producer)
        @errors = []
        @purges = []

        @producer.monitor.subscribe("error.occurred") do |event|
          @errors << event[:error]
        end

        @producer.monitor.subscribe("message.purged") do |event|
          @purges << event[:error]
        end

        @producer.produce_async(build(:valid_message))
        @producer.purge

        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
        sleep(0.01) until @errors.size.positive? || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      end

      it "expect to have it in the errors" do
        assert_kind_of(Rdkafka::RdkafkaError, @errors.first)
        assert_equal(:purge_queue, @errors.first.code)
      end

      it "expect not to publish purge notification" do
        assert_empty(@purges)
      end
    end
  end
end
