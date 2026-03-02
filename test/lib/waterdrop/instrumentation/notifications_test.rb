# frozen_string_literal: true

require "test_helper"

describe_current do
  before do
    @monitor = described_class.new
  end

  describe "#instrument" do
    before do
      @result = rand
      @event_name = "message.produced_async"
      @instrumentation = @monitor.instrument(
        @event_name,
        call: self,
        error: StandardError
      ) { @result }
    end

    it "expect to return blocks execution value" do
      assert_equal(@result, @instrumentation)
    end
  end

  describe "#subscribe" do
    describe "when we have a block based listener" do
      describe "when we try to subscribe to an unsupported event" do
        it do
          expected_error = Karafka::Core::Monitoring::Notifications::EventNotRegistered
          assert_raises(expected_error) { @monitor.subscribe("unsupported") { nil } }
        end
      end

      describe "when we try to subscribe to a supported event" do
        it { @monitor.subscribe("message.produced_async") { nil } }
      end
    end

    describe "when we have an object listener" do
      before do
        @listener = Class.new do
          def on_message_produced_async(_event)
            true
          end
        end
      end

      it { @monitor.subscribe(@listener.new) }
    end
  end

  describe "producer lifecycle events flow" do
    before do
      @producer = WaterDrop::Producer.new
      @events = []
      @events_names = %w[
        producer.connected
        producer.closing
        producer.closed
      ]
      @status = @producer.status
    end

    after { @producer.close }

    describe "when producer is initialized" do
      it { assert_equal("initial", @status.to_s) }
      it { assert_empty(@events) }
    end

    describe "when producer is configured" do
      before do
        @producer.setup { nil }

        @events_names.each do |event_name|
          @producer.monitor.subscribe(event_name) do |event|
            @events << event
          end
        end
      end

      it { assert_equal("configured", @status.to_s) }
      it { assert_empty(@events) }
    end

    describe "when producer is connected" do
      before do
        @producer.setup { nil }

        @events_names.each do |event_name|
          @producer.monitor.subscribe(event_name) do |event|
            @events << event
          end
        end

        @producer.client
      end

      it { assert_equal("connected", @status.to_s) }
      it { assert_equal(1, @events.size) }
      it { assert_equal("producer.connected", @events.last.id) }
      it { assert(@events.last.payload.key?(:producer_id)) }
    end

    describe "when producer is closed" do
      before do
        @producer.setup { nil }

        @events_names.each do |event_name|
          @producer.monitor.subscribe(event_name) do |event|
            @events << event
          end
        end

        @producer.client
        @producer.close
      end

      it { assert_equal("closed", @status.to_s) }
      it { assert_equal(3, @events.size) }
      it { assert_equal("producer.connected", @events.first.id) }
      it { assert(@events.first.payload.key?(:producer_id)) }
      it { assert_equal("producer.closing", @events[1].id) }
      it { assert(@events[1].payload.key?(:producer_id)) }
      it { assert_equal("producer.closed", @events.last.id) }
      it { assert(@events.last.payload.key?(:producer_id)) }
      it { assert(@events.last.payload.key?(:time)) }
    end
  end
end
