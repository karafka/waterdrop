# frozen_string_literal: true

require "test_helper"

describe_current do
  before do
    @notifications = described_class.new
  end

  describe "::EVENTS" do
    it "contains only class-level events" do
      expected_events = %w[
        producer.created
        producer.configured
        connection_pool.created
        connection_pool.setup
        connection_pool.shutdown
        connection_pool.reload
        connection_pool.reloaded
      ]

      assert_equal(expected_events, described_class::EVENTS)
    end

    it "does not contain instance-level events" do
      instance_events = %w[
        producer.connected
        producer.closed
        message.produced_async
        message.produced_sync
        buffer.flushed_async
        error.occurred
      ]

      instance_events.each do |event|
        refute_includes(described_class::EVENTS, event)
      end
    end
  end

  describe "#initialize" do
    it "creates notifications instance without errors" do
      assert_kind_of(described_class, @notifications)
    end

    it "calls register_event for all class events" do
      # We can't directly test if events are registered, but we can test that
      # subscribing to class events works (which means they were registered)
      described_class::EVENTS.each do |event_name|
        @notifications.subscribe(event_name) { |_event| nil }
      end
    end
  end

  describe "#subscribe" do
    it "allows subscribing to registered class events" do
      @notifications.subscribe("producer.created") { |_event| nil }
    end

    it "raises error when subscribing to unregistered events" do
      assert_raises(Karafka::Core::Monitoring::Notifications::EventNotRegistered) do
        @notifications.subscribe("message.produced_sync") { |_event| nil }
      end
    end
  end

  describe "inheritance" do
    it "inherits from Karafka::Core::Monitoring::Notifications" do
      assert_operator(described_class, :<, Karafka::Core::Monitoring::Notifications)
    end
  end

  describe "separation from instance notifications" do
    before do
      @instance_notifications = WaterDrop::Instrumentation::Notifications.new
    end

    it "has different event sets" do
      refute_equal(WaterDrop::Instrumentation::Notifications::EVENTS, described_class::EVENTS)
    end

    it "class events are not available in instance notifications" do
      described_class::EVENTS.each do |event_name|
        assert_raises(Karafka::Core::Monitoring::Notifications::EventNotRegistered) do
          @instance_notifications.subscribe(event_name) { |_event| nil }
        end
      end
    end

    it "instance events are not available in class notifications" do
      sample_instance_events = %w[message.produced_async producer.connected error.occurred]

      sample_instance_events.each do |event_name|
        assert_raises(Karafka::Core::Monitoring::Notifications::EventNotRegistered) do
          @notifications.subscribe(event_name) { |_event| nil }
        end
      end
    end
  end
end
