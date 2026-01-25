# frozen_string_literal: true

RSpec.describe_current do
  subject(:notifications) { described_class.new }

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

      expect(described_class::EVENTS).to eq(expected_events)
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
        expect(described_class::EVENTS).not_to include(event)
      end
    end
  end

  describe "#initialize" do
    it "creates notifications instance without errors" do
      expect(notifications).to be_a(described_class)
    end

    it "calls register_event for all class events" do
      # We can't directly test if events are registered, but we can test that
      # subscribing to class events works (which means they were registered)
      described_class::EVENTS.each do |event_name|
        expect do
          notifications.subscribe(event_name) { |_event| nil }
        end.not_to raise_error
      end
    end
  end

  describe "#subscribe" do
    it "allows subscribing to registered class events" do
      expect do
        notifications.subscribe("producer.created") { |_event| nil }
      end.not_to raise_error
    end

    it "raises error when subscribing to unregistered events" do
      expect do
        notifications.subscribe("message.produced_sync") { |_event| nil }
      end.to raise_error(Karafka::Core::Monitoring::Notifications::EventNotRegistered)
    end
  end

  describe "inheritance" do
    it "inherits from Karafka::Core::Monitoring::Notifications" do
      expect(described_class).to be < Karafka::Core::Monitoring::Notifications
    end
  end

  describe "separation from instance notifications" do
    let(:instance_notifications) { WaterDrop::Instrumentation::Notifications.new }

    it "has different event sets" do
      expect(described_class::EVENTS).not_to eq(WaterDrop::Instrumentation::Notifications::EVENTS)
    end

    it "class events are not available in instance notifications" do
      described_class::EVENTS.each do |event_name|
        expect do
          instance_notifications.subscribe(event_name) { |_event| nil }
        end.to raise_error(Karafka::Core::Monitoring::Notifications::EventNotRegistered)
      end
    end

    it "instance events are not available in class notifications" do
      sample_instance_events = %w[message.produced_async producer.connected error.occurred]

      sample_instance_events.each do |event_name|
        expect do
          notifications.subscribe(event_name) { |_event| nil }
        end.to raise_error(Karafka::Core::Monitoring::Notifications::EventNotRegistered)
      end
    end
  end
end
