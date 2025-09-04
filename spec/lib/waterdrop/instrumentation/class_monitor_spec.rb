# frozen_string_literal: true

RSpec.describe_current do
  subject(:monitor) { described_class.new }

  describe '#initialize' do
    it 'creates a monitor instance without errors' do
      expect(monitor).to be_a(described_class)
    end

    it 'accepts custom notifications bus parameter' do
      custom_bus = WaterDrop::Instrumentation::ClassNotifications.new
      custom_monitor = described_class.new(custom_bus)

      expect(custom_monitor).to be_a(described_class)
    end

    it 'accepts namespace parameter without errors' do
      expect do
        described_class.new(nil, 'test')
      end.not_to raise_error
    end
  end

  describe '#subscribe' do
    it 'allows subscribing to class-level events' do
      events_received = []

      monitor.subscribe('producer.created') do |event|
        events_received << event
      end

      monitor.instrument('producer.created', test_data: 'value')

      expect(events_received.size).to eq(1)
      expect(events_received.first[:test_data]).to eq('value')
    end

    it 'allows subscribing to producer.configured events' do
      events_received = []

      monitor.subscribe('producer.configured') do |event|
        events_received << event
      end

      monitor.instrument('producer.configured', producer_id: 'test-id')

      expect(events_received.size).to eq(1)
      expect(events_received.first[:producer_id]).to eq('test-id')
    end

    it 'raises error when subscribing to non-class events' do
      expect do
        monitor.subscribe('message.produced_async') do |event|
          # This should not be allowed
        end
      end.to raise_error(Karafka::Core::Monitoring::Notifications::EventNotRegistered)
    end

    it 'raises error when subscribing to instance-level events' do
      instance_events = %w[
        producer.connected
        producer.closed
        message.produced_sync
        buffer.flushed_async
        error.occurred
      ]

      instance_events.each do |event_name|
        expect do
          monitor.subscribe(event_name) { |event| }
        end.to raise_error(Karafka::Core::Monitoring::Notifications::EventNotRegistered)
      end
    end
  end

  describe '#instrument' do
    it 'instruments class-level events successfully' do
      events_received = []

      monitor.subscribe('producer.created') do |event|
        events_received << event
      end

      result = monitor.instrument('producer.created', producer: 'test_producer') do
        'instrumented_result'
      end

      expect(result).to eq('instrumented_result')
      expect(events_received.size).to eq(1)
      expect(events_received.first[:producer]).to eq('test_producer')
    end

    it 'supports instrumenting without a block' do
      events_received = []

      monitor.subscribe('producer.configured') do |event|
        events_received << event
      end

      monitor.instrument('producer.configured', config: 'test_config')

      expect(events_received.size).to eq(1)
      expect(events_received.first[:config]).to eq('test_config')
    end

    it 'raises error when instrumenting non-class events' do
      expect do
        monitor.instrument('message.produced_sync', message: {})
      end.to raise_error(Karafka::Core::Monitoring::Notifications::EventNotRegistered)
    end
  end

  describe 'multiple subscribers' do
    it 'notifies all subscribers for the same event' do
      events_received1 = []
      events_received2 = []

      monitor.subscribe('producer.created') do |event|
        events_received1 << event
      end

      monitor.subscribe('producer.created') do |event|
        events_received2 << event
      end

      monitor.instrument('producer.created', producer_id: 'shared-test')

      expect(events_received1.size).to eq(1)
      expect(events_received2.size).to eq(1)
      expect(events_received1.first[:producer_id]).to eq('shared-test')
      expect(events_received2.first[:producer_id]).to eq('shared-test')
    end
  end

  describe 'inheritance' do
    it 'inherits from Karafka::Core::Monitoring::Monitor' do
      expect(described_class).to be < Karafka::Core::Monitoring::Monitor
    end
  end

  describe 'integration with WaterDrop.instrumentation' do
    let(:events_received) { [] }

    before do
      WaterDrop.instrumentation.subscribe('producer.created') do |event|
        events_received << [:producer_created, event]
      end

      WaterDrop.instrumentation.subscribe('producer.configured') do |event|
        events_received << [:producer_configured, event]
      end
    end

    after do
      # Reset the memoized instance after each test to avoid interference
      WaterDrop.instance_variable_set(:@instrumentation, nil)
    end

    context 'when creating a producer without configuration' do
      let!(:producer) { WaterDrop::Producer.new }

      it 'instruments producer.created event' do
        created_events = events_received.select { |event| event.first == :producer_created }

        expect(created_events.size).to eq(1)

        event_data = created_events.first.last
        expect(event_data[:producer]).to be(producer)
        expect(event_data[:producer_id]).to be_nil # Not configured yet
      end

      it 'does not instrument producer.configured event yet' do
        configured_events = events_received.select { |event| event.first == :producer_configured }
        expect(configured_events).to be_empty
      end
    end

    context 'when creating a producer with configuration' do
      let!(:producer) do
        WaterDrop::Producer.new do |config|
          config.deliver = false
          config.kafka = { 'bootstrap.servers': 'localhost:9092' }
        end
      end

      it 'instruments both producer.created and producer.configured events' do
        created_events = events_received.select { |event| event.first == :producer_created }
        configured_events = events_received.select { |event| event.first == :producer_configured }

        expect(created_events.size).to eq(1)
        expect(configured_events.size).to eq(1)

        # Check producer.configured event data
        config_event_data = configured_events.first.last
        expect(config_event_data[:producer]).to be(producer)
        expect(config_event_data[:producer_id]).to eq(producer.id)
        expect(config_event_data[:config]).to be(producer.config)
      end
    end

    context 'when creating multiple producers' do
      let!(:producer1) do
        WaterDrop::Producer.new do |config|
          config.deliver = false
          config.kafka = { 'bootstrap.servers': 'localhost:9092' }
        end
      end

      let!(:producer2) do
        WaterDrop::Producer.new do |config|
          config.deliver = false
          config.kafka = { 'bootstrap.servers': 'localhost:9093' }
        end
      end

      it 'instruments events for each producer separately' do
        created_events = events_received.select { |event| event.first == :producer_created }
        configured_events = events_received.select { |event| event.first == :producer_configured }

        expect(created_events.size).to eq(2)
        expect(configured_events.size).to eq(2)

        # Verify each producer is tracked separately
        created_producers = created_events.map { |event| event.last[:producer] }
        configured_producers = configured_events.map { |event| event.last[:producer] }

        expect(created_producers).to contain_exactly(producer1, producer2)
        expect(configured_producers).to contain_exactly(producer1, producer2)
      end
    end

    describe 'integration with external libraries' do
      it 'allows external libraries to hook into producer lifecycle' do
        middleware_applied = []

        # Simulate Datadog integration subscribing to events
        WaterDrop.instrumentation.subscribe('producer.configured') do |event|
          producer = event[:producer]

          # Simulate adding tracing middleware
          middleware_applied << producer.id

          # Simulate middleware addition
          producer.config.middleware.append(
            lambda do |message|
              message[:headers] ||= {}
              message[:headers]['x-trace-id'] = 'test-trace-id'
              message
            end
          )
        end

        # Create a producer
        producer = WaterDrop::Producer.new do |config|
          config.deliver = false
          config.kafka = { 'bootstrap.servers': 'localhost:9092' }
        end

        # Verify middleware was applied
        expect(middleware_applied).to include(producer.id)

        # Test that middleware actually works
        test_message = { topic: 'test', payload: 'test' }
        processed_message = producer.middleware.run(test_message)

        expect(processed_message[:headers]['x-trace-id']).to eq('test-trace-id')
      end
    end
  end
end
