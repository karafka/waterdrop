# frozen_string_literal: true

RSpec.describe_current do
  subject(:waterdrop) { described_class }

  # Reset the memoized instance after each test to avoid interference
  after do
    waterdrop.instance_variable_set(:@instrumentation, nil)
  end

  describe '#gem_root' do
    context 'when we want to get gem root path' do
      let(:path) { Dir.pwd }

      it { expect(waterdrop.gem_root.to_path).to eq path }
    end
  end

  describe '#instrumentation' do
    it 'returns a class monitor instance' do
      expect(waterdrop.instrumentation).to be_a(WaterDrop::Instrumentation::ClassMonitor)
    end

    it 'memoizes the monitor instance' do
      first_call = waterdrop.instrumentation
      second_call = waterdrop.instrumentation

      expect(first_call).to be(second_call)
    end

    it 'allows subscribing to class-level events' do
      events_received = []

      waterdrop.instrumentation.subscribe('producer.created') do |event|
        events_received << event
      end

      waterdrop.instrumentation.instrument('producer.created', data: 'test_data')

      expect(events_received.size).to eq(1)
      expect(events_received.first[:data]).to eq('test_data')
    end

    it 'raises error when subscribing to non-class events' do
      expect do
        waterdrop.instrumentation.subscribe('message.produced_async') do |event|
          # This should not be allowed
        end
      end.to raise_error(Karafka::Core::Monitoring::Notifications::EventNotRegistered)
    end
  end
end
