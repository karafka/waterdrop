# frozen_string_literal: true

RSpec.describe WaterDrop::Instrumentation::Monitor do
  subject(:monitor) { described_class.new }

  describe '#instrument' do
    let(:result) { rand }
    let(:event_name) { monitor.available_events[0] }
    let(:instrumentation) do
      monitor.instrument(
        event_name,
        call: self,
        attempts_count: 1,
        error: Kafka::Error,
        options: {}
      ) { result }
    end

    it 'expect to return blocks execution value' do
      expect(instrumentation).to eq result
    end
  end

  describe '#subscribe' do
    context 'when we have a block based listener' do
      let(:subscription) { WaterDrop.monitor.subscribe(event_name) {} }

      context 'when we try to subscribe to an unsupported event' do
        let(:event_name) { 'unsupported' }

        it { expect { subscription }.to raise_error WaterDrop::Errors::UnregisteredMonitorEvent }
      end

      context 'when we try to subscribe to a supported event' do
        let(:event_name) { monitor.available_events.sample }

        it { expect { subscription }.not_to raise_error }
      end
    end

    context 'when we have an object listener' do
      let(:subscription) { WaterDrop.monitor.subscribe(listener) }
      let(:listener) { Class.new }

      it { expect { subscription }.not_to raise_error }
    end
  end

  describe '#available_events' do
    it 'expect to include registered events' do
      expect(monitor.available_events.size).to eq 4
    end

    it { expect(monitor.available_events).to include 'sync_producer.call.error' }
  end
end
