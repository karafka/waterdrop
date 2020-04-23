# frozen_string_literal: true

RSpec.describe WaterDrop::Instrumentation::Monitor do
  subject(:monitor) { described_class.new }

  describe '#instrument' do
    let(:result) { rand }
    let(:event_name) { 'message.produced_async' }
    let(:instrumentation) do
      monitor.instrument(
        event_name,
        call: self,
        error: StandardError
      ) { result }
    end

    it 'expect to return blocks execution value' do
      expect(instrumentation).to eq result
    end
  end

  describe '#subscribe' do
    context 'when we have a block based listener' do
      let(:subscription) { monitor.subscribe(event_name) {} }

      context 'when we try to subscribe to an unsupported event' do
        let(:event_name) { 'unsupported' }

        it { expect { subscription }.to raise_error Dry::Events::InvalidSubscriberError }
      end

      context 'when we try to subscribe to a supported event' do
        let(:event_name) { 'message.produced_async' }

        it { expect { subscription }.not_to raise_error }
      end
    end

    context 'when we have an object listener' do
      let(:subscription) { monitor.subscribe(listener.new) }
      let(:listener) do
        Class.new do
          def on_message_produced_async(_event)
            true
          end
        end
      end

      it { expect { subscription }.not_to raise_error }
    end
  end
end
