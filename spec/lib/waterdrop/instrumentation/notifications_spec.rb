# frozen_string_literal: true

RSpec.describe_current do
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

        it do
          expected_error = Karafka::Core::Monitoring::Notifications::EventNotRegistered
          expect { subscription }.to raise_error expected_error
        end
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

  describe 'producer lifecycle events flow' do
    subject(:status) { producer.status }

    let(:producer) { WaterDrop::Producer.new }
    let(:events) { [] }
    let(:events_names) do
      %w[
        producer.connected
        producer.closing
        producer.closed
      ]
    end

    after { producer.close }

    context 'when producer is initialized' do
      it { expect(status.to_s).to eq('initial') }
      it { expect(events).to be_empty }
    end

    context 'when producer is configured' do
      before do
        producer.setup {}

        events_names.each do |event_name|
          producer.monitor.subscribe(event_name) do |event|
            events << event
          end
        end
      end

      it { expect(status.to_s).to eq('configured') }
      it { expect(events).to be_empty }
    end

    context 'when producer is connected' do
      before do
        producer.setup {}

        events_names.each do |event_name|
          producer.monitor.subscribe(event_name) do |event|
            events << event
          end
        end

        producer.client
      end

      it { expect(status.to_s).to eq('connected') }
      it { expect(events.size).to eq(1) }
      it { expect(events.last.id).to eq('producer.connected') }
      it { expect(events.last.payload.key?(:producer_id)).to be(true) }
    end

    context 'when producer is closed' do
      before do
        producer.setup {}

        events_names.each do |event_name|
          producer.monitor.subscribe(event_name) do |event|
            events << event
          end
        end

        producer.client
        producer.close
      end

      it { expect(status.to_s).to eq('closed') }
      it { expect(events.size).to eq(3) }
      it { expect(events.first.id).to eq('producer.connected') }
      it { expect(events.first.payload.key?(:producer_id)).to be(true) }
      it { expect(events[1].id).to eq('producer.closing') }
      it { expect(events[1].payload.key?(:producer_id)).to be(true) }
      it { expect(events.last.id).to eq('producer.closed') }
      it { expect(events.last.payload.key?(:producer_id)).to be(true) }
      it { expect(events.last.payload.key?(:time)).to be(true) }
    end
  end
end
