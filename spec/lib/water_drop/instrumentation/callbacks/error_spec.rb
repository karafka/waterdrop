# frozen_string_literal: true

RSpec.describe_current do
  subject(:callback) { described_class.new(producer_id, client_name, monitor) }

  let(:producer_id) { SecureRandom.uuid }
  let(:client_name) { SecureRandom.uuid }
  let(:monitor) { ::WaterDrop::Instrumentation::Monitor.new }
  let(:error) { ::Rdkafka::RdkafkaError.new(1, []) }

  describe '#call' do
    let(:changed) { [] }

    before do
      monitor.subscribe('error.emitted') do |event|
        changed << event.payload[:error]
      end

      callback.call(client_name, error)
    end

    context 'when emitted error refer different producer' do
      subject(:callback) { described_class.new(producer_id, 'other', monitor) }

      it 'expect not to emit them' do
        expect(changed).to be_empty
      end
    end

    context 'when emitted error refer to expected producer' do
      it 'expects to emit them' do
        expect(changed).to eq([error])
      end
    end
  end

  describe 'emitted event data format' do
    let(:changed) { [] }
    let(:event) { changed.first }

    before do
      monitor.subscribe('error.emitted') do |stat|
        changed << stat
      end

      callback.call(client_name, error)
    end

    it { expect(event.id).to eq('error.emitted') }
    it { expect(event.payload[:producer_id]).to eq(producer_id) }
    it { expect(event.payload[:error]).to eq(error) }
  end
end
