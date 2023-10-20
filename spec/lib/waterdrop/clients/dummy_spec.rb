# frozen_string_literal: true

RSpec.describe_current do
  subject(:client) { described_class.new(nil) }

  describe 'publishing interface' do
    it 'expect to return self for further chaining' do
      expect(client.publish_sync({})).to eq(client)
    end
  end

  describe '#wait' do
    it { expect(client.wait).to be_a(::Rdkafka::Producer::DeliveryReport) }

    context 'when we wait on many messages' do
      before { 10.times { client.wait } }

      it 'expect to bump the offset of the messages' do
        expect(client.wait.offset).to eq(10)
      end
    end
  end

  describe '#respond_to?' do
    it { expect(client.respond_to?(:test)).to eq(true) }
  end

  describe '#transaction' do
    context 'when no error and no abort' do
      it 'expect to return the block value' do
        expect(client.transaction { 1 }).to eq(1)
      end
    end

    context 'when abort occurs' do
      it 'expect not to raise error' do
        expect do
          client.transaction { throw(:abort) }
        end.not_to raise_error
      end
    end

    context 'when WaterDrop::Errors::AbortTransaction error occurs' do
      it 'expect not to raise error' do
        expect do
          client.transaction { raise(WaterDrop::Errors::AbortTransaction) }
        end.not_to raise_error
      end
    end

    context 'when different error occurs' do
      it 'expect to raise error' do
        expect do
          client.transaction { raise(StandardError) }
        end.to raise_error(StandardError)
      end
    end

    context 'when running a nested transaction' do
      it 'expect to work ok' do
        result = client.transaction do
          client.transaction do
            client.produce(topic: '1', payload: '2')
            2
          end
        end

        expect(result).to eq(2)
      end
    end
  end
end
