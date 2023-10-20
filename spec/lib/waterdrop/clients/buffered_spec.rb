# frozen_string_literal: true

RSpec.describe_current do
  subject(:client) { described_class.new(producer) }

  let(:producer) do
    WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = { 'bootstrap.servers': 'localhost:9092' }
    end
  end

  let(:all_messages) do
    [
      { payload: 'one', topic: 'foo' },
      { payload: 'one', topic: 'bar' },
      { payload: 'two', topic: 'foo' }
    ]
  end

  let(:foo_messages) do
    [
      { payload: 'one', topic: 'foo' },
      { payload: 'two', topic: 'foo' }
    ]
  end

  let(:bar_messages) do
    [
      { payload: 'one', topic: 'bar' }
    ]
  end

  before do
    allow(producer).to receive(:client).and_return(client)

    producer.produce_sync(payload: 'one', topic: 'foo')
    producer.produce_sync(payload: 'one', topic: 'bar')
    producer.produce_sync(payload: 'two', topic: 'foo')
  end

  after { producer.close }

  describe '#messages' do
    subject { client.messages }

    it { is_expected.to match(all_messages) }
  end

  describe '#messages_for' do
    context 'with topic that has messages produced to it' do
      it { expect(client.messages_for('foo')).to match(foo_messages) }
      it { expect(client.messages_for('bar')).to match(bar_messages) }
    end

    context 'with topic that has no messages produced to it' do
      it { expect(client.messages_for('buzz')).to be_empty }
    end
  end

  describe '#reset' do
    before { client.reset }

    it { expect(client.messages).to be_empty }
    it { expect(client.messages_for('foo')).to be_empty }
  end

  describe '#transaction' do
    context 'when no error and no abort' do
      it 'expect to return the block value' do
        expect(client.transaction { 1 }).to eq(1)
      end
    end

    context 'when running transaction with production of messages' do
      it 'expect to add them to the buffers' do
        client.transaction do
          client.produce(topic: 'test', payload: 'test')
          client.produce(topic: 'test', payload: 'test')
        end

        expect(client.messages.size).to eq(5)
        expect(client.messages_for('test').size).to eq(2)
      end
    end

    context 'when running nested transaction with production of messages' do
      it 'expect to add them to the buffers' do
        client.transaction do
          client.produce(topic: 'test', payload: 'test')
          client.produce(topic: 'test', payload: 'test')

          client.transaction do
            client.produce(topic: 'test', payload: 'test')
            client.produce(topic: 'test', payload: 'test')
          end
        end

        expect(client.messages.size).to eq(7)
        expect(client.messages_for('test').size).to eq(4)
      end
    end

    context 'when running nested transaction with production of messages on abort' do
      it 'expect to add them to the buffers' do
        client.transaction do
          client.produce(topic: 'test', payload: 'test')
          client.produce(topic: 'test', payload: 'test')

          client.transaction do
            client.produce(topic: 'test', payload: 'test')
            client.produce(topic: 'test', payload: 'test')

            throw(:abort)
          end
        end

        expect(client.messages.size).to eq(3)
        expect(client.messages_for('test').size).to eq(0)
      end
    end

    context 'when abort occurs' do
      it 'expect not to raise error' do
        expect do
          client.transaction { throw(:abort) }
        end.not_to raise_error
      end

      it 'expect not to contain messages from the aborted transaction' do
        client.transaction do
          client.produce(topic: 'test', payload: 'test')
          throw(:abort)
        end

        expect(client.messages.size).to eq(3)
        expect(client.messages_for('test')).to be_empty
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

      it 'expect not to contain messages from the aborted transaction' do
        expect do
          client.transaction do
            client.produce(topic: 'test', payload: 'test')

            raise StandardError
          end
        end.to raise_error(StandardError)

        expect(client.messages.size).to eq(3)
        expect(client.messages_for('test')).to be_empty
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
