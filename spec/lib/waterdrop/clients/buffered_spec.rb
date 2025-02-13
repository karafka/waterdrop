# frozen_string_literal: true

RSpec.describe_current do
  subject(:client) { described_class.new(producer) }

  let(:topic_name) { "it-#{SecureRandom}" }

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
    let(:producer) do
      WaterDrop::Producer.new do |config|
        config.deliver = false
        config.kafka = {
          'bootstrap.servers': 'localhost:9092',
          'transactional.id': SecureRandom.uuid
        }
      end
    end

    context 'when no error and no abort' do
      it 'expect to return the block value' do
        expect(producer.transaction { 1 }).to eq(1)
      end
    end

    context 'when running transaction with production of messages' do
      it 'expect to add them to the buffers' do
        producer.transaction do
          producer.produce_sync(topic: topic_name, payload: 'test')
          producer.produce_sync(topic: topic_name, payload: 'test')
        end

        expect(client.messages.size).to eq(5)
        expect(client.messages_for(topic_name).size).to eq(2)
      end
    end

    context 'when running nested transaction with production of messages' do
      it 'expect to add them to the buffers' do
        producer.transaction do
          producer.produce_sync(topic: topic_name, payload: 'test')
          producer.produce_sync(topic: topic_name, payload: 'test')

          producer.transaction do
            producer.produce_sync(topic: topic_name, payload: 'test')
            producer.produce_sync(topic: topic_name, payload: 'test')
          end
        end

        expect(client.messages.size).to eq(7)
        expect(client.messages_for(topic_name).size).to eq(4)
      end
    end

    context 'when running nested transaction with production of messages on abort' do
      it 'expect to add them to the buffers' do
        producer.transaction do
          producer.produce_sync(topic: topic_name, payload: 'test')
          producer.produce_sync(topic: topic_name, payload: 'test')

          producer.transaction do
            producer.produce_sync(topic: topic_name, payload: 'test')
            producer.produce_sync(topic: topic_name, payload: 'test')

            raise WaterDrop::AbortTransaction
          end
        end

        expect(client.messages.size).to eq(3)
        expect(client.messages_for('test').size).to eq(0)
      end
    end

    context 'when abort occurs' do
      it 'expect not to raise error' do
        expect do
          producer.transaction { raise WaterDrop::AbortTransaction }
        end.not_to raise_error
      end

      it 'expect not to contain messages from the aborted transaction' do
        producer.transaction do
          producer.produce_sync(topic: topic_name, payload: 'test')

          raise WaterDrop::AbortTransaction
        end

        expect(client.messages.size).to eq(3)
        expect(client.messages_for('test')).to be_empty
      end
    end

    context 'when WaterDrop::AbortTransaction error occurs' do
      it 'expect not to raise error' do
        expect do
          producer.transaction { raise(WaterDrop::AbortTransaction) }
        end.not_to raise_error
      end
    end

    context 'when different error occurs' do
      it 'expect to raise error' do
        expect do
          producer.transaction { raise(StandardError) }
        end.to raise_error(StandardError)
      end

      it 'expect not to contain messages from the aborted transaction' do
        expect do
          producer.transaction do
            producer.produce_sync(topic: topic_name, payload: 'test')

            raise StandardError
          end
        end.to raise_error(StandardError)

        expect(client.messages.size).to eq(3)
        expect(client.messages_for('test')).to be_empty
      end
    end

    context 'when running a nested transaction' do
      it 'expect to work ok' do
        result = producer.transaction do
          producer.transaction do
            producer.produce_sync(topic: '1', payload: '2')
            2
          end
        end

        expect(result).to eq(2)
      end
    end

    context 'when we try to store offset without a transaction' do
      let(:topic) { "it-#{SecureRandom.uuid}" }
      let(:message) { OpenStruct.new(topic: topic, partition: 0, offset: 10) }

      it 'expect to raise an error' do
        expect { producer.transaction_mark_as_consumed(nil, message) }
          .to raise_error(WaterDrop::Errors::TransactionRequiredError)
      end
    end

    context 'when trying to store offset with transaction' do
      let(:topic) { "it-#{SecureRandom.uuid}" }
      let(:consumer) { OpenStruct.new(consumer_group_metadata_pointer: nil) }
      let(:message) { OpenStruct.new(topic: topic, partition: 0, offset: 10) }

      it do
        expect do
          producer.transaction do
            producer.transaction_mark_as_consumed(consumer, message)
          end
        end.not_to raise_error
      end
    end
  end
end
