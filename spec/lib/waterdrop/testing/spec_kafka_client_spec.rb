# frozen_string_literal: true

RSpec.describe_current do
  subject(:client) { described_class.new }

  let(:producer) do
    WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = { 'bootstrap.servers': 'localhost:9092' }
    end
  end

  before do
    allow(producer).to receive(:client).and_return(client)

    producer.produce_sync(payload: 'one', topic: 'foo')
    producer.produce_sync(payload: 'one', topic: 'bar')
    producer.produce_sync(payload: 'two', topic: 'foo')
  end

  describe '#messages' do
    it 'yields all produced messages' do
      expect(client.messages).to match(
        [
          { payload: 'one', topic: 'foo' },
          { payload: 'one', topic: 'bar' },
          { payload: 'two', topic: 'foo' }
        ]
      )
    end
  end

  describe '#messages_for' do
    context 'with topic that has messages produced to' do
      it 'yields corresponding messages' do
        expect(client.messages_for('foo')).to match(
          [
            { payload: 'one', topic: 'foo' },
            { payload: 'two', topic: 'foo' }
          ]
        )
        expect(client.messages_for('bar')).to match(
          [
            { payload: 'one', topic: 'bar' }
          ]
        )
      end
    end

    context 'when topic that was not produced to' do
      it 'yields no messages' do
        expect(client.messages_for('buzz')).to be_empty
      end
    end
  end

  describe '#reset' do
    it 'clears all buffers' do
      client.reset

      expect(client.messages).to be_empty
      expect(client.messages_for('foo')).to be_empty
    end
  end
end
