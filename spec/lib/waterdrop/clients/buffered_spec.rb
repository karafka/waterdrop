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
end
