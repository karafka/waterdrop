# frozen_string_literal: true

RSpec.describe_current do
  subject(:producer) { build(:producer).tap(&:client).client }

  after { producer.close unless producer.closed? }

  describe '#queue_size' do
    it { expect(producer.queue_size).to be_a(Integer) }
    it { expect(producer.queue_size).to eq(0) }
  end

  describe '#queue_length' do
    it { expect(producer.queue_length).to eq(producer.queue_size) }
  end

  context 'when producer is closed' do
    before { producer.close }

    it 'expect to raise error' do
      expect { producer.queue_size }.to raise_error(Rdkafka::ClosedProducerError)
    end
  end
end
