# frozen_string_literal: true

RSpec.describe_current do
  subject(:client) { producer.client }

  let(:producer) do
    WaterDrop::Producer.new do |config|
      config.deliver = true
      config.kafka = { 'bootstrap.servers': '127.0.0.1:9092' }
    end
  end

  after { producer.close }

  describe '#partition_count' do
    it { expect(client.partition_count('example_topic')).to eq(1) }

    context 'when the partition count value is already cached' do
      before do
        client.partition_count('example_topic')
        allow(::Rdkafka::Metadata).to receive(:new).and_call_original
      end

      it 'expect not to query it again' do
        client.partition_count('example_topic')
        expect(::Rdkafka::Metadata).not_to have_received(:new)
      end
    end

    context 'when the partition count value was cached but time expired' do
      before do
        allow(::Process).to receive(:clock_gettime).and_return(0, 30.001)
        client.partition_count('example_topic')
        allow(::Rdkafka::Metadata).to receive(:new).and_call_original
      end

      it 'expect not to query it again' do
        client.partition_count('example_topic')
        expect(::Rdkafka::Metadata).to have_received(:new)
      end
    end

    context 'when the partition count value was cached and time did not expire' do
      before do
        allow(::Process).to receive(:clock_gettime).and_return(0, 29.001)
        client.partition_count('example_topic')
        allow(::Rdkafka::Metadata).to receive(:new).and_call_original
      end

      it 'expect not to query it again' do
        client.partition_count('example_topic')
        expect(::Rdkafka::Metadata).not_to have_received(:new)
      end
    end
  end
end
