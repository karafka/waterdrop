# frozen_string_literal: true

RSpec.describe_current do
  subject(:client) { described_class.new(nil) }

  let(:producer) do
    WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = { 'bootstrap.servers': 'localhost:9092' }
    end
  end

  describe 'publishing interface' do
    it 'expect to return self for further chaining' do
      expect(client.publish_sync({})).to eq(client)
    end

    context 'when using producer #produce_async' do
      let(:handler) { producer.produce_async(topic: 'test', partition: 2, payload: '1') }

      it { expect(handler).to be_a(described_class::Handle) }
      it { expect(handler.wait.topic_name).to eq('test') }
      it { expect(handler.wait.partition).to eq(2) }
      it { expect(handler.wait.offset).to eq(0) }

      context 'with multiple dispatches to the same topic partition' do
        before do
          3.times { producer.produce_async(topic: 'test', partition: 2, payload: '1') }
        end

        it { expect(handler).to be_a(described_class::Handle) }
        it { expect(handler.wait.topic_name).to eq('test') }
        it { expect(handler.wait.partition).to eq(2) }
        it { expect(handler.wait.offset).to eq(3) }
      end

      context 'with multiple dispatches to different partitions' do
        before do
          3.times { |i| producer.produce_async(topic: 'test', partition: i, payload: '1') }
        end

        it { expect(handler).to be_a(described_class::Handle) }
        it { expect(handler.wait.topic_name).to eq('test') }
        it { expect(handler.wait.partition).to eq(2) }
        it { expect(handler.wait.offset).to eq(1) }
      end

      context 'with multiple dispatches to different topics' do
        before do
          3.times { |i| producer.produce_async(topic: "test#{i}", partition: 0, payload: '1') }
        end

        it { expect(handler).to be_a(described_class::Handle) }
        it { expect(handler.wait.topic_name).to eq('test') }
        it { expect(handler.wait.partition).to eq(2) }
        it { expect(handler.wait.offset).to eq(0) }
      end
    end

    context 'when using producer #produce_sync' do
      let(:report) { producer.produce_sync(topic: 'test', partition: 2, payload: '1') }

      it { expect(report).to be_a(Rdkafka::Producer::DeliveryReport) }
      it { expect(report.topic_name).to eq('test') }
      it { expect(report.partition).to eq(2) }
      it { expect(report.offset).to eq(0) }

      context 'with multiple dispatches to the same topic partition' do
        before do
          3.times { producer.produce_sync(topic: 'test', partition: 2, payload: '1') }
        end

        it { expect(report).to be_a(Rdkafka::Producer::DeliveryReport) }
        it { expect(report.topic_name).to eq('test') }
        it { expect(report.partition).to eq(2) }
        it { expect(report.offset).to eq(3) }
      end

      context 'with multiple dispatches to different partitions' do
        before do
          3.times { |i| producer.produce_sync(topic: 'test', partition: i, payload: '1') }
        end

        it { expect(report).to be_a(Rdkafka::Producer::DeliveryReport) }
        it { expect(report.topic_name).to eq('test') }
        it { expect(report.partition).to eq(2) }
        it { expect(report.offset).to eq(1) }
      end

      context 'with multiple dispatches to different topics' do
        before do
          3.times { |i| producer.produce_sync(topic: "test#{i}", partition: 0, payload: '1') }
        end

        it { expect(report).to be_a(Rdkafka::Producer::DeliveryReport) }
        it { expect(report.topic_name).to eq('test') }
        it { expect(report.partition).to eq(2) }
        it { expect(report.offset).to eq(0) }
      end
    end
  end

  describe '#respond_to?' do
    it { expect(client.respond_to?(:test)).to eq(true) }
  end

  describe '#transaction' do
    context 'when no error and no abort' do
      it 'expect to return the block value' do
        expect(producer.transaction { 1 }).to eq(1)
      end
    end

    context 'when WaterDrop::AbortTransaction occurs' do
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
  end
end
