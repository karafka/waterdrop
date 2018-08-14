# frozen_string_literal: true

RSpec.describe WaterDrop::Instrumentation::StdoutListener do
  let(:event) { Dry::Events::Event.new(rand, payload) }
  let(:attempts_count) { rand(10) }
  let(:error) { Kafka::Error }
  let(:options) { { topic: rand.to_s } }
  let(:payload) { { attempts_count: attempts_count, error: error, options: options } }

  describe '#on_sync_producer_call_retry' do
    subject(:trigger) { described_class.on_sync_producer_call_retry(event) }

    let(:message) { "Attempt #{attempts_count} of delivery to: #{options} because of #{error}" }

    it 'expect logger to log proper message' do
      expect(WaterDrop.logger).to receive(:info).with(message)
      trigger
    end
  end

  describe 'on_sync_producer_call_error' do
    subject(:trigger) { described_class.on_sync_producer_call_error(event) }

    let(:message) { "Delivery failure to: #{options} because of #{error}" }

    it 'expect logger to log proper message' do
      expect(WaterDrop.logger).to receive(:error).with(message)
      trigger
    end
  end

  describe 'on_async_producer_call_retry' do
    subject(:trigger) { described_class.on_sync_producer_call_retry(event) }

    let(:message) { "Attempt #{attempts_count} of delivery to: #{options} because of #{error}" }

    it 'expect logger to log proper message' do
      expect(WaterDrop.logger).to receive(:info).with(message)
      trigger
    end
  end

  describe 'on_async_producer_call_error' do
    subject(:trigger) { described_class.on_async_producer_call_error(event) }

    let(:message) { "Delivery failure to: #{options} because of #{error}" }

    it 'expect logger to log proper message' do
      expect(WaterDrop.logger).to receive(:error).with(message)
      trigger
    end
  end
end
