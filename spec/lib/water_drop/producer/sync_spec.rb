# frozen_string_literal: true

RSpec.describe WaterDrop::Producer::Sync do
  subject(:producer) { build(:producer) }

  after { producer.close }

  describe '#produce_sync' do
    subject(:delivery) { producer.produce_sync(message) }

    context 'when message is invalid' do
      let(:message) { build(:invalid_message) }

      it { expect { delivery }.to raise_error(WaterDrop::Errors::MessageInvalidError) }
    end

    context 'when message is valid' do
      let(:message) { build(:valid_message) }

      it { expect(delivery).to be_a(Rdkafka::Producer::DeliveryReport) }
    end
  end

  describe '#produce_sync with partition key' do
    subject(:delivery) { producer.produce_sync(message) }

    before do
      # There is a bug in rdkafka that causes error when sending first message with partition key
      # to a non-existing partition, that's why for this particular case we crete it before
      # @see https://github.com/appsignal/rdkafka-ruby/issues/163
      Rdkafka::Config.new(
        producer.config.kafka.to_h
      ).admin.create_topic(message[:topic], 1, 1)

      sleep(0.5)
    end

    let(:message) { build(:valid_message, partition_key: rand.to_s) }

    it { expect(delivery).to be_a(Rdkafka::Producer::DeliveryReport) }
  end

  describe '#produce_many_sync' do
    subject(:delivery) { producer.produce_many_sync(messages) }

    context 'when we have several invalid messages' do
      let(:messages) { Array.new(10) { build(:invalid_message) } }

      it { expect { delivery }.to raise_error(WaterDrop::Errors::MessageInvalidError) }
    end

    context 'when the last message out of a batch is invalid' do
      let(:messages) { [build(:valid_message), build(:invalid_message)] }

      before { allow(producer.client).to receive(:produce) }

      it { expect { delivery }.to raise_error(WaterDrop::Errors::MessageInvalidError) }

      it 'expect to never reach the client so no messages arent sent' do
        expect(producer.client).not_to have_received(:produce)
      end
    end

    context 'when we have several valid messages' do
      let(:messages) { Array.new(10) { build(:valid_message) } }

      it 'expect all the results to be delivery handles' do
        expect(delivery).to all be_a(Rdkafka::Producer::DeliveryReport)
      end
    end
  end
end
