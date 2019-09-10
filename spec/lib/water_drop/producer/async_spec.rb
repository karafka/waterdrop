# frozen_string_literal: true

RSpec.describe WaterDrop::Producer::Async do
  let(:producer) { build(:producer) }

  describe '#produce_async' do
    subject(:delivery) { producer.produce_async(message) }

    let(:message) { {} }

    context 'when message is invalid' do
      it { expect { delivery }.to raise_error(WaterDrop::Errors::MessageInvalidError) }
    end

    context 'when message is valid' do
      let(:message) { { topic: rand.to_s, payload: rand.to_s } }

      it { expect(delivery).to be_a(Rdkafka::Producer::DeliveryHandle) }
    end
  end

  describe '#produce_many_async' do
    context 'when we have several messages' do
      pending
    end
  end
end
