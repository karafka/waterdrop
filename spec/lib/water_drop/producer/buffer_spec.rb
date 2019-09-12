# frozen_string_literal: true

RSpec.describe WaterDrop::Producer::Buffer do
  subject(:producer) { build(:producer) }

  describe '#buffer' do
    subject(:buffering) { producer.buffer(message) }

    context 'when message is invalid' do
      let(:message) { build(:invalid_message) }

      it { expect { buffering }.to raise_error(WaterDrop::Errors::MessageInvalidError) }
    end

    context 'when message is valid' do
      let(:message) { build(:valid_message) }

      it { expect(buffering).to include(message) }
    end
  end

  describe '#buffer_many' do
    subject(:buffering) { producer.buffer_many(messages) }

    context 'when we have several invalid messages' do
      let(:messages) { Array.new(10) { build(:invalid_message) } }

      it { expect { buffering }.to raise_error(WaterDrop::Errors::MessageInvalidError) }
    end

    context 'when the last message out of a batch is invalid' do
      let(:messages) { [build(:valid_message), build(:invalid_message)] }

      before { allow(producer.client).to receive(:produce) }

      it { expect { buffering }.to raise_error(WaterDrop::Errors::MessageInvalidError) }

      it 'expect to never reach the client so no messages arent sent' do
        expect(producer.client).not_to have_received(:produce)
      end
    end

    context 'when we have several valid messages' do
      let(:messages) { Array.new(10) { build(:valid_message) } }

      it 'expect all the results to be buffered' do
        expect(buffering).to eq(messages)
      end
    end
  end

  describe '#flush_async' do
    pending
  end

  describe '#flush_sync' do
    pending
  end
end
