# frozen_string_literal: true

RSpec.describe WaterDrop::SyncProducer do
  specify { expect(described_class).to eq WaterDrop::Producer }

  describe '#call' do
    subject(:delivery) { described_class.call(message, topic: topic) }

    let(:message) { rand.to_s }
    let(:topic) { 'typical-topic' }

    context 'when we want to send message with invalid options' do
      let(:topic) { '%^&*(' }
      let(:expected_error) { WaterDrop::Errors::InvalidMessageOptions }

      it 'expect not to pass to ruby-kafka and raise' do
        expect(DeliveryBoy).not_to receive(:deliver)
        expect { delivery }.to raise_error(expected_error)
      end
    end

    context 'when we send message with valid options' do

      context 'but the deliver flag is set to false' do
        before { expect(WaterDrop.config).to receive(:deliver).and_return(false) }

        it 'expect not to pass to ruby-kafka' do
          expect(DeliveryBoy).not_to receive(:deliver)
          expect { delivery }.not_to raise_error
        end
      end

      context 'but the deliver flag is set to true' do

        before { expect(WaterDrop.config).to receive(:deliver).and_return(true) }

        it 'expect to pass to ruby-kafka' do
          expect(DeliveryBoy).to receive(:deliver).with(message, topic: topic)
          expect { delivery }.not_to raise_error
        end
      end
    end
  end
end
