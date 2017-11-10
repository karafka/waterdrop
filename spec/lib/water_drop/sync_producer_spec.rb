# frozen_string_literal: true

RSpec.describe WaterDrop::SyncProducer do
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
      context 'when the deliver flag is set to false' do
        before { allow(WaterDrop.config).to receive(:deliver).and_return(false) }

        it 'expect not to pass to ruby-kafka' do
          expect(DeliveryBoy).not_to receive(:deliver)
          expect { delivery }.not_to raise_error
        end
      end

      context 'but the deliver flag is set to true' do
        before { allow(WaterDrop.config).to receive(:deliver).and_return(true) }

        it 'expect to pass to ruby-kafka' do
          expect(DeliveryBoy).to receive(:deliver).with(message, topic: topic)
          expect { delivery }.not_to raise_error
        end
      end
    end

    context 'when there was a kafka error' do
      context 'when it happened only once' do
        before do
          call_count = 0
          allow(DeliveryBoy).to receive(:deliver) do
            call_count += 1
            call_count == 1 ? raise(Kafka::Error) : nil
          end
        end

        it { expect { delivery }.not_to raise_error }
      end

      context 'when it happened more times than max_retries' do
        before { allow(DeliveryBoy).to receive(:deliver).and_raise(Kafka::Error) }

        it { expect { delivery }.to raise_error(Kafka::Error) }
      end
    end
  end
end
