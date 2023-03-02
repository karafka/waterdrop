# frozen_string_literal: true

RSpec.describe_current do
  subject(:producer) { build(:producer) }

  after { producer.close }

  describe '#produce_async' do
    subject(:delivery) { producer.produce_async(message) }

    context 'when message is invalid' do
      let(:message) { build(:invalid_message) }

      it { expect { delivery }.to raise_error(WaterDrop::Errors::MessageInvalidError) }
    end

    context 'when message is valid' do
      let(:message) { build(:valid_message) }

      it { expect(delivery).to be_a(Rdkafka::Producer::DeliveryHandle) }
    end

    context 'when sending a tombstone message' do
      let(:message) { build(:valid_message, payload: nil) }

      it { expect(delivery).to be_a(Rdkafka::Producer::DeliveryHandle) }
    end

    context 'when producing with good middleware' do
      before do
        mid = lambda do |msg|
          msg[:partition_key] = '1'
          msg
        end

        producer.middleware.append mid
      end

      let(:message) { build(:valid_message, payload: nil) }

      it { expect(delivery).to be_a(Rdkafka::Producer::DeliveryHandle) }
    end

    context 'when producing with corrupted middleware' do
      before do
        mid = lambda do |msg|
          msg[:partition_key] = -1
          msg
        end

        producer.middleware.append mid
      end

      let(:message) { build(:valid_message, payload: nil) }

      it { expect { delivery }.to raise_error(WaterDrop::Errors::MessageInvalidError) }
    end
  end

  describe '#produce_many_async' do
    subject(:delivery) { producer.produce_many_async(messages) }

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
        expect(delivery).to all be_a(Rdkafka::Producer::DeliveryHandle)
      end
    end

    context 'when inline error occurs in librdkafka' do
      let(:errors) { [] }
      let(:error) { errors.first }
      let(:messages) { Array.new(100) { build(:valid_message) } }
      let(:producer) do
        build(
          :producer,
          kafka: {
            'bootstrap.servers': 'localhost:9092',
            'request.required.acks': 1,
            # Those will cause inline buffer overflow
            'queue.buffering.max.messages': 1,
            'queue.buffering.max.ms': 10_000
          }
        )
      end

      before do
        # Intercept the error so it won't bubble up as we want to check the notifications pipeline
        begin
          producer.produce_many_async(messages)
        rescue WaterDrop::Errors::ProduceError => e
          errors << e
        end
      end

      it { expect(error.dispatched.size).to eq(1) }
      it { expect(error.dispatched.first).to be_a(Rdkafka::Producer::DeliveryHandle) }
      it { expect(error).to be_a(WaterDrop::Errors::ProduceError) }
      it { expect(error.cause).to be_a(Rdkafka::RdkafkaError) }
    end
  end
end
