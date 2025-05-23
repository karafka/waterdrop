# frozen_string_literal: true

RSpec.describe WaterDrop::Producer::Buffer do
  subject(:producer) { build(:producer) }

  let(:invalid_error) { WaterDrop::Errors::MessageInvalidError }

  after do
    producer.purge
    producer.close
  end

  describe '#buffer' do
    subject(:buffering) { producer.buffer(message) }

    context 'when producer is closed' do
      before { producer.close }

      let(:message) { build(:valid_message) }

      it { expect { buffering }.to raise_error(WaterDrop::Errors::ProducerClosedError) }
    end

    context 'when message is invalid' do
      let(:message) { build(:invalid_message) }

      it { expect { buffering }.not_to raise_error }

      it 'expect to raise on attempt to flush' do
        buffering
        expect { producer.flush_async }.to raise_error(invalid_error)
      end
    end

    context 'when message is valid' do
      let(:message) { build(:valid_message) }

      it { expect(buffering).to include(message) }
    end

    context 'with middleware' do
      let(:message) { build(:valid_message) }

      let(:middleware) do
        lambda do |message|
          message[:payload] += 'test '
          message
        end
      end

      before { producer.middleware.append(middleware) }

      it 'expect to run middleware only once during the flow' do
        producer.buffer(message)
        producer.flush_async
        expect(message[:payload].scan('test').size).to eq(1)
      end
    end
  end

  describe '#buffer_many' do
    subject(:buffering) { producer.buffer_many(messages) }

    context 'when producer is closed' do
      before { producer.close }

      let(:messages) { [build(:valid_message)] }

      it { expect { buffering }.to raise_error(WaterDrop::Errors::ProducerClosedError) }
    end

    context 'when we have several invalid messages' do
      let(:messages) { Array.new(10) { build(:invalid_message) } }

      it { expect { buffering }.not_to raise_error }

      it 'expect to validate on flush' do
        buffering
        expect { producer.flush_async }.to raise_error(invalid_error)
      end
    end

    context 'when the last message out of a batch is invalid' do
      let(:messages) { [build(:valid_message), build(:invalid_message)] }

      before { allow(producer.client).to receive(:produce) }

      it { expect { buffering }.not_to raise_error }

      it 'expect to never reach the client so no messages arent sent' do
        expect(producer.client).not_to have_received(:produce)
      end

      it 'expect to validate on flush' do
        buffering
        expect { producer.flush_async }.to raise_error(invalid_error)
      end
    end

    context 'when we have several valid messages' do
      let(:messages) { Array.new(10) { build(:valid_message) } }

      it 'expect all the results to be buffered' do
        expect(buffering).to eq(messages)
      end
    end

    context 'with middleware' do
      let(:message) { build(:valid_message) }

      let(:middleware) do
        lambda do |message|
          message[:payload] += 'test '
          message
        end
      end

      before { producer.middleware.append(middleware) }

      it 'expect to run middleware only once during the flow' do
        producer.buffer_many([message])
        producer.flush_async
        expect(message[:payload].scan('test').size).to eq(1)
      end
    end
  end

  describe '#flush_async' do
    subject(:flushing) { producer.flush_async }

    context 'when there are no messages in the buffer' do
      it { expect(flushing).to eq([]) }
    end

    context 'when there are messages in the buffer' do
      before { producer.buffer(build(:valid_message)) }

      it { expect(flushing[0]).to be_a(Rdkafka::Producer::DeliveryHandle) }
      it { expect(producer.tap(&:flush_async).messages).to be_empty }
    end

    context 'when an error occurred during flushing' do
      let(:error) { Rdkafka::RdkafkaError.new(0) }

      before do
        allow(producer.client).to receive(:produce).and_raise(error)
        producer.buffer(build(:valid_message))
      end

      it { expect { flushing }.to raise_error(WaterDrop::Errors::ProduceManyError) }
    end
  end

  describe '#flush_sync' do
    subject(:flushing) { producer.flush_sync }

    context 'when there are no messages in the buffer' do
      it { expect(flushing).to eq([]) }
    end

    context 'when there are messages in the buffer' do
      before { producer.buffer(build(:valid_message)) }

      it { expect(flushing[0]).to be_a(Rdkafka::Producer::DeliveryHandle) }
      it { expect(producer.tap(&:flush_sync).messages).to be_empty }
    end

    context 'when an error occurred during flushing' do
      let(:error) { Rdkafka::RdkafkaError.new(0) }

      before do
        allow(producer.client).to receive(:produce).and_raise(error)
        producer.buffer(build(:valid_message))
      end

      it { expect { flushing }.to raise_error(WaterDrop::Errors::ProduceManyError) }
    end
  end
end
