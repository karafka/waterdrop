# frozen_string_literal: true

RSpec.describe_current do
  subject(:producer) { build(:producer) }

  let(:topic_name) { "it-#{SecureRandom.uuid}" }

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

    context 'when message has array headers' do
      let(:message) { build(:valid_message, headers: { 'a' => %w[b c] }) }

      it { expect(delivery).to be_a(Rdkafka::Producer::DeliveryReport) }
    end

    context 'when message has invalid headers' do
      let(:message) { build(:valid_message, headers: { 'a' => %i[b c] }) }

      it { expect { delivery }.to raise_error(WaterDrop::Errors::MessageInvalidError) }
    end

    context 'when message is valid and with label' do
      let(:message) { build(:valid_message, label: 'test') }

      it { expect(delivery.label).to eq('test') }
    end

    context 'when producing with topic as a symbol' do
      let(:message) do
        msg = build(:valid_message)
        msg[:topic] = msg[:topic].to_sym
        msg
      end

      it { expect(delivery).to be_a(Rdkafka::Producer::DeliveryReport) }
    end

    context 'when producing sync to an unreachable cluster' do
      let(:message) { build(:valid_message) }
      let(:producer) { build(:unreachable_producer) }

      it 'expect to raise final error' do
        expect { producer.produce_sync(message) }
          .to raise_error(WaterDrop::Errors::ProduceError, /msg_timed_out/)
      end
    end

    context 'when producing sync to a topic that does not exist with partition_key' do
      let(:message) { build(:valid_message, partition_key: 'test', key: 'test') }

      it 'expect not to raise error and work correctly as the topic should be created' do
        expect(delivery).to be_a(Rdkafka::Producer::DeliveryReport)
      end
    end

    context 'when allow.auto.create.topics is set to false' do
      let(:message) { build(:valid_message) }

      let(:producer) do
        build(
          :producer,
          kafka: {
            'bootstrap.servers': 'localhost:9092',
            'allow.auto.create.topics': false,
            'message.timeout.ms': 500
          }
        )
      end

      it 'expect to raise final error' do
        expect { producer.produce_sync(message) }
          .to raise_error(WaterDrop::Errors::ProduceError, /msg_timed_out/)
      end
    end

    context 'when allow.auto.create.topics is set to false and we use partition key' do
      let(:message) { build(:valid_message, partition_key: 'test', key: 'test') }

      let(:producer) do
        build(
          :producer,
          kafka: {
            'bootstrap.servers': 'localhost:9092',
            'allow.auto.create.topics': false,
            'message.timeout.ms': 500
          }
        )
      end

      it 'expect to raise final error' do
        expect { producer.produce_sync(message) }
          .to raise_error(WaterDrop::Errors::ProduceError, /msg_timed_out/)
      end
    end

    context 'when inline error occurs in librdkafka' do
      let(:errors) { [] }
      let(:error) { errors.first }
      let(:occurred) { [] }
      let(:producer) { build(:limited_producer) }

      before do
        producer.monitor.subscribe('error.occurred') do |event|
          # Avoid side effects
          event.payload[:error] = event[:error].dup
          occurred << event
        end

        message = build(:valid_message, label: 'test')
        threads = Array.new(20) do
          Thread.new do
            producer.produce_sync(message)
          rescue StandardError => e
            errors << e
          end
        end

        threads.each(&:join)
      end

      it { expect(error).to be_a(WaterDrop::Errors::ProduceError) }
      it { expect(error.message).to eq(error.cause.inspect) }
      it { expect(error.cause).to be_a(Rdkafka::RdkafkaError) }
      it { expect(occurred.first.payload[:error].cause).to be_a(Rdkafka::RdkafkaError) }
      it { expect(occurred.first.payload[:type]).to eq('message.produce_sync') }
      # We expect this to be nil because the error was raised by the code that was attempting to
      # produce, hence there is a chance of not even having a handler
      it { expect(occurred.first.payload[:label]).to be_nil }
    end
  end

  describe '#produce_sync with partition key' do
    subject(:delivery) { producer.produce_sync(message) }

    let(:message) { build(:valid_message, partition_key: rand.to_s, topic: topic_name) }

    before { producer.produce_sync(topic: topic_name, payload: '1') }

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
        expect(delivery).to all be_a(Rdkafka::Producer::DeliveryHandle)
      end
    end

    context 'when we have several valid messages with array headers' do
      let(:messages) { Array.new(10) { build(:valid_message, headers: { 'a' => %w[b c] }) } }

      it 'expect all the results to be delivery handles' do
        expect(delivery).to all be_a(Rdkafka::Producer::DeliveryHandle)
      end
    end
  end

  context 'when using compression.codec' do
    subject(:delivery) { producer.produce_sync(message) }

    let(:producer) do
      build(
        :producer,
        kafka: {
          'bootstrap.servers': 'localhost:9092',
          'compression.codec': codec
        }
      )
    end

    let(:message) { build(:valid_message) }

    context 'when it is gzip' do
      let(:codec) { 'gzip' }

      it { expect(delivery).to be_a(Rdkafka::Producer::DeliveryReport) }
    end

    context 'when it is installed zstd' do
      let(:codec) { 'zstd' }

      it { expect(delivery).to be_a(Rdkafka::Producer::DeliveryReport) }
    end

    context 'when it is installed lz4' do
      let(:codec) { 'lz4' }

      it { expect(delivery).to be_a(Rdkafka::Producer::DeliveryReport) }
    end

    context 'when it is installed snappy' do
      let(:codec) { 'snappy' }

      it { expect(delivery).to be_a(Rdkafka::Producer::DeliveryReport) }
    end
  end

  context 'when producing and disconnected in a loop' do
    let(:message) { build(:valid_message) }

    it 'expect to always disconnect and reconnect and continue to produce' do
      100.times do |i|
        expect(producer.produce_sync(message).offset).to eq(i)
        producer.disconnect
      end
    end
  end
end
