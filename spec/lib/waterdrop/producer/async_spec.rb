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

    context 'when message is valid with array headers' do
      let(:message) { build(:valid_message, headers: { 'a' => %w[b c] }) }

      it { expect(delivery).to be_a(Rdkafka::Producer::DeliveryHandle) }
    end

    context 'when message is valid and with label' do
      let(:message) { build(:valid_message, label: 'test') }

      it { expect(delivery.label).to eq('test') }
    end

    context 'when sending a tombstone message' do
      let(:message) { build(:valid_message, payload: nil) }

      it { expect(delivery).to be_a(Rdkafka::Producer::DeliveryHandle) }
    end

    context 'when producing with good middleware' do
      let(:message) { build(:valid_message, payload: nil) }

      before do
        producer.produce_sync(topic: message[:topic], payload: nil)

        mid = lambda do |msg|
          msg[:partition_key] = '1'
          msg
        end

        producer.middleware.append mid
      end

      it { expect(delivery).to be_a(Rdkafka::Producer::DeliveryHandle) }
    end

    context 'when producing with corrupted middleware' do
      let(:message) { build(:valid_message, payload: nil) }

      before do
        mid = lambda do |msg|
          msg[:partition_key] = -1
          msg
        end

        producer.middleware.append mid
      end

      it { expect { delivery }.to raise_error(WaterDrop::Errors::MessageInvalidError) }
    end

    context 'when inline error occurs in librdkafka and we do not retry on queue full' do
      let(:errors) { [] }
      let(:occurred) { [] }
      let(:error) { errors.first }
      let(:producer) { build(:limited_producer) }

      before do
        producer.monitor.subscribe('error.occurred') do |event|
          # Avoid side effects
          event.payload[:error] = event[:error].dup
          occurred << event
        end

        begin
          message = build(:valid_message)
          100.times { producer.produce_async(message) }
        rescue WaterDrop::Errors::ProduceError => e
          errors << e
        end
      end

      it { expect(error).to be_a(WaterDrop::Errors::ProduceError) }
      it { expect(error.message).to eq(error.cause.inspect) }
      it { expect(error.cause).to be_a(Rdkafka::RdkafkaError) }
      it { expect(occurred.first.payload[:error].cause).to be_a(Rdkafka::RdkafkaError) }
      it { expect(occurred.first.payload[:type]).to eq('message.produce_async') }
    end

    context 'when inline error occurs in librdkafka and we retry on queue full' do
      let(:errors) { [] }
      let(:occurred) { [] }
      let(:error) { errors.first }
      let(:producer) { build(:slow_producer, wait_on_queue_full: true) }

      before do
        producer.config.wait_on_queue_full = true

        producer.monitor.subscribe('error.occurred') do |event|
          occurred << event
        end

        begin
          message = build(:valid_message, label: 'test')
          5.times { producer.produce_async(message) }
        rescue WaterDrop::Errors::ProduceError => e
          errors << e
        end
      end

      it { expect(errors).to be_empty }
      it { expect(occurred.first.payload[:error].cause).to be_a(Rdkafka::RdkafkaError) }
      it { expect(occurred.first.payload[:type]).to eq('message.produce_async') }
      it { expect(occurred.first.payload[:label]).to be_nil }
    end

    context 'when linger is longer than shutdown' do
      let(:occurred) { [] }
      let(:error) { occurred.first[:error] }

      before do
        while occurred.empty?
          # On fast CPUs we may actually be fast enough to dispatch it to local kafka with ack
          # within 1ms and no errors will occur. That's why we do repeat it
          producer = build(
            :slow_producer,
            kafka: {
              'bootstrap.servers': 'localhost:9092',
              'queue.buffering.max.ms': 0,
              'message.timeout.ms': 1
            }
          )

          producer.monitor.subscribe('error.occurred') do |event|
            occurred << event
          end

          message = build(:valid_message, label: 'test')
          100.times { producer.produce_async(message) }
          producer.close
        end
      end

      it { expect(occurred).not_to be_empty }
      it { expect(error).to be_a(Rdkafka::RdkafkaError) }
      it { expect(error.code).to eq(:msg_timed_out) }
    end

    context 'when inline error occurs and we retry on queue full but instrumentation off' do
      let(:errors) { [] }
      let(:occurred) { [] }
      let(:error) { errors.first }
      let(:producer) { build(:slow_producer, wait_on_queue_full: true) }

      before do
        producer.config.wait_on_queue_full = true
        producer.config.instrument_on_wait_queue_full = false

        producer.monitor.subscribe('error.occurred') do |event|
          occurred << event
        end

        begin
          message = build(:valid_message, label: 'test')
          5.times { producer.produce_async(message) }
        rescue WaterDrop::Errors::ProduceError => e
          errors << e
        end
      end

      it { expect(errors).to be_empty }
      it { expect(occurred).to be_empty }
    end

    context 'when inline error occurs in librdkafka and we go beyond max wait on queue full' do
      let(:errors) { [] }
      let(:occurred) { [] }
      let(:error) { errors.first }
      let(:producer) do
        build(
          :slow_producer,
          wait_on_queue_full: true,
          wait_timeout_on_queue_full: 0.5
        )
      end

      before do
        producer.config.wait_on_queue_full = true

        producer.monitor.subscribe('error.occurred') do |event|
          occurred << event
        end

        begin
          message = build(:valid_message, label: 'test')
          5.times { producer.produce_async(message) }
        rescue WaterDrop::Errors::ProduceError => e
          errors << e
        end
      end

      it { expect(errors).not_to be_empty }
      it { expect(occurred.first.payload[:error].cause).to be_a(Rdkafka::RdkafkaError) }
      it { expect(occurred.first.payload[:type]).to eq('message.produce_async') }
      it { expect(occurred.first.payload[:label]).to be_nil }
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
      let(:producer) { build(:limited_producer) }

      before do
        # Intercept the error so it won't bubble up as we want to check the notifications pipeline
        producer.produce_many_async(messages)
      rescue WaterDrop::Errors::ProduceError => e
        errors << e
      end

      it { expect(error.dispatched.size).to eq(1) }
      it { expect(error.dispatched.first).to be_a(Rdkafka::Producer::DeliveryHandle) }
      it { expect(error).to be_a(WaterDrop::Errors::ProduceError) }
      it { expect(error.message).to eq(error.cause.inspect) }
      it { expect(error.cause).to be_a(Rdkafka::RdkafkaError) }
    end

    context 'when there are dispatched messages not in kafka yet' do
      let(:producer) do
        build(
          :slow_producer,
          kafka: {
            'bootstrap.servers': 'localhost:9092',
            'queue.buffering.max.ms': 5_000,
            'queue.buffering.max.messages': 2_000
          }
        )
      end

      let(:dispatched) do
        message = build(:valid_message, label: 'test')
        Array.new(1_000) { producer.produce_async(message) }
      end

      it 'expect not to allow for disconnect' do
        expect(producer.disconnect).to be(false)
      end

      it 'expect to allow disconnect after they are dispatched' do
        dispatched.each(&:wait)
        expect(producer.disconnect).to be(true)
      end
    end
  end
end
