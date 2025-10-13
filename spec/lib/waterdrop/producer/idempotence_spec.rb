# frozen_string_literal: true

RSpec.describe_current do
  subject(:producer) { build(:producer) }

  after { producer.close }

  describe '#idempotent?' do
    context 'when producer is transactional' do
      subject(:producer) { build(:transactional_producer) }

      it 'expect to return true as all transactional producers are idempotent' do
        expect(producer.idempotent?).to be(true)
      end
    end

    context 'when producer has enable.idempotence set to true' do
      subject(:producer) { build(:idempotent_producer) }

      it 'expect to return true' do
        expect(producer.idempotent?).to be(true)
      end
    end

    context 'when producer has enable.idempotence set to false' do
      subject(:producer) do
        build(
          :producer,
          kafka: { 'bootstrap.servers': BOOTSTRAP_SERVERS, 'enable.idempotence': false }
        )
      end

      it 'expect to return false' do
        expect(producer.idempotent?).to be(false)
      end
    end

    context 'when producer does not have enable.idempotence configured' do
      subject(:producer) { build(:producer) }

      it 'expect to return false by default' do
        expect(producer.idempotent?).to be(false)
      end
    end

    context 'when idempotent? is called multiple times' do
      subject(:producer) { build(:idempotent_producer) }

      it 'expect to cache the result' do
        first_result = producer.idempotent?
        expect(producer.idempotent?).to eq(first_result)
        expect(producer.instance_variable_defined?(:@idempotent)).to be(true)
      end
    end
  end

  describe 'fatal error handling' do
    let(:topic_name) { "it-#{SecureRandom.uuid}" }
    let(:message) { build(:valid_message, topic: topic_name) }

    context 'when fatal error occurs with reload_on_idempotent_fatal_error enabled' do
      subject(:producer) do
        build(:idempotent_producer, reload_on_idempotent_fatal_error: true)
      end

      let(:fatal_error) { Rdkafka::RdkafkaError.new(-150, fatal: true) }
      let(:reloaded_events) { [] }

      before do
        producer.monitor.subscribe('producer.reloaded') { |event| reloaded_events << event }
      end

      it 'expect to instrument producer.reloaded event during reload' do
        call_count = 0

        # Stub the initial client's produce method
        allow(producer.client).to receive(:produce) do
          call_count += 1
          raise fatal_error if call_count == 1

          # Return a successful delivery handle
          instance_double(Rdkafka::Producer::DeliveryHandle, wait: nil)
        end

        # Create a mock builder instance
        mock_builder = instance_double(WaterDrop::Producer::Builder)

        # Stub Builder.new to return our mock builder
        allow(WaterDrop::Producer::Builder).to receive(:new).and_return(mock_builder)

        # Stub the builder's call method to return a mock client
        allow(mock_builder).to receive(:call) do
          mock_client = instance_double(Rdkafka::Producer)

          allow(mock_client).to receive(:produce) do
            call_count += 1
            instance_double(Rdkafka::Producer::DeliveryHandle, wait: nil)
          end

          # Mock other required methods for reload
          allow(mock_client).to receive(:flush)
          allow(mock_client).to receive(:close)
          allow(mock_client).to receive(:closed?).and_return(false)
          allow(mock_client).to receive(:purge)

          mock_client
        end

        # This should trigger one reload and succeed on retry
        producer.produce_sync(message)

        expect(reloaded_events.size).to eq(1)
        expect(reloaded_events.first[:producer_id]).to eq(producer.id)
        expect(reloaded_events.first[:attempt]).to eq(1)
      end

      it 'expect to include attempt number in instrumentation' do
        reloaded_events = []
        producer.monitor.subscribe('producer.reloaded') { |event| reloaded_events << event }

        produce_call_count = 0

        # Stub the initial client's produce method to fail twice
        allow(producer.client).to receive(:produce) do
          produce_call_count += 1
          raise fatal_error if produce_call_count <= 2

          # Return a successful delivery handle
          instance_double(Rdkafka::Producer::DeliveryHandle, wait: nil)
        end

        # Create a mock builder instance
        mock_builder = instance_double(WaterDrop::Producer::Builder)

        # Stub Builder.new to return our mock builder
        allow(WaterDrop::Producer::Builder).to receive(:new).and_return(mock_builder)

        # Stub the builder's call method to return mock clients
        allow(mock_builder).to receive(:call) do
          mock_client = instance_double(Rdkafka::Producer)

          allow(mock_client).to receive(:produce) do
            produce_call_count += 1
            raise fatal_error if produce_call_count <= 2

            instance_double(Rdkafka::Producer::DeliveryHandle, wait: nil)
          end

          # Mock other required methods for reload
          allow(mock_client).to receive(:flush)
          allow(mock_client).to receive(:close)
          allow(mock_client).to receive(:closed?).and_return(false)
          allow(mock_client).to receive(:purge)

          mock_client
        end

        # This should trigger two reloads and succeed on third attempt
        producer.produce_sync(message)

        expect(reloaded_events.size).to eq(2)
        expect(reloaded_events[0][:attempt]).to eq(1)
        expect(reloaded_events[1][:attempt]).to eq(2)
      end
    end

    context 'when fatal error occurs with reload_on_idempotent_fatal_error disabled' do
      subject(:producer) do
        build(:idempotent_producer, reload_on_idempotent_fatal_error: false)
      end

      let(:fatal_error) { Rdkafka::RdkafkaError.new(-150, fatal: true) }

      before do
        allow(producer.client).to receive(:produce).and_raise(fatal_error)
      end

      it 'expect to raise the fatal error without reload' do
        expect { producer.produce_sync(message) }
          .to raise_error(WaterDrop::Errors::ProduceError)
      end
    end

    context 'when non-reloadable fatal error (fenced) occurs' do
      subject(:producer) do
        build(:idempotent_producer, reload_on_idempotent_fatal_error: true)
      end

      # Fenced error - code -144 from librdkafka for producer fenced
      let(:fenced_error) { Rdkafka::RdkafkaError.new(-144, fatal: true) }
      let(:reloaded_events) { [] }

      before do
        producer.monitor.subscribe('producer.reloaded') { |event| reloaded_events << event }
        allow(producer.client).to receive(:produce).and_raise(fenced_error)
      end

      it 'expect not to reload and raise the error' do
        # This should raise because fenced errors are non-reloadable by default
        expect { producer.produce_sync(message) }
          .to raise_error(WaterDrop::Errors::ProduceError)
      end

      it 'expect not to emit producer.reloaded event' do
        begin
          producer.produce_sync(message)
        rescue WaterDrop::Errors::ProduceError
          nil
        end

        expect(reloaded_events).to be_empty
      end
    end

    context 'when fatal error occurs on transactional producer' do
      subject(:producer) do
        build(:transactional_producer, reload_on_idempotent_fatal_error: true)
      end

      let(:fatal_error) { Rdkafka::RdkafkaError.new(-150, fatal: true) }

      before do
        allow(producer.client).to receive(:produce).and_raise(fatal_error)
      end

      it 'expect not to use idempotent reload path' do
        # Transactional producers should not use the idempotent reload path
        expect { producer.produce_sync(message) }
          .to raise_error(WaterDrop::Errors::ProduceError)
      end
    end

    context 'when fatal error occurs on non-idempotent producer' do
      subject(:producer) do
        build(:producer, reload_on_idempotent_fatal_error: true)
      end

      let(:fatal_error) { Rdkafka::RdkafkaError.new(-150, fatal: true) }

      before do
        allow(producer.client).to receive(:produce).and_raise(fatal_error)
      end

      it 'expect not to reload as producer is not idempotent' do
        expect { producer.produce_sync(message) }
          .to raise_error(WaterDrop::Errors::ProduceError)
      end
    end

    context 'when non-fatal error occurs' do
      subject(:producer) do
        build(:idempotent_producer, reload_on_idempotent_fatal_error: true)
      end

      # Non-fatal error like queue_full
      let(:queue_full_error) { Rdkafka::RdkafkaError.new(-184, fatal: false) }

      before do
        allow(producer.client).to receive(:produce).and_raise(queue_full_error)
      end

      it 'expect not to reload for non-fatal errors' do
        # Should follow normal error handling path, not reload
        expect { producer.produce_sync(message) }
          .to raise_error(WaterDrop::Errors::ProduceError)
      end
    end
  end
end
