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

      it 'expect to instrument producer.reload and producer.reloaded events during reload' do
        reload_events = []
        producer.monitor.subscribe('producer.reload') { |event| reload_events << event }

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

        # Verify producer.reload event was emitted
        expect(reload_events.size).to eq(1)
        expect(reload_events.first[:producer_id]).to eq(producer.id)
        expect(reload_events.first[:error]).to eq(fatal_error)
        expect(reload_events.first[:attempt]).to eq(1)
        expect(reload_events.first[:caller]).to eq(producer)

        # Verify producer.reloaded event was emitted
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

    context 'when config modification is done via producer.reload event' do
      subject(:producer) do
        build(:idempotent_producer, reload_on_idempotent_fatal_error: true)
      end

      let(:fatal_error) { Rdkafka::RdkafkaError.new(-150, fatal: true) }
      let(:reload_events) { [] }

      before do
        # Subscribe to producer.reload and modify config directly
        producer.monitor.subscribe('producer.reload') do |event|
          # User modifies kafka config via event[:caller].config
          event[:caller].config.kafka[:'enable.idempotence'] = false
        end

        producer.monitor.subscribe('producer.reloaded') { |event| reload_events << event }
      end

      it 'expect to apply config.kafka changes from the event' do
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
        allow(mock_builder).to receive(:call) do |_prod, config|
          # Verify config was updated
          expect(config.kafka[:'enable.idempotence']).to eq(false)

          mock_client = instance_double(Rdkafka::Producer)
          allow(mock_client).to receive(:produce) do
            call_count += 1
            instance_double(Rdkafka::Producer::DeliveryHandle, wait: nil)
          end
          allow(mock_client).to receive(:flush)
          allow(mock_client).to receive(:close)
          allow(mock_client).to receive(:closed?).and_return(false)
          allow(mock_client).to receive(:purge)

          mock_client
        end

        # This should trigger reload with config change
        producer.produce_sync(message)

        # Verify reload happened
        expect(reload_events.size).to eq(1)

        # Verify cached state was cleared
        expect(producer.instance_variable_get(:@idempotent)).to be_nil
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

    context 'when using Testing helper to trigger real fatal errors' do
      subject(:producer) do
        build(:idempotent_producer, reload_on_idempotent_fatal_error: true)
      end

      before do
        # Include the Testing module to enable fatal error injection
        producer.singleton_class.include(WaterDrop::Producer::Testing)
      end

      it 'expect to trigger a fatal error and have it queryable' do
        # Trigger a real fatal error using the testing helper
        result = producer.trigger_test_fatal_error(47, 'Test producer epoch error')

        # Verify the trigger was successful
        expect(result).to eq(0)

        # Verify the fatal error is now present and queryable
        fatal_error = producer.fatal_error
        expect(fatal_error).not_to be_nil
        expect(fatal_error[:error_code]).to eq(47)
        expect(fatal_error[:error_string]).to include('test_fatal_error')
      end

      it 'expect to detect fatal error state after triggering' do
        # Trigger a real fatal error
        producer.trigger_test_fatal_error(47, 'Injected test error')

        # Verify fatal error is present and has correct error code
        fatal_error = producer.fatal_error
        expect(fatal_error).not_to be_nil
        expect(fatal_error[:error_code]).to eq(47)
      end
    end

    context 'with real fatal error injection and reload testing' do
      subject(:producer) do
        build(:idempotent_producer, reload_on_idempotent_fatal_error: true)
      end

      before do
        producer.singleton_class.include(WaterDrop::Producer::Testing)
      end

      it 'can inject fatal errors with various error codes' do
        # Error code 47 (INVALID_PRODUCER_EPOCH)
        result = producer.trigger_test_fatal_error(47, 'Simulated producer fencing')
        expect(result).to eq(0)

        fatal_error = producer.fatal_error
        expect(fatal_error).not_to be_nil
        expect(fatal_error[:error_code]).to eq(47)
        expect(fatal_error[:error_string]).to be_a(String)
        expect(fatal_error[:error_string]).not_to be_empty
      end

      it 'persists fatal error state across multiple queries' do
        producer.trigger_test_fatal_error(47, 'Test error persistence')

        # Query multiple times
        first_query = producer.fatal_error
        second_query = producer.fatal_error
        third_query = producer.fatal_error

        expect(first_query).to eq(second_query)
        expect(second_query).to eq(third_query)
        expect(first_query[:error_code]).to eq(47)
      end

      it 'maintains fatal error state after triggering' do
        # Trigger fatal error
        producer.trigger_test_fatal_error(47, 'Test fatal error state')

        # Verify error is present
        expect(producer.fatal_error).not_to be_nil

        # Error state should persist
        sleep 0.1
        expect(producer.fatal_error).not_to be_nil
        expect(producer.fatal_error[:error_code]).to eq(47)
      end

      it 'detects real fatal errors correctly' do
        # First, produce a message successfully to ensure producer is working
        report = producer.produce_sync(message)
        expect(report).to be_a(Rdkafka::Producer::DeliveryReport)
        expect(report.error).to be_nil

        # Now inject a real fatal error
        producer.trigger_test_fatal_error(47, 'Test reload trigger')

        # Verify the fatal error is present
        fatal_error = producer.fatal_error
        expect(fatal_error).not_to be_nil
        expect(fatal_error[:error_code]).to eq(47)
      end

      it 'can create new producer after fatal error for recovery' do
        # Trigger fatal error on first producer
        producer.trigger_test_fatal_error(47, 'First producer fatal')
        expect(producer.fatal_error).not_to be_nil

        # Close it
        producer.close

        # Create new producer - should work fine
        new_producer = build(:idempotent_producer)
        new_producer.singleton_class.include(WaterDrop::Producer::Testing)

        # Verify new producer has no fatal error
        expect(new_producer.fatal_error).to be_nil

        # Should be able to produce
        expect do
          new_producer.produce_sync(message)
        end.not_to raise_error

        new_producer.close
      end
    end

    context 'with real fatal error injection and reload event instrumentation' do
      subject(:producer) do
        build(
          :idempotent_producer,
          reload_on_idempotent_fatal_error: true,
          max_attempts_on_idempotent_fatal_error: 3,
          wait_backoff_on_idempotent_fatal_error: 100
        )
      end

      let(:reload_events) { [] }
      let(:reloaded_events) { [] }
      let(:error_events) { [] }

      before do
        producer.singleton_class.include(WaterDrop::Producer::Testing)
        producer.monitor.subscribe('producer.reload') { |event| reload_events << event }
        producer.monitor.subscribe('producer.reloaded') { |event| reloaded_events << event }
        producer.monitor.subscribe('error.occurred') { |event| error_events << event }
      end

      it 'emits producer.reload and producer.reloaded events when fatal error triggers reload' do
        # Track the initial client to detect when reload happens
        initial_client = producer.client

        # Stub the initial client to raise fatal error once, then succeed
        call_count = 0
        allow(initial_client).to receive(:produce) do
          call_count += 1
          if call_count == 1
            # Trigger real fatal error before raising
            producer.trigger_test_fatal_error(47, 'Test reload with real fatal error')
            # Raise the error to trigger reload path
            raise Rdkafka::RdkafkaError.new(-150, fatal: true)
          else
            instance_double(Rdkafka::Producer::DeliveryHandle, wait: nil)
          end
        end

        # Mock the builder to return a new client after reload
        mock_builder = instance_double(WaterDrop::Producer::Builder)
        allow(WaterDrop::Producer::Builder).to receive(:new).and_return(mock_builder)
        allow(mock_builder).to receive(:call) do
          mock_client = instance_double(Rdkafka::Producer)
          allow(mock_client).to receive(:produce) do
            instance_double(Rdkafka::Producer::DeliveryHandle, wait: nil)
          end
          allow(mock_client).to receive(:flush)
          allow(mock_client).to receive(:close)
          allow(mock_client).to receive(:closed?).and_return(false)
          allow(mock_client).to receive(:purge)
          mock_client
        end

        # This should trigger reload
        producer.produce_sync(message)

        # Verify producer.reload event was emitted
        expect(reload_events.size).to eq(1)
        expect(reload_events.first[:producer_id]).to eq(producer.id)
        expect(reload_events.first[:error]).to be_a(Rdkafka::RdkafkaError)
        expect(reload_events.first[:error].fatal?).to be(true)
        expect(reload_events.first[:attempt]).to eq(1)
        expect(reload_events.first[:caller]).to eq(producer)

        # Verify producer.reloaded event was emitted
        expect(reloaded_events.size).to eq(1)
        expect(reloaded_events.first[:producer_id]).to eq(producer.id)
        expect(reloaded_events.first[:attempt]).to eq(1)
      end

      it 'emits multiple reload events for multiple fatal error attempts' do
        initial_client = producer.client

        # Trigger fatal error upfront so it's set in librdkafka state
        producer.trigger_test_fatal_error(47, 'Pre-set fatal error')

        # Stub to fail twice, then succeed
        call_count = 0
        allow(initial_client).to receive(:produce) do
          call_count += 1
          if call_count <= 2
            raise Rdkafka::RdkafkaError.new(-150, fatal: true)
          else
            instance_double(Rdkafka::Producer::DeliveryHandle, wait: nil)
          end
        end

        # Mock the builder for reload
        mock_builder = instance_double(WaterDrop::Producer::Builder)
        allow(WaterDrop::Producer::Builder).to receive(:new).and_return(mock_builder)
        allow(mock_builder).to receive(:call) do
          mock_client = instance_double(Rdkafka::Producer)
          allow(mock_client).to receive(:produce) do
            call_count += 1
            if call_count <= 2
              raise Rdkafka::RdkafkaError.new(-150, fatal: true)
            else
              instance_double(Rdkafka::Producer::DeliveryHandle, wait: nil)
            end
          end
          allow(mock_client).to receive(:flush)
          allow(mock_client).to receive(:close)
          allow(mock_client).to receive(:closed?).and_return(false)
          allow(mock_client).to receive(:purge)
          mock_client
        end

        # This should trigger two reloads
        producer.produce_sync(message)

        # Verify multiple reload events
        expect(reload_events.size).to eq(2)
        expect(reload_events[0][:attempt]).to eq(1)
        expect(reload_events[1][:attempt]).to eq(2)

        # Verify multiple reloaded events
        expect(reloaded_events.size).to eq(2)
        expect(reloaded_events[0][:attempt]).to eq(1)
        expect(reloaded_events[1][:attempt]).to eq(2)
      end

      it 'includes error.occurred event with fatal error details' do
        initial_client = producer.client

        # Stub to raise fatal error once
        call_count = 0
        allow(initial_client).to receive(:produce) do
          call_count += 1
          if call_count == 1
            producer.trigger_test_fatal_error(47, 'Test error.occurred event')
            raise Rdkafka::RdkafkaError.new(-150, fatal: true)
          else
            instance_double(Rdkafka::Producer::DeliveryHandle, wait: nil)
          end
        end

        # Mock the builder
        mock_builder = instance_double(WaterDrop::Producer::Builder)
        allow(WaterDrop::Producer::Builder).to receive(:new).and_return(mock_builder)
        allow(mock_builder).to receive(:call) do
          mock_client = instance_double(Rdkafka::Producer)
          allow(mock_client).to receive(:produce) do
            instance_double(Rdkafka::Producer::DeliveryHandle, wait: nil)
          end
          allow(mock_client).to receive(:flush)
          allow(mock_client).to receive(:close)
          allow(mock_client).to receive(:closed?).and_return(false)
          allow(mock_client).to receive(:purge)
          mock_client
        end

        # This should trigger reload
        producer.produce_sync(message)

        # Verify error.occurred event was emitted
        expect(error_events.size).to be >= 1
        fatal_error_events = error_events.select { |e| e[:type] == 'librdkafka.idempotent_fatal_error' }
        expect(fatal_error_events).not_to be_empty
        expect(fatal_error_events.first[:producer_id]).to eq(producer.id)
        expect(fatal_error_events.first[:error]).to be_a(Rdkafka::RdkafkaError)
        expect(fatal_error_events.first[:attempt]).to eq(1)
      end

      it 'allows config modification in producer.reload event with real fatal errors' do
        config_modified = false

        # Subscribe to reload event and modify config
        producer.monitor.subscribe('producer.reload') do |event|
          event[:caller].config.kafka[:'enable.idempotence'] = false
          config_modified = true
        end

        initial_client = producer.client

        # Stub to raise fatal error once
        call_count = 0
        allow(initial_client).to receive(:produce) do
          call_count += 1
          if call_count == 1
            producer.trigger_test_fatal_error(47, 'Test config modification')
            raise Rdkafka::RdkafkaError.new(-150, fatal: true)
          else
            instance_double(Rdkafka::Producer::DeliveryHandle, wait: nil)
          end
        end

        # Mock the builder to verify config was modified
        mock_builder = instance_double(WaterDrop::Producer::Builder)
        allow(WaterDrop::Producer::Builder).to receive(:new).and_return(mock_builder)
        allow(mock_builder).to receive(:call) do |_prod, config|
          # Verify config was updated in the reload event
          expect(config.kafka[:'enable.idempotence']).to eq(false)

          mock_client = instance_double(Rdkafka::Producer)
          allow(mock_client).to receive(:produce) do
            instance_double(Rdkafka::Producer::DeliveryHandle, wait: nil)
          end
          allow(mock_client).to receive(:flush)
          allow(mock_client).to receive(:close)
          allow(mock_client).to receive(:closed?).and_return(false)
          allow(mock_client).to receive(:purge)
          mock_client
        end

        # This should trigger reload with config modification
        producer.produce_sync(message)

        # Verify config was modified
        expect(config_modified).to be(true)
        expect(reload_events.size).to eq(1)
        expect(reloaded_events.size).to eq(1)
      end

      it 'does not emit reload events when fatal error is non-reloadable' do
        # Set fenced error as non-reloadable (default behavior)
        expect(producer.config.non_reloadable_errors).to include(:fenced)

        initial_client = producer.client

        # Stub to raise fenced error
        allow(initial_client).to receive(:produce) do
          producer.trigger_test_fatal_error(47, 'Non-reloadable fenced error')
          # Fenced error - code -144 from librdkafka
          raise Rdkafka::RdkafkaError.new(-144, fatal: true)
        end

        # Should raise without reloading
        expect do
          producer.produce_sync(message)
        end.to raise_error(WaterDrop::Errors::ProduceError)

        # Verify no reload events were emitted
        expect(reload_events).to be_empty
        expect(reloaded_events).to be_empty
      end
    end

    context 'when reload is disabled for idempotent producer' do
      subject(:producer) do
        build(
          :idempotent_producer,
          reload_on_idempotent_fatal_error: false,
          max_attempts_on_idempotent_fatal_error: 3,
          wait_backoff_on_idempotent_fatal_error: 100
        )
      end

      let(:reload_events) { [] }
      let(:reloaded_events) { [] }

      before do
        producer.singleton_class.include(WaterDrop::Producer::Testing)
        producer.monitor.subscribe('producer.reload') { |event| reload_events << event }
        producer.monitor.subscribe('producer.reloaded') { |event| reloaded_events << event }
      end

      it 'keeps throwing errors without reload when reload is disabled' do
        initial_client = producer.client

        # Trigger fatal error
        producer.trigger_test_fatal_error(47, 'Fatal error with reload disabled')

        # Stub to always raise fatal error
        allow(initial_client).to receive(:produce) do
          raise Rdkafka::RdkafkaError.new(-150, fatal: true)
        end

        # First attempt should raise
        expect do
          producer.produce_sync(message)
        end.to raise_error(WaterDrop::Errors::ProduceError)

        # Second attempt should also raise (no reload)
        expect do
          producer.produce_sync(message)
        end.to raise_error(WaterDrop::Errors::ProduceError)

        # Third attempt should also raise (no reload)
        expect do
          producer.produce_sync(message)
        end.to raise_error(WaterDrop::Errors::ProduceError)

        # Verify no reload events were emitted
        expect(reload_events).to be_empty
        expect(reloaded_events).to be_empty
      end

      it 'does not attempt reload even with multiple produce attempts' do
        initial_client = producer.client
        produce_attempts = 0

        # Trigger fatal error
        producer.trigger_test_fatal_error(47, 'Fatal error without reload')

        # Stub to count produce attempts
        allow(initial_client).to receive(:produce) do
          produce_attempts += 1
          raise Rdkafka::RdkafkaError.new(-150, fatal: true)
        end

        # Multiple produce attempts
        5.times do
          begin
            producer.produce_sync(message)
          rescue WaterDrop::Errors::ProduceError
            # Expected to fail
          end
        end

        # All attempts should have hit the same client (no reload)
        expect(produce_attempts).to eq(5)
        expect(reload_events).to be_empty
        expect(reloaded_events).to be_empty

        # Fatal error should still be present
        expect(producer.fatal_error).not_to be_nil
        expect(producer.fatal_error[:error_code]).to eq(47)
      end
    end

    context 'when calling produce methods after fatal state is reached' do
      subject(:producer) do
        build(
          :idempotent_producer,
          reload_on_idempotent_fatal_error: false
        )
      end

      let(:messages) { Array.new(3) { build(:valid_message, topic: topic_name) } }

      before do
        producer.singleton_class.include(WaterDrop::Producer::Testing)
        # Trigger fatal error to put producer in fatal state
        producer.trigger_test_fatal_error(47, 'Producer in fatal state')
      end

      it 'produce_sync raises error when producer is in fatal state' do
        error = nil
        begin
          producer.produce_sync(message)
        rescue WaterDrop::Errors::ProduceError => e
          error = e
        end

        expect(error).not_to be_nil
        expect(error.cause).to be_a(Rdkafka::RdkafkaError)
        expect(error.cause.fatal?).to be(true)

        # Verify fatal error persists
        expect(producer.fatal_error).not_to be_nil
        expect(producer.fatal_error[:error_code]).to eq(47)
      end

      it 'produce_async raises error when producer is in fatal state' do
        error = nil
        begin
          producer.produce_async(message)
        rescue WaterDrop::Errors::ProduceError => e
          error = e
        end

        expect(error).not_to be_nil
        expect(error.cause).to be_a(Rdkafka::RdkafkaError)
        expect(error.cause.fatal?).to be(true)

        # Verify fatal error persists
        expect(producer.fatal_error).not_to be_nil
      end

      it 'produce_many_sync raises error when producer is in fatal state' do
        error = nil
        begin
          producer.produce_many_sync(messages)
        rescue WaterDrop::Errors::ProduceManyError => e
          error = e
        end

        expect(error).not_to be_nil
        expect(error.cause).to be_a(Rdkafka::RdkafkaError)
        expect(error.cause.fatal?).to be(true)

        # Verify fatal error persists
        expect(producer.fatal_error).not_to be_nil
      end

      it 'produce_many_async raises error when producer is in fatal state' do
        error = nil
        begin
          producer.produce_many_async(messages)
        rescue WaterDrop::Errors::ProduceManyError => e
          error = e
        end

        expect(error).not_to be_nil
        expect(error.cause).to be_a(Rdkafka::RdkafkaError)
        expect(error.cause.fatal?).to be(true)

        # Verify fatal error persists
        expect(producer.fatal_error).not_to be_nil
      end

      it 'all produce methods fail consistently in fatal state' do
        # Try each method multiple times to verify consistent behavior
        3.times do
          expect { producer.produce_sync(message) }
            .to raise_error(WaterDrop::Errors::ProduceError) { |e|
              expect(e.cause).to be_a(Rdkafka::RdkafkaError)
              expect(e.cause.fatal?).to be(true)
            }
          expect { producer.produce_async(message) }
            .to raise_error(WaterDrop::Errors::ProduceError) { |e|
              expect(e.cause).to be_a(Rdkafka::RdkafkaError)
              expect(e.cause.fatal?).to be(true)
            }
          expect { producer.produce_many_sync(messages) }
            .to raise_error(WaterDrop::Errors::ProduceManyError) { |e|
              expect(e.cause).to be_a(Rdkafka::RdkafkaError)
              expect(e.cause.fatal?).to be(true)
            }
          expect { producer.produce_many_async(messages) }
            .to raise_error(WaterDrop::Errors::ProduceManyError) { |e|
              expect(e.cause).to be_a(Rdkafka::RdkafkaError)
              expect(e.cause.fatal?).to be(true)
            }
        end

        # Fatal error should still be present after all attempts
        expect(producer.fatal_error).not_to be_nil
        expect(producer.fatal_error[:error_code]).to eq(47)
      end

      it 'documents that producer remains unusable after fatal error without reload' do
        # First attempt fails
        expect { producer.produce_sync(message) }.to raise_error(WaterDrop::Errors::ProduceError)

        # Producer is still in fatal state
        expect(producer.fatal_error).not_to be_nil

        # Subsequent attempts also fail
        expect { producer.produce_sync(message) }.to raise_error(WaterDrop::Errors::ProduceError)
        expect { producer.produce_async(message) }.to raise_error(WaterDrop::Errors::ProduceError)

        # Fatal error persists - producer needs reload or recreation
        expect(producer.fatal_error[:error_code]).to eq(47)
      end
    end
  end
end
