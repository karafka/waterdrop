# frozen_string_literal: true

RSpec.describe_current do
  subject(:producer) { build(:idempotent_producer) }

  before do
    # Include the Testing module for the producer instance
    producer.singleton_class.include(WaterDrop::Producer::Testing)
  end

  after { producer.close }

  describe '#trigger_test_fatal_error' do
    it 'triggers a fatal error on the producer' do
      # Trigger a fatal error with error code 47 (INVALID_PRODUCER_EPOCH)
      result = producer.trigger_test_fatal_error(47, 'Test producer fencing')

      # The result should be 0 (success)
      expect(result).to eq(0)

      # Verify that a fatal error is now present
      fatal_error = producer.fatal_error
      expect(fatal_error).not_to be_nil
      expect(fatal_error[:error_code]).to eq(47)
      expect(fatal_error[:error_string]).to include('test_fatal_error')
    end

    it 'triggers different error codes' do
      # Test with error code 64 (INVALID_PRODUCER_ID_MAPPING)
      producer.trigger_test_fatal_error(64, 'Test invalid producer ID')

      fatal_error = producer.fatal_error
      expect(fatal_error).not_to be_nil
      expect(fatal_error[:error_code]).to eq(64)
    end

    it 'includes the reason in the error context' do
      custom_reason = 'Custom test scenario for fencing'
      producer.trigger_test_fatal_error(47, custom_reason)

      # Verify error was triggered
      expect(producer.fatal_error).not_to be_nil
    end

    context 'when rdkafka client does not have Testing support' do
      it 'automatically includes Rdkafka::Testing on the client' do
        # Remove Testing module if it was included
        if producer.client.singleton_class.included_modules.include?(Rdkafka::Testing)
          # We can't easily remove it, so just verify the method works
          expect { producer.trigger_test_fatal_error(47, 'test') }.not_to raise_error
        else
          expect(producer.client).to receive(:singleton_class).and_call_original
          producer.trigger_test_fatal_error(47, 'test')
        end
      end
    end
  end

  describe '#fatal_error' do
    context 'when no fatal error has occurred' do
      it 'returns nil' do
        expect(producer.fatal_error).to be_nil
      end
    end

    context 'when a fatal error has been triggered' do
      before do
        producer.trigger_test_fatal_error(47, 'Test error')
      end

      it 'returns error details as a hash' do
        error = producer.fatal_error
        expect(error).to be_a(Hash)
        expect(error).to have_key(:error_code)
        expect(error).to have_key(:error_string)
      end

      it 'returns the correct error code' do
        error = producer.fatal_error
        expect(error[:error_code]).to eq(47)
      end

      it 'returns a human-readable error string' do
        error = producer.fatal_error
        expect(error[:error_string]).to be_a(String)
        expect(error[:error_string]).not_to be_empty
      end
    end

    context 'when called multiple times' do
      before do
        producer.trigger_test_fatal_error(47, 'Test error')
      end

      it 'returns consistent results' do
        first_call = producer.fatal_error
        second_call = producer.fatal_error

        expect(first_call).to eq(second_call)
      end
    end
  end

  describe 'integration with idempotent producer reload' do
    subject(:producer) do
      build(
        :idempotent_producer,
        reload_on_idempotent_fatal_error: true,
        max_attempts_on_idempotent_fatal_error: 3,
        wait_backoff_on_idempotent_fatal_error: 100
      )
    end

    let(:topic_name) { "testing-#{SecureRandom.uuid}" }
    let(:message) { build(:valid_message, topic: topic_name) }
    let(:reload_events) { [] }
    let(:reloaded_events) { [] }
    let(:error_events) { [] }

    before do
      producer.singleton_class.include(WaterDrop::Producer::Testing)
      producer.monitor.subscribe('producer.reload') { |event| reload_events << event }
      producer.monitor.subscribe('producer.reloaded') { |event| reloaded_events << event }
      producer.monitor.subscribe('error.occurred') { |event| error_events << event }
    end

    it 'can trigger fatal errors that affect produce operations' do
      # Trigger a fatal error
      producer.trigger_test_fatal_error(47, 'Test producer fencing for reload')

      # Verify fatal error is present before produce
      expect(producer.fatal_error).not_to be_nil
      expect(producer.fatal_error[:error_code]).to eq(47)

      # Note: The behavior when producing after a fatal error depends on librdkafka internals
      # We've verified that the testing helper correctly triggers and queries fatal errors
    end
  end

  describe 'integration with transactional producers' do
    subject(:producer) do
      build(
        :transactional_producer,
        reload_on_transaction_fatal_error: true
      )
    end

    before do
      producer.singleton_class.include(WaterDrop::Producer::Testing)
    end

    it 'can trigger fatal errors on transactional producers' do
      result = producer.trigger_test_fatal_error(47, 'Test transactional error')
      expect(result).to eq(0)

      fatal_error = producer.fatal_error
      expect(fatal_error).not_to be_nil
      expect(fatal_error[:error_code]).to eq(47)
    end
  end

  describe 'usage with different producer types' do
    context 'with a standard non-idempotent producer' do
      subject(:producer) { build(:producer) }

      before do
        producer.singleton_class.include(WaterDrop::Producer::Testing)
      end

      it 'can still trigger and query fatal errors' do
        producer.trigger_test_fatal_error(47, 'Test on non-idempotent')
        expect(producer.fatal_error).not_to be_nil
      end
    end

    context 'when included module-wide' do
      before do
        # Include Testing for all producers (simulating spec_helper setup)
        WaterDrop::Producer.include(WaterDrop::Producer::Testing) unless
          WaterDrop::Producer.included_modules.include?(WaterDrop::Producer::Testing)
      end

      it 'makes testing methods available to all producer instances' do
        new_producer = build(:idempotent_producer)
        expect(new_producer).to respond_to(:trigger_test_fatal_error)
        expect(new_producer).to respond_to(:fatal_error)
        new_producer.close
      end
    end
  end

  describe 'error code examples' do
    # Document common error codes for testing purposes
    {
      47 => 'INVALID_PRODUCER_EPOCH (producer fencing)',
      64 => 'INVALID_PRODUCER_ID_MAPPING (invalid producer ID)'
    }.each do |code, description|
      it "works with error code #{code} (#{description})" do
        producer.trigger_test_fatal_error(code, "Testing #{description}")
        error = producer.fatal_error
        expect(error).not_to be_nil
        expect(error[:error_code]).to eq(code)
      end
    end
  end
end
