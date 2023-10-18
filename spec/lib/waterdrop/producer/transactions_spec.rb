# frozen_string_literal: true

RSpec.describe_current do
  subject(:producer) { build(:transactional_producer) }

  let(:transactional_id) { SecureRandom.uuid }

  after { producer.close }

  it do
    # First run will check if cached
    producer.transactional?
    expect(producer.transactional?).to eq(true)
  end

  context 'when we try to create producer with invalid transactional settings' do
    it 'expect to raise an error' do
      expect do
        build(:transactional_producer, transaction_timeout_ms: 100).client
      end.to raise_error(Rdkafka::Config::ConfigError, /transaction\.timeout\.ms/)
    end
  end

  context 'when we try to start transaction without transactional.id' do
    subject(:producer) { build(:producer) }

    it 'expect to raise with info that this functionality is not configured' do
      expect { producer.transaction {} }
        .to raise_error(::Rdkafka::RdkafkaError, /Local: Functionality not configured/)
    end

    it { expect(producer.transactional?).to eq(false) }
  end

  context 'when we make a transaction without sending any messages' do
    it 'expect not to crash and do nothing' do
      expect { producer.transaction {} }.not_to raise_error
    end
  end

  context 'when we dispatch in transaction to multiple topics' do
    it 'expect to work' do
      handlers = []

      producer.transaction do
        handlers << producer.produce_async(topic: 'example_topic1', payload: '1')
        handlers << producer.produce_async(topic: 'example_topic2', payload: '2')
      end

      expect { handlers.map!(&:wait) }.not_to raise_error
    end

    it 'expect to return block result as the transaction result' do
      result = rand

      transaction_result = producer.transaction do
        producer.produce_async(topic: 'example_topic', payload: '2')
        result
      end

      expect(transaction_result).to eq(result)
    end
  end

  context 'when trying to use transaction on a non-existing topics and short time' do
    subject(:producer) { build(:transactional_producer, transaction_timeout_ms: 1_000) }

    it 'expect to crash with an inconsistent state after abort' do
      error = nil

      begin
        producer.transaction do
          10.times do |i|
            producer.produce_async(topic: SecureRandom.uuid, payload: i.to_s)
          end
        end
      rescue Rdkafka::RdkafkaError => e
        error = e
      end

      expect(error).to be_a(Rdkafka::RdkafkaError)
      expect(error.code).to eq(:state)
      expect(error.cause).to be_a(Rdkafka::RdkafkaError)
      expect(error.cause.code).to eq(:inconsistent)
    end
  end

  context 'when trying to use transaction on a non-existing topics and enough time' do
    subject(:producer) { build(:transactional_producer) }

    it 'expect not to crash and publish all data' do
      expect do
        producer.transaction do
          10.times do |i|
            producer.produce_async(topic: SecureRandom.uuid, payload: i.to_s)
          end
        end
      end.not_to raise_error
    end
  end

  context 'when we start transaction and raise an error' do
    it 'expect to re-raise this error' do
      expect do
        producer.transaction do
          producer.produce_async(topic: 'example_topic', payload: 'na')

          raise StandardError
        end
      end.to raise_error(StandardError)
    end

    it 'expect to cancel the dispatch of the message' do
      handler = nil

      begin
        producer.transaction do
          handler = producer.produce_async(topic: 'example_topic', payload: 'na')

          raise StandardError
        end
      rescue StandardError
        nil
      end

      expect { handler.wait }.to raise_error(Rdkafka::RdkafkaError, /Purged in queue/)
    end

    context 'when we have error instrumentation' do
      let(:errors) { [] }

      before do
        producer.monitor.subscribe('error.occurred') do |event|
          errors << event[:error]
        end

        begin
          producer.transaction do
            producer.produce_async(topic: 'example_topic', payload: 'na')

            raise StandardError
          end
        rescue StandardError
          nil
        end
      end

      it 'expect to emit the cancellation error via the error pipeline' do
        expect(errors.first).to be_a(Rdkafka::RdkafkaError)
        expect(errors.first.code).to eq(:purge_queue)
      end
    end

    context 'when using sync producer' do
      it 'expect to wait on the initial delivery per message and have it internally' do
        result = nil

        begin
          producer.transaction do
            result = producer.produce_sync(topic: 'example_topic', payload: 'na')

            expect(result.partition).to eq(0)
            expect(result.error).to eq(nil)

            raise StandardError
          end
        rescue StandardError
          nil
        end

        # It will be compacted but is still visible as a delivery report
        expect(result.partition).to eq(0)
        expect(result.error).to eq(nil)
      end
    end

    context 'when using async producer and waiting' do
      it 'expect to wait on the initial delivery per message' do
        handler = nil

        begin
          producer.transaction do
            handler = producer.produce_async(topic: 'example_topic', payload: 'na')

            raise StandardError
          end
        rescue StandardError
          nil
        end

        result = handler.create_result

        # It will be compacted but is still visible as a delivery report
        expect(result.partition).to eq(-1)
        expect(result.error).to be_a(Rdkafka::RdkafkaError)
      end
    end
  end

  context 'when we start transaction and abort' do
    it 'expect not to re-raise' do
      expect do
        producer.transaction do
          producer.produce_async(topic: 'example_topic', payload: 'na')

          throw(:abort)
        end
      end.not_to raise_error
    end

    it 'expect to cancel the dispatch of the message' do
      handler = nil

      producer.transaction do
        handler = producer.produce_async(topic: 'example_topic', payload: 'na')

        raise WaterDrop::Errors::AbortTransaction
      end

      expect { handler.wait }.to raise_error(Rdkafka::RdkafkaError, /Purged in queue/)
    end

    context 'when we have error instrumentation' do
      let(:errors) { [] }

      before do
        producer.monitor.subscribe('error.occurred') do |event|
          errors << event[:error]
        end

        producer.transaction do
          producer.produce_async(topic: 'example_topic', payload: 'na')

          throw(:abort)
        end
      end

      it 'expect to emit the cancellation error via the error pipeline' do
        expect(errors.first).to be_a(Rdkafka::RdkafkaError)
        expect(errors.first.code).to eq(:purge_queue)
      end
    end

    context 'when using sync producer' do
      it 'expect to wait on the initial delivery per message and have it internally' do
        result = nil

        producer.transaction do
          result = producer.produce_sync(topic: 'example_topic', payload: 'na')

          expect(result.partition).to eq(0)
          expect(result.error).to eq(nil)

          throw(:abort)
        end

        # It will be compacted but is still visible as a delivery report
        expect(result.partition).to eq(0)
        expect(result.error).to eq(nil)
      end
    end

    context 'when using async producer and waiting' do
      it 'expect to wait on the initial delivery per message' do
        handler = nil

        producer.transaction do
          handler = producer.produce_async(topic: 'example_topic', payload: 'na')

          throw(:abort)
        end

        result = handler.create_result

        # It will be compacted but is still visible as a delivery report
        expect(result.partition).to eq(-1)
        expect(result.error).to be_a(Rdkafka::RdkafkaError)
      end
    end
  end

  context 'when we try to create a producer with already used transactional_id' do
    let(:producer1) { build(:transactional_producer, transactional_id: transactional_id) }
    let(:producer2) { build(:transactional_producer, transactional_id: transactional_id) }

    after do
      producer1.close
      producer2.close
    end

    it 'expect to fence out the previous one' do
      producer1.transaction {}
      producer2.transaction {}

      expect do
        producer1.transaction do
          producer1.produce_async(topic: 'example_topic', payload: '1')
        end
      end.to raise_error(Rdkafka::RdkafkaError, /fenced by a newer instance/)
    end

    it 'expect not to fence out the new one' do
      producer1.transaction {}
      producer2.transaction {}

      expect do
        producer2.transaction do
          producer2.produce_async(topic: 'example_topic', payload: '1')
        end
      end.not_to raise_error
    end
  end

  context 'when trying to close a producer from inside of a transaction' do
    it 'expect to raise an error' do
      expect do
        producer.transaction do
          producer.close
        end
      end.to raise_error(Rdkafka::RdkafkaError, /Erroneous state/)
    end
  end

  context 'when transaction crashes internally on one of the retryable operations' do
    before do
      counter = 0
      ref = producer.client.method(:begin_transaction)

      allow(producer.client).to receive(:begin_transaction) do
        if counter.zero?
          counter += 1

          raise(Rdkafka::RdkafkaError.new(-152, retryable: true))
        end

        ref.call
      end
    end

    it 'expect to retry and continue' do
      expect { producer.transaction {} }.not_to raise_error
    end
  end
end
