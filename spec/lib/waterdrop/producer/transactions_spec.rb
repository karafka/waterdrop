# frozen_string_literal: true

RSpec.describe_current do
  subject(:producer) { build(:transactional_producer) }

  let(:transactional_id) { SecureRandom.uuid }
  let(:critical_error) { Exception }

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

  context 'when we try to create producer with invalid acks' do
    it 'expect to raise an error' do
      expect do
        build(:transactional_producer, request_required_acks: 1).client
      end.to raise_error(Rdkafka::Config::ClientCreationError, /acks/)
    end
  end

  context 'when we try to start transaction without transactional.id' do
    subject(:producer) { build(:producer) }

    it 'expect to raise with info that this functionality is not configured' do
      expect { producer.transaction {} }
        .to raise_error(::Rdkafka::RdkafkaError, /Local: Functionality not configured/)
    end

    it { expect(producer.transactional?).to eq(false) }
    it { expect(producer.transaction?).to eq(false) }
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

    it 'expect to crash with an inconsistent or a timeout state after abort' do
      error = nil

      begin
        producer.transaction do
          20.times do |i|
            producer.produce_async(topic: SecureRandom.uuid, payload: i.to_s)
          end
        end
      rescue Rdkafka::RdkafkaError => e
        error = e
      end

      # This spec is not fully stable due to how librdkafka works
      if error
        expect(error).to be_a(Rdkafka::RdkafkaError)
        expect(error.code).to eq(:state)
        expect(error.cause).to be_a(Rdkafka::RdkafkaError)
        expect(error.cause.code).to eq(:inconsistent).or eq(:timed_out)
      end
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
      let(:purges) { [] }

      before do
        producer.monitor.subscribe('error.occurred') do |event|
          errors << event[:error]
        end

        producer.monitor.subscribe('message.purged') do |event|
          purges << event[:error]
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

      it 'expect not to emit the cancellation error via the error pipeline' do
        expect(errors).to be_empty
      end

      it 'expect to emit the cancellation error via the message.purged' do
        expect(purges.first).to be_a(Rdkafka::RdkafkaError)
        expect(purges.first.code).to eq(:purge_queue)
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
        expect(result.partition).to eq(-1).or eq(0)
        expect(result.offset).to eq(-1_001)
        expect(result.error).to be_a(Rdkafka::RdkafkaError)
      end
    end
  end

  context 'when we start transaction and raise a critical Exception' do
    it 'expect to re-raise this error' do
      expect do
        producer.transaction do
          producer.produce_async(topic: 'example_topic', payload: 'na')

          raise critical_error
        end
      end.to raise_error(critical_error)
    end

    it 'expect to cancel the dispatch of the message' do
      handler = nil

      begin
        producer.transaction do
          handler = producer.produce_async(topic: 'example_topic', payload: 'na')

          raise critical_error
        end
      rescue critical_error
        nil
      end

      expect { handler.wait }.to raise_error(Rdkafka::RdkafkaError, /Purged in queue/)
    end

    # The rest is expected to behave the same way as StandardError so not duplicating
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
      let(:purges) { [] }

      before do
        producer.monitor.subscribe('error.occurred') do |event|
          errors << event[:error]
        end

        producer.monitor.subscribe('message.purged') do |event|
          purges << event[:error]
        end

        producer.transaction do
          producer.produce_async(topic: 'example_topic', payload: 'na')

          throw(:abort)
        end
      end

      it 'expect not to emit the cancellation error via the error pipeline' do
        expect(errors).to be_empty
      end

      it 'expect to emit the cancellation error via the message.purged' do
        expect(purges.first).to be_a(Rdkafka::RdkafkaError)
        expect(purges.first.code).to eq(:purge_queue)
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
        # It can be either -1 or 0 if topic was created fast enough for report to become aware
        # of it
        expect(result.partition).to eq(-1).or eq(0)
        expect(result.offset).to eq(-1_001)
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

  context 'when trying to close a producer from a different thread during transaction' do
    it 'expect to raise an error' do
      expect do
        producer.transaction do
          Thread.new { producer.close }
          sleep(1)
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

  context 'when we use transactional producer without transaction' do
    it 'expect to allow as it will wrap with a transaction' do
      expect do
        producer.produce_sync(topic: 'example_topic', payload: rand.to_s)
      end.not_to raise_error
    end

    it 'expect to deliver message correctly' do
      result = producer.produce_sync(topic: 'example_topic', payload: rand.to_s)
      expect(result.topic_name).to eq('example_topic')
      expect(result.error).to eq(nil)
    end

    it 'expect to use the async dispatch though with transaction wrapper' do
      handler = producer.produce_async(topic: 'example_topic', payload: rand.to_s)
      result = handler.wait
      expect(result.topic_name).to eq('example_topic')
      expect(result.error).to eq(nil)
    end

    context 'when using with produce_many_sync' do
      let(:messages) { Array.new(10) { build(:valid_message) } }
      let(:counts) { [] }

      before do
        local_counts = counts
        producer.monitor.subscribe('transaction.committed') { local_counts << true }
      end

      it 'expect to wrap it with a single transaction' do
        producer.produce_many_sync(messages)
        expect(counts.size).to eq(1)
      end
    end

    context 'when using with produce_many_async' do
      let(:messages) { Array.new(10) { build(:valid_message) } }
      let(:counts) { [] }

      before do
        local_counts = counts
        producer.monitor.subscribe('transaction.committed') { local_counts << true }
      end

      it 'expect to wrap it with a single transaction' do
        producer.produce_many_async(messages)
        expect(counts.size).to eq(1)
      end
    end
  end

  context 'when nesting transaction inside of a transaction' do
    let(:counts) { [] }

    before do
      local_counts = counts
      producer.monitor.subscribe('transaction.committed') { local_counts << true }
    end

    it 'expect to work' do
      handlers = []

      producer.transaction do
        handlers << producer.produce_async(topic: 'example_topic', payload: 'data')

        producer.transaction do
          handlers << producer.produce_async(topic: 'example_topic', payload: 'data')
        end
      end

      handlers.each { |handler| expect { handler.wait }.not_to raise_error }
    end

    it 'expect to have one actual transaction' do
      producer.transaction do
        producer.produce_async(topic: 'example_topic', payload: 'data')

        producer.transaction do
          producer.produce_async(topic: 'example_topic', payload: 'data')
        end
      end

      expect(counts.size).to eq(1)
    end

    context 'when we abort the nested transaction' do
      it 'expect to abort all levels' do
        handlers = []

        producer.transaction do
          handlers << producer.produce_async(topic: 'example_topic', payload: 'data')

          producer.transaction do
            handlers << producer.produce_async(topic: 'example_topic', payload: 'data')
            throw(:abort)
          end
        end

        handlers.each do |handler|
          expect { handler.wait }.to raise_error(Rdkafka::RdkafkaError, /Purged in queue/)
        end
      end
    end
  end

  context 'when trying to mark as consumed in a transaction' do
    let(:message) { OpenStruct.new(topic: rand.to_s, partition: 0, offset: 100) }

    context 'when we try mark as consumed without a transaction' do
      it 'expect to raise an error' do
        expect { producer.transaction_mark_as_consumed(nil, message) }
          .to raise_error(WaterDrop::Errors::TransactionRequiredError)
      end
    end

    context 'when we try mark as consumed with invalid arguments' do
      let(:consumer) { OpenStruct.new }

      before { allow(producer.client).to receive(:send_offsets_to_transaction) }

      it 'expect to delegate to client send_offsets_to_transaction with correct timeout' do
        producer.transaction do
          expect { producer.transaction_mark_as_consumed(consumer, message) }
            .to raise_error(WaterDrop::Errors::TransactionalOffsetInvalidError)
        end
      end
    end

    # Full e2e integration of this is checked in Karafka as we do not operate on consumers here
    context 'when trying mark as consumed inside a transaction' do
      let(:consumer) { OpenStruct.new(consumer_group_metadata_pointer: 1) }

      before do
        allow(producer.client).to receive(:send_offsets_to_transaction)

        producer.transaction do
          producer.transaction_mark_as_consumed(consumer, message)
        end
      end

      it 'expect to delegate to client send_offsets_to_transaction with correct timeout' do
        expect(producer.client)
          .to have_received(:send_offsets_to_transaction)
          .with(consumer, any_args, 30_000)
      end
    end
  end

  context 'when creating transactional producer with default config' do
    let(:producer) do
      WaterDrop::Producer.new do |config|
        config.deliver = true
        config.kafka = {
          'bootstrap.servers': 'localhost:9092',
          'request.required.acks': 1,
          'transactional.id': SecureRandom.uuid,
          acks: 'all'
        }
      end
    end

    it 'expect to be able to do so and to send a message' do
      expect { producer.produce_async(topic: 'test', payload: 'a') }
        .not_to raise_error
    end
  end

  context 'when we are not inside a running transaction' do
    it { expect(producer.transaction?).to eq(false) }
  end

  context 'when we are inside a transaction' do
    it 'expect to be recognize it and be true' do
      producer.transaction do
        expect(producer.transaction?).to eq(true)
      end
    end
  end

  context 'when we are inside a transaction and early break' do
    it 'expect not to corrupt the state of the producer' do
      10.times do
        producer.transaction { break }
        producer.transaction {}
      end
    end

    it 'expect to return nil' do
      result = producer.transaction { break(10) }
      expect(result).to eq(nil)
    end
  end

  context 'when producer gets a critical broker errors with reload on' do
    let(:topic) { SecureRandom.uuid }

    let(:producer) do
      WaterDrop::Producer.new do |config|
        config.max_payload_size = 1_000_000_000_000
        config.kafka = {
          'bootstrap.servers': 'localhost:9092',
          'transactional.id': SecureRandom.uuid,
          'max.in.flight': 5
        }
      end
    end

    before do
      admin = Rdkafka::Config.new('bootstrap.servers': 'localhost:9092').admin
      admin.create_topic(topic, 1, 1, 'max.message.bytes': 128).wait
      admin.close
    end

    it 'expect to be able to use same producer after the error when async' do
      errored = false

      begin
        producer.produce_async(topic: topic, payload: '1' * 512)
      rescue WaterDrop::Errors::ProduceError
        errored = true
      end

      expect(errored).to eq(true)

      producer.produce_async(topic: topic, payload: '1')
    end

    it 'expect to be able to use same producer after the error when sync' do
      errored = false

      begin
        producer.produce_sync(topic: topic, payload: '1' * 512)
      rescue WaterDrop::Errors::ProduceError
        errored = true
      end

      expect(errored).to eq(true)

      producer.produce_sync(topic: topic, payload: '1')
    end
  end
end
