# frozen_string_literal: true

RSpec.describe_current do
  subject(:producer) { described_class.new }

  let(:topic_name) { "it-#{SecureRandom.uuid}" }

  after { producer.close }

  describe '#initialize' do
    context 'when we initialize without a setup' do
      it { expect { producer }.not_to raise_error }
      it { expect(producer.status.active?).to be(false) }
    end

    context 'when initializing with setup' do
      subject(:producer) do
        described_class.new do |config|
          config.deliver = false
          config.kafka = { 'bootstrap.servers': 'localhost:9092' }
        end
      end

      it { expect { producer }.not_to raise_error }
      it { expect(producer.status.configured?).to be(true) }
      it { expect(producer.status.active?).to be(true) }
    end

    context 'when initializing with oauth listener' do
      let(:listener) do
        Class.new do
          def on_oauthbearer_token_refresh(_); end
        end
      end

      subject(:producer) do
        described_class.new do |config|
          config.kafka = { 'bootstrap.servers': 'localhost:9092' }
          config.oauth.token_provider_listener = listener.new
        end
      end

      it { expect { producer }.not_to raise_error }
      it { expect(producer.status.configured?).to be(true) }
      it { expect(producer.status.active?).to be(true) }
    end
  end

  describe '#setup' do
    context 'when producer has already been configured' do
      subject(:producer) { build(:producer) }

      let(:expected_error) { WaterDrop::Errors::ProducerAlreadyConfiguredError }

      it { expect { producer.setup {} }.to raise_error(expected_error) }
    end

    context 'when producer was not yet configured' do
      let(:setup) do
        lambda { |config|
          config.deliver = false
          config.kafka = { 'bootstrap.servers': 'localhost:9092' }
        }
      end

      it { expect { producer.setup(&setup) }.not_to raise_error }
    end
  end

  describe '#client' do
    subject(:client) { producer.client }

    context 'when producer is not configured' do
      let(:expected_error) { WaterDrop::Errors::ProducerNotConfiguredError }

      it 'expect not to allow to build client' do
        expect { client }.to raise_error expected_error
      end
    end

    context 'when client is already connected' do
      let(:producer) { build(:producer) }
      let(:client) { producer.client }

      before do
        client
        producer.client
      end

      after { client.close }

      context 'when called from a fork' do
        let(:expected_error) { WaterDrop::Errors::ProducerUsedInParentProcess }

        # Simulates fork by changing the pid
        before { allow(Process).to receive(:pid).and_return(-1) }

        it { expect { producer.client }.to raise_error(expected_error) }
      end

      context 'when called from the main process' do
        it { expect { client }.not_to raise_error }
      end
    end

    context 'when client is not initialized' do
      let(:producer) { build(:producer) }

      context 'when called from a fork' do
        before { allow(Process).to receive(:pid).and_return(-1) }

        it { expect { client }.not_to raise_error }
      end

      context 'when called from the main process' do
        it { expect { client }.not_to raise_error }
      end
    end
  end

  describe '#partition_count' do
    subject(:producer) do
      described_class.new do |config|
        config.kafka = { 'bootstrap.servers': 'localhost:9092' }
      end
    end

    let(:count) { producer.partition_count(topic) }

    context 'when topic does not exist' do
      let(:topic) { "it-#{SecureRandom.uuid}" }

      # Older versions of librdkafka (pre 2.10) would raise error, after that it may not
      # topic may be created if fast enough or error may happen
      it do
        expect(count).to eq(1)
      rescue Rdkafka::RdkafkaError => e
        expect(e).to be_a(Rdkafka::RdkafkaError)
        expect(e.code).to eq(:unknown_topic_or_part)
      end
    end

    context 'when topic exists' do
      let(:topic) { "it-#{SecureRandom.uuid}" }

      before { producer.produce_sync(topic: topic, payload: '') }

      it { expect(count).to eq(1) }
    end
  end

  describe '#idempotent?' do
    context 'when producer is transactional' do
      subject(:producer) { build(:transactional_producer) }

      it { expect(producer.idempotent?).to be(true) }
    end

    context 'when it is a regular producer' do
      subject(:producer) { build(:producer) }

      it { expect(producer.idempotent?).to be(false) }
    end

    context 'when it is an idempotent producer' do
      subject(:producer) { build(:idempotent_producer) }

      it { expect(producer.idempotent?).to be(true) }
    end
  end

  describe '#close' do
    before { allow(producer).to receive(:client).and_call_original }

    context 'when producer is already closed' do
      subject(:producer) { build(:producer).tap(&:client) }

      before { producer.close }

      it { expect { producer.close }.not_to raise_error }
      it { expect(producer.tap(&:close).status.closed?).to be(true) }
    end

    context 'when producer was not yet closed' do
      subject(:producer) { build(:producer).tap(&:client) }

      it { expect { producer.close }.not_to raise_error }
      it { expect(producer.tap(&:close).status.closed?).to be(true) }
    end

    context 'when there were messages in the buffer' do
      subject(:producer) { build(:producer).tap(&:client) }

      let(:client) { producer.client }

      before do
        producer.buffer(build(:valid_message))
        allow(client).to receive(:close).and_call_original
      end

      it { expect { producer.close }.to change { producer.messages.size }.from(1).to(0) }

      it 'expect to close client since was open' do
        producer.close
        expect(client).to have_received(:close)
      end
    end

    context 'when producer was configured but not connected' do
      subject(:producer) { build(:producer) }

      it { expect(producer.status.configured?).to be(true) }
      it { expect { producer.close }.not_to raise_error }
      it { expect(producer.tap(&:close).status.closed?).to be(true) }

      it 'expect not to close client since was not open' do
        producer.close
        expect(producer).not_to have_received(:client)
      end
    end

    context 'when producer was configured and connected' do
      subject(:producer) { build(:producer).tap(&:client) }

      let(:client) { producer.client }

      before { allow(client).to receive(:close).and_call_original }

      it { expect(producer.status.connected?).to be(true) }
      it { expect { producer.close }.not_to raise_error }
      it { expect(producer.tap(&:close).status.closed?).to be(true) }

      it 'expect to close client since was open' do
        producer.close
        expect(client).to have_received(:close)
      end
    end

    context 'when flush timeouts' do
      subject(:producer) { build(:producer).tap(&:client) }

      let(:message) { build(:valid_message) }

      # This will be reached when we dispatch a lot of messages
      before do
        # Simulate flush failure
        allow(producer.client).to receive(:flush).and_raise(::Rdkafka::RdkafkaError.new(1))
      end

      it 'expect to close and not to raise any errors' do
        expect { producer.close }.not_to raise_error
      end
    end
  end

  describe '#close!' do
    context 'when producer cannot connect and dispatch messages' do
      subject(:producer) do
        build(
          :producer,
          kafka: { 'bootstrap.servers': 'localhost:9093' },
          max_wait_timeout: 1
        )
      end

      it 'expect not to hang forever' do
        producer.produce_async(topic: 'na', payload: 'data')
        producer.close!
      end
    end
  end

  describe '#purge' do
    context 'when there are some outgoing messages and we purge' do
      subject(:producer) do
        build(
          :producer,
          kafka: { 'bootstrap.servers': 'localhost:9093' },
          max_wait_timeout: 1
        )
      end

      after { producer.close! }

      it 'expect their deliveries to materialize with errors' do
        handler = producer.produce_async(topic: 'na', payload: 'data')
        producer.purge
        expect { handler.wait }.to raise_error(Rdkafka::RdkafkaError, /Purged in queue/)
      end

      context 'when monitoring errors via instrumentation' do
        let(:detected) { [] }

        before do
          producer.monitor.subscribe('error.occurred') do |event|
            next unless event[:type] == 'librdkafka.dispatch_error'

            detected << event
          end
        end

        it 'expect the error notifications to publish those errors' do
          handler = producer.produce_async(topic: topic_name, payload: 'data', label: 'test')
          producer.purge

          handler.wait(raise_response_error: false)
          expect(detected.first[:error].code).to eq(:purge_queue)
          expect(detected.first[:label]).to eq('test')
        end
      end
    end
  end

  describe '#ensure_usable!' do
    subject(:producer) { build(:producer) }

    context 'when status is invalid' do
      let(:expected_error) { WaterDrop::Errors::StatusInvalidError }

      before do
        allow(producer.status).to receive_messages(
          configured?: false,
          connected?: false,
          initial?: false,
          closing?: false,
          closed?: false
        )
      end

      it { expect { producer.send(:ensure_active!) }.to raise_error(expected_error) }
    end
  end

  describe '#tags' do
    let(:producer1) { build(:producer) }
    let(:producer2) { build(:producer) }

    before do
      producer1.tags.add(:type, 'transactional')
      producer2.tags.add(:type, 'regular')
    end

    it { expect(producer1.tags.to_a).to eq(%w[transactional]) }
    it { expect(producer2.tags.to_a).to eq(%w[regular]) }
  end

  describe 'statistics callback hook' do
    let(:message) { build(:valid_message) }

    context 'when stats are emitted' do
      subject(:producer) { build(:producer) }

      let(:events) { [] }

      before do
        producer.monitor.subscribe('statistics.emitted') do |event|
          events << event
        end

        producer.produce_sync(message)

        sleep(0.001) while events.size < 3
      end

      it { expect(events.last.id).to eq('statistics.emitted') }
      it { expect(events.last[:producer_id]).to eq(producer.id) }
      it { expect(events.last[:statistics]['ts']).to be > 0 }
      # This is in microseconds. We needed a stable value for comparison, and the distance in
      # between statistics events should always be within 1ms
      it { expect(events.last[:statistics]['ts_d']).to be_between(90_000, 200_000) }
    end

    context 'when we have more producers' do
      let(:producer1) { build(:producer) }
      let(:producer2) { build(:producer) }
      let(:events1) { [] }
      let(:events2) { [] }

      before do
        producer1.monitor.subscribe('statistics.emitted') do |event|
          events1 << event
        end

        producer2.monitor.subscribe('statistics.emitted') do |event|
          events2 << event
        end

        producer1.produce_sync(message)
        producer2.produce_sync(message)

        # Wait for the error to occur
        sleep(0.001) while events1.size < 2
        sleep(0.001) while events2.size < 2
      end

      after do
        producer1.close
        producer2.close
      end

      it 'expect not to have same statistics from both producers' do
        ids1 = events1.map(&:payload).map { |payload| payload[:statistics] }.map(&:object_id)
        ids2 = events2.map(&:payload).map { |payload| payload[:statistics] }.map(&:object_id)

        expect(ids1 & ids2).to be_empty
      end
    end
  end

  describe 'error callback hook' do
    let(:message) { build(:valid_message) }

    context 'when error occurs' do
      subject(:producer) { build(:producer, kafka: { 'bootstrap.servers': 'localhost:9090' }) }

      let(:events) { [] }

      before do
        producer.monitor.subscribe('error.occurred') do |event|
          events << event
        end

        # Forceful creation of a client will trigger a connection attempt
        producer.client

        # Wait for the error to occur
        sleep(0.001) while events.empty?
      end

      it 'expect to emit proper stats' do
        expect(events.first.id).to eq('error.occurred')
        expect(events.first[:producer_id]).to eq(producer.id)
        expect(events.first[:error]).to be_a(Rdkafka::RdkafkaError)
      end
    end

    context 'when we have more producers' do
      let(:producer1) { build(:producer, kafka: { 'bootstrap.servers': 'localhost:9090' }) }
      let(:producer2) { build(:producer, kafka: { 'bootstrap.servers': 'localhost:9090' }) }
      let(:events1) { [] }
      let(:events2) { [] }

      before do
        producer1.monitor.subscribe('error.occurred') do |event|
          events1 << event
        end

        producer2.monitor.subscribe('error.occurred') do |event|
          events2 << event
        end

        # Forceful creation of a client will trigger a connection attempt
        producer1.client
        producer2.client

        # Wait for the error to occur
        sleep(0.001) while events1.empty?
        sleep(0.001) while events2.empty?
      end

      after do
        producer1.close
        producer2.close
      end

      it 'expect not to have same errors from both producers' do
        ids1 = events1.map(&:payload).map { |payload| payload[:error] }.map(&:object_id)
        ids2 = events2.map(&:payload).map { |payload| payload[:error] }.map(&:object_id)

        expect(ids1 & ids2).to be_empty
      end
    end
  end

  describe 'producer with enable.idempotence true' do
    subject(:producer) { build(:idempotent_producer) }

    let(:message) { build(:valid_message) }

    it 'expect to work as any producer without any exceptions' do
      expect { producer.produce_sync(message) }.not_to raise_error
    end
  end

  describe 'fork integration spec' do
    subject(:producer) { build(:producer) }

    let(:child_process) do
      fork do
        producer.produce_sync(topic: topic_name, payload: '1')
        producer.close
      end
    end

    context 'when producer not in use' do
      it 'expect to work correctly' do
        child_process
        Process.wait
        expect($CHILD_STATUS.to_i).to eq(0)
      end
    end
  end
end
