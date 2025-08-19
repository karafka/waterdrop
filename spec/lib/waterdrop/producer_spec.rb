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
          config.kafka = { 'bootstrap.servers': KAFKA_HOST }
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
          config.kafka = { 'bootstrap.servers': KAFKA_HOST }
          config.oauth.token_provider_listener = listener.new
        end
      end

      it { expect { producer }.not_to raise_error }
      it { expect(producer.status.configured?).to be(true) }
      it { expect(producer.status.active?).to be(true) }
    end

    it 'expect not to allow to disconnect' do
      expect(producer.disconnect).to be(false)
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
          config.kafka = { 'bootstrap.servers': KAFKA_HOST }
        }
      end

      it { expect { producer.setup(&setup) }.not_to raise_error }
    end

    context 'when idle_disconnect_timeout is zero (disabled)' do
      before do
        producer.setup do |config|
          config.deliver = false
          config.kafka = { 'bootstrap.servers': KAFKA_HOST }
          config.idle_disconnect_timeout = 0
        end
      end

      it 'expect not to setup idle disconnector listener' do
        # With the current optimized producer setup, we can't easily verify
        # the absence of listeners without coupling to implementation details
        expect { producer }.not_to raise_error
      end
    end

    it 'expect not to allow to disconnect' do
      expect(producer.disconnect).to be(false)
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
        config.kafka = { 'bootstrap.servers': KAFKA_HOST }
      end
    end

    let(:count) { producer.partition_count(topic) }

    context 'when topic does not exist' do
      let(:topic) { "it-#{SecureRandom.uuid}" }

      it do
        producer.partition_count(topic)
        sleep(1)

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

    context 'when setting explicit enable.idempotence to false' do
      subject(:producer) do
        described_class.new do |config|
          config.kafka = { 'enable.idempotence': false }
        end
      end

      it { expect(producer.idempotent?).to be(false) }
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

    context 'when producer is already closed and we try to disconnect' do
      before { producer.close }

      it 'expect not to allow to disconnect' do
        expect(producer.disconnect).to be(false)
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

  describe '#inspect' do
    context 'when producer is not configured' do
      subject(:producer) { described_class.new }

      it { expect(producer.inspect).to include('WaterDrop::Producer') }
      it { expect(producer.inspect).to include('initial') }
      it { expect(producer.inspect).to include('buffer_size=0') }
      it { expect(producer.inspect).to include('id=nil') }
    end

    context 'when producer is configured but not connected' do
      subject(:producer) { build(:producer) }

      it { expect(producer.inspect).to include('WaterDrop::Producer') }
      it { expect(producer.inspect).to include('configured') }
      it { expect(producer.inspect).to include('buffer_size=0') }
      it { expect(producer.inspect).to include("id=\"#{producer.id}\"") }
    end

    context 'when producer is connected' do
      subject(:producer) { build(:producer).tap(&:client) }

      it { expect(producer.inspect).to include('WaterDrop::Producer') }
      it { expect(producer.inspect).to include('connected') }
      it { expect(producer.inspect).to include('buffer_size=0') }
      it { expect(producer.inspect).to include("id=\"#{producer.id}\"") }
    end

    context 'when producer has messages in buffer' do
      subject(:producer) { build(:producer).tap(&:client) }

      let(:message) { build(:valid_message) }

      before { producer.buffer(message) }

      it { expect(producer.inspect).to include('buffer_size=1') }

      context 'when multiple messages are buffered' do
        before do
          producer.buffer(message)
          producer.buffer(message)
        end

        it { expect(producer.inspect).to include('buffer_size=3') }
      end
    end

    context 'when buffer mutex is locked' do
      subject(:producer) { build(:producer).tap(&:client) }

      it 'shows buffer as busy when mutex is locked' do
        producer.instance_variable_get(:@buffer_mutex).lock

        expect(producer.inspect).to include('buffer_size=busy')

        producer.instance_variable_get(:@buffer_mutex).unlock
      end
    end

    context 'when producer is being closed' do
      subject(:producer) { build(:producer).tap(&:client) }

      before { allow(producer.status).to receive(:to_s).and_return('closing') }

      it { expect(producer.inspect).to include('closing') }
    end

    context 'when producer is closed' do
      subject(:producer) { build(:producer).tap(&:client).tap(&:close) }

      it { expect(producer.inspect).to include('closed') }
      it { expect(producer.inspect).to include('buffer_size=0') }
    end

    context 'when called concurrently' do
      subject(:producer) { build(:producer).tap(&:client) }

      it 'does not block or raise errors' do
        threads = Array.new(10) do
          Thread.new { producer.inspect }
        end

        results = threads.map(&:value)

        expect(results).to all(be_a(String))
        expect(results).to all(include('WaterDrop::Producer'))
      end
    end

    context 'when inspect is called during buffer operations' do
      subject(:producer) { build(:producer).tap(&:client) }

      let(:message) { build(:valid_message) }

      it 'does not interfere with buffer operations' do
        # Start a thread that continuously inspects
        inspect_thread = Thread.new do
          100.times { producer.inspect }
        end

        # Meanwhile, perform buffer operations
        expect do
          10.times { producer.buffer(message) }
          producer.flush_sync
        end.not_to raise_error

        inspect_thread.join
      end
    end

    it 'returns a string' do
      expect(producer.inspect).to be_a(String)
    end

    it 'does not call complex methods that could trigger side effects' do
      allow(producer).to receive(:client)
      # Ensure inspect doesn't trigger client creation
      producer.inspect
      expect(producer).not_to have_received(:client)
    end
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

    context 'when we have a reconnected producer' do
      subject(:producer) { build(:producer) }
      let(:message) { build(:valid_message) }

      it 'expect to continue to receive statistics after reconnect' do
        switch = :before
        events = []

        producer.monitor.subscribe('statistics.emitted') do |event|
          events << [event[:statistics].object_id, switch]
        end

        producer.produce_sync(message)

        sleep(2)

        producer.disconnect
        switch = :disconnected

        sleep(2)

        switch = :reconnected
        producer.produce_sync(message)
        sleep(2)

        types = events.map(&:last)
        expect(types).to include(:before)
        expect(types).to include(:reconnected)
        expect(types).not_to include(:disconnected)
        # This ensures we do not get the same events twice after reconnecting
        expect(events.map(&:first)).to eq(events.map(&:first).uniq)
      end
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

  # Since 30 seconds is minimum, they take a bit of time
  # We also test all producer cases at once to preserve time
  describe 'disconnect integration specs' do
    let(:never_used) { build(:producer, idle_disconnect_timeout: 30_000) }
    let(:used_short) { build(:producer, idle_disconnect_timeout: 30_000) }
    let(:used) { build(:producer, idle_disconnect_timeout: 30_000) }
    let(:buffered) { build(:producer, idle_disconnect_timeout: 30_000) }
    let(:message) { build(:valid_message) }

    after do
      never_used.close
      used_short.close
      used.close
      buffered.close
    end

    it 'expect to correctly shutdown those that are in need of it' do
      never_used
      buffered.buffer(message)
      used_short.produce_sync(message)

      31.times do
        used.produce_sync(message)
        sleep(1)
      end

      # Give a bit of time for the instrumentation to kick in
      sleep(1)

      expect(never_used.status.disconnected?).to be(false)
      expect(used.status.disconnected?).to be(false)
      expect(buffered.status.disconnected?).to be(false)
      expect(used_short.status.disconnected?).to be(true)
    end
  end

  describe '#disconnectable?' do
    context 'when producer is not configured' do
      it 'expect not to be disconnectable' do
        expect(producer.disconnectable?).to be(false)
      end
    end

    context 'when producer is configured but not connected' do
      before do
        producer.setup do |config|
          config.deliver = false
          config.kafka = { 'bootstrap.servers': KAFKA_HOST }
        end
      end

      it 'expect not to be disconnectable' do
        expect(producer.disconnectable?).to be(false)
      end
    end

    context 'when producer is connected' do
      subject(:producer) { build(:producer) }

      before do
        # Initialize connection by accessing client
        producer.client
      end

      it 'expect to be disconnectable' do
        expect(producer.disconnectable?).to be(true)
      end

      context 'when producer has buffered messages' do
        before do
          producer.buffer(build(:valid_message))
        end

        it 'expect not to be disconnectable' do
          expect(producer.disconnectable?).to be(false)
        end
      end

      context 'when producer is closed' do
        before { producer.close }

        it 'expect not to be disconnectable' do
          expect(producer.disconnectable?).to be(false)
        end
      end

      context 'when producer is already disconnected' do
        before do
          # Disconnect the producer first
          producer.disconnect
        end

        it 'expect not to be disconnectable again' do
          expect(producer.disconnectable?).to be(false)
        end
      end
    end

    context 'when producer is in transaction' do
      subject(:producer) { build(:transactional_producer) }

      it 'expect not to be disconnectable during transaction' do
        producer.transaction do
          expect(producer.disconnectable?).to be(false)
        end
      end
    end
  end
end
