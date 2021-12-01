# frozen_string_literal: true

RSpec.describe_current do
  subject(:producer) { described_class.new }

  after { producer.close }

  describe '#initialize' do
    context 'when we initialize without a setup' do
      it { expect { producer }.not_to raise_error }
      it { expect(producer.status.active?).to eq(false) }
    end

    context 'when initializing with setup' do
      subject(:producer) do
        described_class.new do |config|
          config.deliver = false
          config.kafka = { 'bootstrap.servers' => 'localhost:9092' }
        end
      end

      it { expect { producer }.not_to raise_error }
      it { expect(producer.status.configured?).to eq(true) }
      it { expect(producer.status.active?).to eq(true) }
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
          config.kafka = { 'bootstrap.servers' => 'localhost:9092' }
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

      before { producer.client }

      context 'when called from a fork' do
        let(:expected_error) { WaterDrop::Errors::ProducerUsedInParentProcess }

        # Simulates fork by changing the pid
        before { allow(Process).to receive(:pid).and_return(-1) }

        it { expect { client }.to raise_error(expected_error) }
      end

      context 'when called from the main process' do
        it { expect { client }.not_to raise_error }
      end
    end

    context 'when client is not connected' do
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

  describe '#close' do
    before { allow(producer).to receive(:client).and_call_original }

    context 'when producer is already closed' do
      subject(:producer) { build(:producer).tap(&:client) }

      before { producer.close }

      it { expect { producer.close }.not_to raise_error }
      it { expect(producer.tap(&:close).status.closed?).to eq(true) }
    end

    context 'when producer was not yet closed' do
      subject(:producer) { build(:producer).tap(&:client) }

      it { expect { producer.close }.not_to raise_error }
      it { expect(producer.tap(&:close).status.closed?).to eq(true) }
    end

    context 'when there were messages in the buffer' do
      subject(:producer) { build(:producer).tap(&:client) }

      before do
        producer.buffer(build(:valid_message))
        allow(producer.client).to receive(:close).and_call_original
      end

      it { expect { producer.close }.to change { producer.messages.size }.from(1).to(0) }

      it 'expect to close client since was open' do
        producer.close
        expect(producer.client).to have_received(:close)
      end
    end

    context 'when producer was configured but not connected' do
      subject(:producer) { build(:producer) }

      it { expect(producer.status.configured?).to eq(true) }
      it { expect { producer.close }.not_to raise_error }
      it { expect(producer.tap(&:close).status.closed?).to eq(true) }

      it 'expect not to close client since was not open' do
        producer.close
        expect(producer).not_to have_received(:client)
      end
    end

    context 'when producer was configured and connected' do
      subject(:producer) { build(:producer).tap(&:client) }

      before { allow(producer.client).to receive(:close).and_call_original }

      it { expect(producer.status.connected?).to eq(true) }
      it { expect { producer.close }.not_to raise_error }
      it { expect(producer.tap(&:close).status.closed?).to eq(true) }

      it 'expect to close client since was open' do
        producer.close
        expect(producer.client).to have_received(:close)
      end
    end
  end

  describe '#ensure_usable!' do
    subject(:producer) { create(:producer) }

    context 'when status is invalid' do
      let(:expected_error) { WaterDrop::Errors::StatusInvalidError }

      before do
        allow(producer.status).to receive(:configured?).and_return(false)
        allow(producer.status).to receive(:connected?).and_return(false)
        allow(producer.status).to receive(:initial?).and_return(false)
        allow(producer.status).to receive(:closing?).and_return(false)
        allow(producer.status).to receive(:closed?).and_return(false)
      end

      it { expect { producer.send(:ensure_active!) }.to raise_error(expected_error) }
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
      subject(:producer) { build(:producer, kafka: { 'bootstrap.servers' => 'localhost:9090' }) }

      let(:events) { [] }

      before do
        producer.monitor.subscribe('error.emitted') do |event|
          events << event
        end

        # Forceful creation of a client will trigger a connection attempt
        producer.client

        # Wait for the error to occur
        sleep(0.001) while events.empty?
      end

      it 'expect to emit proper stats' do
        expect(events.first.id).to eq('error.emitted')
        expect(events.first[:producer_id]).to eq(producer.id)
        expect(events.first[:error]).to be_a(Rdkafka::RdkafkaError)
      end
    end

    context 'when we have more producers' do
      let(:producer1) { build(:producer, kafka: { 'bootstrap.servers' => 'localhost:9090' }) }
      let(:producer2) { build(:producer, kafka: { 'bootstrap.servers' => 'localhost:9090' }) }
      let(:events1) { [] }
      let(:events2) { [] }

      before do
        producer1.monitor.subscribe('error.emitted') do |event|
          events1 << event
        end

        producer2.monitor.subscribe('error.emitted') do |event|
          events2 << event
        end

        # Forceful creation of a client will trigger a connection attempt
        producer1.client
        producer2.client

        # Wait for the error to occur
        sleep(0.001) while events1.empty?
        sleep(0.001) while events2.empty?
      end

      it 'expect not to have same errors from both producers' do
        ids1 = events1.map(&:payload).map { |payload| payload[:error] }.map(&:object_id)
        ids2 = events2.map(&:payload).map { |payload| payload[:error] }.map(&:object_id)

        expect(ids1 & ids2).to be_empty
      end
    end
  end
end
