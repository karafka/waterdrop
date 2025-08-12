# frozen_string_literal: true

RSpec.describe_current do
  subject(:listener) { described_class.new(producer, disconnect_timeout: timeout_ms) }

  let(:producer) { build(:producer) }
  let(:timeout_ms) { 1000 } # 1 second for fast tests
  let(:disconnected_events) { [] }

  before do
    producer.monitor.subscribe('producer.disconnected') do |event|
      disconnected_events << event
    end

    # Initialize producer connection
    producer.produce_sync(topic: 'test', payload: 'msg')
  end

  describe '#call' do
    let(:statistics) { { 'txmsgs' => 100 } }
    let(:event) { { statistics: statistics } }

    context 'when messages are being transmitted' do
      it 'expect not to disconnect producer' do
        listener.on_statistics_emitted(event)
        expect(disconnected_events).to be_empty
      end

      context 'with increasing message count' do
        it 'expect not to disconnect producer' do
          listener.on_statistics_emitted({ statistics: { 'txmsgs' => 50 } })
          listener.on_statistics_emitted({ statistics: { 'txmsgs' => 100 } })
          listener.on_statistics_emitted({ statistics: { 'txmsgs' => 150 } })

          expect(disconnected_events).to be_empty
        end
      end
    end

    context 'when no new messages are being transmitted' do
      let(:old_time) { listener.send(:monotonic_now) - timeout_ms - 100 }

      before do
        # Set up listener with old activity time to simulate timeout
        listener.instance_variable_set(:@last_activity_time, old_time)
        listener.instance_variable_set(:@last_txmsgs, 100)
      end

      context 'when producer can be disconnected' do
        it 'expect to disconnect the producer' do
          listener.on_statistics_emitted(event)
          sleep(0.1) # a bit of time because disconnect happens async
          expect(disconnected_events.size).to eq(1)
        end

        it 'expect to emit disconnect event with producer id' do
          listener.on_statistics_emitted(event)
          sleep(0.1) # a bit of time because disconnect happens async
          expect(disconnected_events.first[:producer_id]).to eq(producer.id)
        end

        it 'expect not to disconnect again on subsequent calls with same txmsgs' do
          listener.on_statistics_emitted(event)
          listener.on_statistics_emitted(event)

          sleep(0.1) # a bit of time because disconnect happens async

          expect(disconnected_events.size).to eq(1)
        end
      end

      context 'when producer cannot be disconnected' do
        before do
          # Simulate producer being busy (transaction, operations, etc)
          allow(producer).to receive(:disconnect).and_return(false)
        end

        it 'expect not to emit disconnect event' do
          listener.on_statistics_emitted(event)
          expect(disconnected_events).to be_empty
        end

        it 'expect to try again after timeout period' do
          # First attempt fails
          listener.on_statistics_emitted(event)
          expect(disconnected_events).to be_empty

          # Simulate time passing again
          listener.instance_variable_set(:@last_activity_time, old_time)

          # Allow disconnect to succeed this time
          allow(producer).to receive(:disconnect).and_call_original

          listener.on_statistics_emitted(event)

          sleep(0.1) # a bit of time because disconnect happens async

          expect(disconnected_events.size).to eq(1)
        end
      end
    end

    context 'when timeout has not been exceeded' do
      let(:recent_time) { listener.send(:monotonic_now) - (timeout_ms / 2) }

      before do
        listener.instance_variable_set(:@last_activity_time, recent_time)
        listener.instance_variable_set(:@last_txmsgs, 100)
      end

      it 'expect not to disconnect producer' do
        listener.on_statistics_emitted(event)
        expect(disconnected_events).to be_empty
      end
    end

    context 'when statistics contain no txmsgs field' do
      let(:statistics) { {} }

      it 'expect to handle missing txmsgs gracefully' do
        expect { listener.on_statistics_emitted(event) }.not_to raise_error
      end

      it 'expect to treat as 0 messages' do
        # Should detect activity change from -1 to 0
        listener.on_statistics_emitted(event)
        expect(disconnected_events).to be_empty
      end
    end
  end

  describe '#on_statistics_emitted' do
    let(:event) { { statistics: { 'txmsgs' => 150 } } }
    let(:call_args) { [] }

    before do
      allow(listener).to receive(:call) do |stats|
        call_args << stats
      end
    end

    it 'expect to delegate to call method with statistics' do
      listener.on_statistics_emitted(event)
      expect(call_args).to eq([{ 'txmsgs' => 150 }])
    end
  end

  describe 'integration with producer monitoring' do
    let(:events) { [] }

    before do
      producer.monitor.subscribe(listener)

      producer.monitor.subscribe('statistics.emitted') do |event|
        events << event
      end
    end

    it 'expect to respond to statistics events' do
      expect(listener).to respond_to(:on_statistics_emitted)
    end

    context 'with default timeout' do
      let(:long_timeout_listener) { described_class.new(producer) }

      it 'expect to use 5 minute default timeout' do
        # This is hard to test behaviorally, but we can verify it doesn't disconnect immediately
        long_timeout_listener.on_statistics_emitted({ statistics: { 'txmsgs' => 100 } })
        # Same count, no activity
        long_timeout_listener.on_statistics_emitted({ statistics: { 'txmsgs' => 100 } })

        expect(disconnected_events).to be_empty
      end
    end
  end
end
