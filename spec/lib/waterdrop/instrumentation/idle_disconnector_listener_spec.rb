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

  after { producer.close }

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

      context 'when producer is not disconnectable' do
        before do
          # Mock disconnectable? to return false (producer busy with transaction, operations, etc)
          allow(producer).to receive(:disconnectable?).and_return(false)
          allow(producer).to receive(:disconnect)
        end

        it 'expect not to attempt disconnect at all' do
          listener.on_statistics_emitted(event)
          expect(disconnected_events).to be_empty
          expect(producer).not_to have_received(:disconnect)
        end

        it 'expect to still reset activity time' do
          old_activity_time = listener.instance_variable_get(:@last_activity_time)
          listener.on_statistics_emitted(event)
          new_activity_time = listener.instance_variable_get(:@last_activity_time)

          expect(new_activity_time).to be > old_activity_time
        end
      end

      context 'when disconnect fails with an error in the thread' do
        let(:error_events) { [] }
        let(:test_error) { StandardError.new('disconnect failed') }

        before do
          producer.monitor.subscribe('error.occurred') do |event|
            error_events << event
          end

          # Producer is disconnectable but disconnect raises an error
          allow(producer).to receive(:disconnectable?).and_return(true)
          allow(producer).to receive(:disconnect).and_raise(test_error)
        end

        it 'expect to handle error gracefully and instrument it' do
          listener.on_statistics_emitted(event)
          sleep(0.1) # Give thread time to complete and handle error

          expect(disconnected_events).to be_empty
          expect(error_events).not_to be_empty
          expect(error_events.first[:type]).to eq('producer.disconnect.error')
          expect(error_events.first[:error]).to eq(test_error)
          expect(error_events.first[:producer_id]).to eq(producer.id)
        end

        it 'expect to still reset activity time even after error' do
          old_activity_time = listener.instance_variable_get(:@last_activity_time)
          listener.on_statistics_emitted(event)
          sleep(0.1) # Give thread time to complete
          new_activity_time = listener.instance_variable_get(:@last_activity_time)

          expect(new_activity_time).to be > old_activity_time
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
