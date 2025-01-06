# frozen_string_literal: true

RSpec.describe_current do
  subject(:callback) { described_class.new(bearer, monitor) }

  let(:bearer) { instance_double(Rdkafka::Producer, name: 'test_bearer') }
  let(:monitor) { WaterDrop::Instrumentation::Monitor.new }
  let(:rd_config) { Rdkafka::Config.new }
  let(:bearer_name) { 'test_bearer' }

  describe '#call' do
    context 'when the bearer name matches' do
      before do
        allow(monitor).to receive(:instrument)
        callback.call(rd_config, bearer_name)
      end

      it 'instruments an oauthbearer.token_refresh event' do
        expect(monitor).to have_received(:instrument).with(
          'oauthbearer.token_refresh',
          bearer: bearer,
          caller: callback
        )
      end
    end

    context 'when the bearer name does not match' do
      let(:bearer_name) { 'different_bearer' }

      before do
        allow(monitor).to receive(:instrument)
        callback.call(rd_config, bearer_name)
      end

      it 'does not instrument any event' do
        expect(monitor).not_to have_received(:instrument)
      end
    end

    context 'when oauth bearer handler contains error' do
      let(:statistics) { { 'name' => client_name } }
      let(:tracked_errors) { [] }

      before do
        monitor.subscribe('oauthbearer.token_refresh') do
          raise
        end

        local_errors = tracked_errors

        monitor.subscribe('error.occurred') do |event|
          local_errors << event
        end
      end

      it 'expect to contain in, notify and continue as we do not want to crash rdkafka' do
        expect { callback.call(rd_config, bearer_name) }.not_to raise_error
        expect(tracked_errors.size).to eq(1)
        expect(tracked_errors.first[:type]).to eq('callbacks.oauthbearer_token_refresh.error')
      end
    end
  end
end
