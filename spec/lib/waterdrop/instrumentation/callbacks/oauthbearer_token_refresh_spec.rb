# frozen_string_literal: true

RSpec.describe_current do
  subject(:callback) { described_class.new(bearer, monitor) }

  let(:bearer) { instance_double('Rdkafka::Producer', name: 'test_bearer') }
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
  end
end
