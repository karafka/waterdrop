require 'spec_helper'
require 'logger'

RSpec.describe WaterDrop do
  describe '#logger' do
    let(:logger) { double }
    let(:log) { double }
    let(:set_logger) { double }

    it 'returns NULL logger' do
      allow(WaterDrop.instance_variable_get(:@logger)) { nil }
      expect(WaterDrop.logger).to eq(WaterDrop::NullLogger)
    end

    it 'returns set logger' do
      allow(WaterDrop.instance_variable_get(:@logger)) { set_logger }
      expect(WaterDrop).to receive(:logger).and_return(set_logger)
      WaterDrop.logger
    end
  end

  describe '#setup' do
    before do
      WaterDrop.setup do |config|
        config.send_events = true
      end
    end

    it 'sets up the configuration' do
      expect(WaterDrop.config.send_events).to eq(true)
    end
  end
end
