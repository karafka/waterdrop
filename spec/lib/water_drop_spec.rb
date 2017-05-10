# frozen_string_literal: true
require 'logger'

RSpec.describe WaterDrop do
  describe '#logger' do
    let(:logger) { double }
    let(:log) { double }
    let(:set_logger) { double }

    it 'returns default null logger instnace' do
      allow(described_class.instance_variable_get(:@logger)) { nil }
      expect(described_class.logger).to be_a(NullLogger)
    end

    it 'returns set logger' do
      allow(described_class.instance_variable_get(:@logger)) { set_logger }
      expect(described_class).to receive(:logger).and_return(set_logger)
      described_class.logger
    end
  end

  describe '#setup' do
    before do
      described_class.setup do |config|
        config.send_messages = true
      end
    end

    it 'sets up the configuration' do
      expect(described_class.config.send_messages).to eq(true)
    end
  end
end
