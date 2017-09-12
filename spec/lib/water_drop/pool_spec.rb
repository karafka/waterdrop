# frozen_string_literal: true

RSpec.describe WaterDrop::Pool do
  subject(:waterdrop_pool) { described_class }

  let(:pool) { double }
  let(:producer) { double }

  describe '.with' do
    let(:key) { rand }

    it 'delegates it to pool' do
      expect(waterdrop_pool).to receive(:pool).and_return(pool)
      expect(pool).to receive(:with).and_yield(producer)
      expect(producer).to receive(:get).with(key)

      waterdrop_pool.with { |statsd| statsd.get(key) }
    end
  end

  describe '.pool' do
    let(:config) { double }
    let(:connection_pool) { OpenStruct.new(size: double, timeout: double) }
    let(:config_details) do
      {
        size: connection_pool.size,
        timeout: connection_pool.timeout
      }
    end

    before do
      allow(::WaterDrop::Config)
        .to receive(:config)
        .and_return(config)

      allow(config)
        .to receive(:connection_pool)
        .and_return(connection_pool)
    end

    it 'expect to build up a pool' do
      expect(ConnectionPool).to receive(:new).with(config_details).and_yield
      expect(WaterDrop::ProducerProxy).to receive(:new).and_return(producer)
      waterdrop_pool.pool
    end
  end
end
