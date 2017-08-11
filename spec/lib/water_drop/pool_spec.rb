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

    before do
      expect(::WaterDrop::Config)
        .to receive(:config)
        .and_return(config)
        .exactly(2).times

      expect(config)
        .to receive(:connection_pool)
        .and_return(connection_pool)
        .exactly(2).times

      expect(ConnectionPool)
        .to receive(:new)
        .with(
          size: connection_pool.size,
          timeout: connection_pool.timeout
        )
        .and_yield

      expect(WaterDrop::ProducerProxy)
        .to receive(:new)
        .and_return(producer)
    end

    it { waterdrop_pool.pool }
  end
end
