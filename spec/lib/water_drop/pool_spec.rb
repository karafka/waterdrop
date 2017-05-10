# frozen_string_literal: true

RSpec.describe WaterDrop::Pool do
  subject { described_class }

  let(:pool) { double }
  let(:producer) { double }

  describe '.with' do
    let(:key) { rand }

    it 'delegates it to pool' do
      expect(subject).to receive(:pool).and_return(pool)
      expect(pool).to receive(:with).and_yield(producer)
      expect(producer).to receive(:get).with(key)

      subject.with { |statsd| statsd.get(key) }
    end
  end

  describe '.pool' do
    let(:config) { double }
    let(:connection_pool_size) { double }
    let(:connection_pool_timeout) { double }

    before do
      expect(::WaterDrop::Config)
        .to receive(:config)
        .and_return(config)
        .exactly(2).times

      expect(config)
        .to receive(:connection_pool_timeout)
        .and_return(connection_pool_timeout)

      expect(config)
        .to receive(:connection_pool_size)
        .and_return(connection_pool_size)

      expect(ConnectionPool)
        .to receive(:new)
        .with(
          size: connection_pool_size,
          timeout: connection_pool_timeout
        )
        .and_yield

      expect(WaterDrop::ProducerProxy)
        .to receive(:new)
        .and_return(producer)
    end

    it { subject.pool }
  end
end
