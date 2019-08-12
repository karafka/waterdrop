# frozen_string_literal: true

RSpec.describe WaterDrop::Contracts::Config do
  let(:contract_result) { described_class.new.call(config) }
  let(:config) do
    {
      logger: Logger.new('/dev/null'),
      deliver: false,
      kafka: { 'bootstrap.servers' => 'localhost:9092' },
      wait_timeout: 1
    }
  end

  context 'when config is valid' do
    it { expect(contract_result).to be_success }
  end

  pending
end
