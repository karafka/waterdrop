# frozen_string_literal: true

RSpec.describe WaterDrop::Contracts::Config do
  subject(:contract_result) { described_class.new.call(config) }

  let(:contract_errors) { contract_result.errors.to_h }
  let(:config) do
    {
      id: SecureRandom.uuid,
      logger: Logger.new('/dev/null'),
      deliver: false,
      kafka: { 'bootstrap.servers' => 'localhost:9092' },
      max_payload_size: 1024 * 1024,
      max_wait_timeout: 1,
      wait_timeout: 0.1
    }
  end

  context 'when config is valid' do
    it { expect(contract_result).to be_success }
  end

  context 'when id is missing' do
    before { config.delete(:id) }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:id]).not_to be_empty }
  end

  context 'when id is nil' do
    before { config[:id] = nil }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:id]).not_to be_empty }
  end

  context 'when id is not a string' do
    before { config[:id] = rand }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:id]).not_to be_empty }
  end

  context 'when logger is missing' do
    before { config.delete(:logger) }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:logger]).not_to be_empty }
  end

  context 'when logger is nil' do
    before { config[:logger] = nil }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:logger]).not_to be_empty }
  end

  context 'when deliver is missing' do
    before { config.delete(:deliver) }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:deliver]).not_to be_empty }
  end

  context 'when deliver is nil' do
    before { config[:deliver] = nil }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:deliver]).not_to be_empty }
  end

  context 'when deliver is not a string' do
    before { config[:deliver] = rand }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:deliver]).not_to be_empty }
  end

  context 'when kafka is missing' do
    before { config.delete(:kafka) }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:kafka]).not_to be_empty }
  end

  context 'when kafka is nil' do
    before { config[:kafka] = nil }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:kafka]).not_to be_empty }
  end

  context 'when kafka is an empty hash' do
    before { config[:kafka] = {} }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:kafka]).not_to be_empty }
  end

  context 'when max_payload_size is nil' do
    before { config[:max_payload_size] = nil }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:max_payload_size]).not_to be_empty }
  end

  context 'when max_payload_size is a negative int' do
    before { config[:max_payload_size] = -1 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:max_payload_size]).not_to be_empty }
  end

  context 'when max_payload_size is a negative float' do
    before { config[:max_payload_size] = -0.1 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:max_payload_size]).not_to be_empty }
  end

  context 'when max_payload_size is 0' do
    before { config[:max_payload_size] = 0 }

    it { expect(contract_result).not_to be_success }
  end

  context 'when max_payload_size is positive int' do
    before { config[:max_payload_size] = 1 }

    it { expect(contract_result).to be_success }
  end

  context 'when max_payload_size is positive float' do
    before { config[:max_payload_size] = 1.1 }

    it { expect(contract_result).not_to be_success }
  end

  context 'when max_wait_timeout is missing' do
    before { config.delete(:max_wait_timeout) }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:max_wait_timeout]).not_to be_empty }
  end

  context 'when max_wait_timeout is nil' do
    before { config[:max_wait_timeout] = nil }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:max_wait_timeout]).not_to be_empty }
  end

  context 'when max_wait_timeout is a negative int' do
    before { config[:max_wait_timeout] = -1 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:max_wait_timeout]).not_to be_empty }
  end

  context 'when max_wait_timeout is a negative float' do
    before { config[:max_wait_timeout] = -0.1 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:max_wait_timeout]).not_to be_empty }
  end

  context 'when max_wait_timeout is 0' do
    before { config[:max_wait_timeout] = 0 }

    it { expect(contract_result).to be_success }
  end

  context 'when max_wait_timeout is positive int' do
    before { config[:max_wait_timeout] = 1 }

    it { expect(contract_result).to be_success }
  end

  context 'when max_wait_timeout is positive float' do
    before { config[:max_wait_timeout] = 1.1 }

    it { expect(contract_result).to be_success }
  end

  context 'when wait_timeout is missing' do
    before { config.delete(:wait_timeout) }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:wait_timeout]).not_to be_empty }
  end

  context 'when wait_timeout is nil' do
    before { config[:wait_timeout] = nil }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:wait_timeout]).not_to be_empty }
  end

  context 'when wait_timeout is a negative int' do
    before { config[:wait_timeout] = -1 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:wait_timeout]).not_to be_empty }
  end

  context 'when wait_timeout is a negative float' do
    before { config[:wait_timeout] = -0.1 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:wait_timeout]).not_to be_empty }
  end

  context 'when wait_timeout is 0' do
    before { config[:wait_timeout] = 0 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:wait_timeout]).not_to be_empty }
  end

  context 'when wait_timeout is positive int' do
    before { config[:wait_timeout] = 1 }

    it { expect(contract_result).to be_success }
  end

  context 'when wait_timeout is positive float' do
    before { config[:wait_timeout] = 1.1 }

    it { expect(contract_result).to be_success }
  end
end
