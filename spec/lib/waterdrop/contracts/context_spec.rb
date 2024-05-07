# frozen_string_literal: true

RSpec.describe_current do
  subject(:contract_result) { described_class.new.call(context) }

  let(:contract_errors) { contract_result.errors.to_h }
  let(:context) do
    {
      default: true,
      max_wait_timeout: 10,
      topic_config: {
        'request.required.acks': -1,
        acks: 'all',
        'request.timeout.ms': 5_000,
        'message.timeout.ms': 10_000,
        'delivery.timeout.ms': 15_000,
        partitioner: 'consistent_random',
        'compression.codec': 'gzip'
      }
    }
  end

  context 'when context is valid' do
    it { expect(contract_result).to be_success }
  end

  context 'when default is missing' do
    before { context.delete(:default) }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:default]).not_to be_empty }
  end

  context 'when default is not a boolean' do
    before { context[:default] = 'true' }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:default]).not_to be_empty }
  end

  context 'when max_wait_timeout is missing' do
    before { context.delete(:max_wait_timeout) }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:max_wait_timeout]).not_to be_empty }
  end

  context 'when max_wait_timeout is not a number' do
    before { context[:max_wait_timeout] = '10' }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:max_wait_timeout]).not_to be_empty }
  end

  context 'when topic_config hash is present' do
    context 'when there is a non-symbol key setting' do
      before { context[:topic_config]['invalid_key'] = true }

      it { expect(contract_result).not_to be_success }
    end
  end

  context 'when topic_config has contains non per-topic keys' do
    before { context[:topic_config][:'batch.size'] = 1 }

    it { expect(contract_result).not_to be_success }
  end
end
