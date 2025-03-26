# frozen_string_literal: true

RSpec.describe_current do
  subject(:contract_result) do
    described_class.new(
      max_payload_size: max_payload_size
    ).call(message)
  end

  let(:errors) { contract_result.errors.to_h }
  let(:max_payload_size) { 1024 }
  let(:message) do
    {
      topic: 'name',
      payload: 'data',
      key: rand.to_s,
      partition: 0,
      partition_key: rand.to_s,
      timestamp: Time.now,
      headers: {}
    }
  end

  context 'when message is valid' do
    it { expect(contract_result).to be_success }
    it { expect(errors).to be_empty }
  end

  context 'when we run topic validations' do
    context 'when topic is nil but present in options' do
      before { message[:topic] = nil }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:topic]).not_to be_empty }
    end

    context 'when topic is not a string' do
      before { message[:topic] = rand }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:topic]).not_to be_empty }
    end

    context 'when topic is a valid symbol' do
      before { message[:topic] = :symbol }

      it { expect(contract_result).to be_success }
    end

    context 'when topic is a symbol that will not be a topic' do
      before { message[:topic] = '$%^&*()'.to_sym }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:topic]).not_to be_empty }
    end

    context 'when topic has an invalid format' do
      before { message[:topic] = '%^&*(' }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:topic]).not_to be_empty }
    end

    context 'when topic is not present in options' do
      before { message.delete(:topic) }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:topic]).not_to be_empty }
    end
  end

  context 'when we run payload validations' do
    context 'when payload is nil but present (tombstone)' do
      before { message[:payload] = nil }

      it { expect(contract_result).to be_success }
      it { expect(errors).to be_empty }
    end

    context 'when payload is not a string' do
      before { message[:payload] = rand }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:payload]).not_to be_empty }
    end

    context 'when payload is a symbol' do
      before { message[:payload] = :symbol }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:payload]).not_to be_empty }
    end

    context 'when payload is not present in options' do
      before { message.delete(:payload) }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:payload]).not_to be_empty }
    end

    context 'when payload size is more than allowed' do
      before { message[:payload] = 'X' * 2048 }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:payload]).not_to be_empty }
    end
  end

  context 'when we run key validations' do
    context 'when key is nil but present in options' do
      before { message[:key] = nil }

      it { expect(contract_result).to be_success }
      it { expect(errors).to be_empty }
    end

    context 'when key is not a string' do
      before { message[:key] = rand }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:key]).not_to be_empty }
    end

    context 'when key is empty' do
      before { message[:key] = '' }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:key]).not_to be_empty }
    end

    context 'when key is valid' do
      before { message[:key] = rand.to_s }

      it { expect(contract_result).to be_success }
      it { expect(errors).to be_empty }
    end

    context 'when key is not present in options' do
      before { message.delete(:key) }

      it { expect(contract_result).to be_success }
      it { expect(errors).to be_empty }
    end
  end

  context 'when we run partition validations' do
    context 'when partition is nil but present in options' do
      before { message[:partition] = nil }

      it { expect(contract_result).not_to be_success }
    end

    context 'when partition is not an int' do
      before { message[:partition] = rand }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:partition]).not_to be_empty }
    end

    context 'when partition is empty' do
      before { message[:partition] = '' }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:partition]).not_to be_empty }
    end

    context 'when partition is valid' do
      before { message[:partition] = rand(100) }

      it { expect(contract_result).to be_success }
      it { expect(errors).to be_empty }
    end

    context 'when partition is not present in options' do
      before { message.delete(:partition) }

      it { expect(contract_result).to be_success }
      it { expect(errors).to be_empty }
    end

    context 'when partition is less than -1' do
      before { message[:partition] = rand(2..100) * -1 }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:partition]).not_to be_empty }
    end
  end

  context 'when we run partition_key validations' do
    context 'when partition_key is nil but present in options' do
      before { message[:partition_key] = nil }

      it { expect(contract_result).to be_success }
      it { expect(errors).to be_empty }
    end

    context 'when partition_key is not a string' do
      before { message[:partition_key] = rand }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:partition_key]).not_to be_empty }
    end

    context 'when partition_key is empty' do
      before { message[:partition_key] = '' }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:partition_key]).not_to be_empty }
    end

    context 'when partition_key is valid' do
      before { message[:partition_key] = rand.to_s }

      it { expect(contract_result).to be_success }
      it { expect(errors).to be_empty }
    end

    context 'when partition_key is not present in options' do
      before { message.delete(:partition_key) }

      it { expect(contract_result).to be_success }
      it { expect(errors).to be_empty }
    end
  end

  context 'when we run timestamp validations' do
    context 'when timestamp is nil but present in options' do
      before { message[:timestamp] = nil }

      it { expect(contract_result).to be_success }
      it { expect(errors).to be_empty }
    end

    context 'when timestamp is not a time' do
      before { message[:timestamp] = rand }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:timestamp]).not_to be_empty }
    end

    context 'when timestamp is empty' do
      before { message[:timestamp] = '' }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:timestamp]).not_to be_empty }
    end

    context 'when timestamp is valid time' do
      before { message[:timestamp] = Time.now }

      it { expect(contract_result).to be_success }
      it { expect(errors).to be_empty }
    end

    context 'when timestamp is valid integer' do
      before { message[:timestamp] = Time.now.to_i }

      it { expect(contract_result).to be_success }
      it { expect(errors).to be_empty }
    end

    context 'when timestamp is not present in options' do
      before { message.delete(:timestamp) }

      it { expect(contract_result).to be_success }
      it { expect(errors).to be_empty }
    end
  end

  context 'when we run headers validations' do
    context 'when headers is nil but present in options' do
      before { message[:headers] = nil }

      it { expect(contract_result).to be_success }
      it { expect(errors).to be_empty }
    end

    context 'when headers is not a hash' do
      before { message[:headers] = rand }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:headers]).not_to be_empty }
    end

    context 'when headers key is not a string' do
      before { message[:headers] = { rand => 'value' } }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:headers]).not_to be_empty }
    end

    context 'when headers value is not a string or array of strings' do
      before { message[:headers] = { 'key' => rand } }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:headers]).not_to be_empty }
    end

    context 'when headers value is an array with non-string elements' do
      before { message[:headers] = { 'key' => ['value', rand] } }

      it { expect(contract_result).not_to be_success }
      it { expect(errors[:headers]).not_to be_empty }
    end

    context 'when headers value is a valid string' do
      before { message[:headers] = { 'key' => 'value' } }

      it { expect(contract_result).to be_success }
      it { expect(errors).to be_empty }
    end

    context 'when headers value is a valid array of strings' do
      before { message[:headers] = { 'key' => ['value1', 'value2'] } }

      it { expect(contract_result).to be_success }
      it { expect(errors).to be_empty }
    end

    context 'when headers is not present in options' do
      before { message.delete(:headers) }

      it { expect(contract_result).to be_success }
      it { expect(errors).to be_empty }
    end
  end
end
