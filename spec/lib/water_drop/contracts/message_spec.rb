# frozen_string_literal: true

RSpec.describe WaterDrop::Contracts::Message do
  let(:contract_result) { described_class.new.call(message) }

  let(:message) do
    {
      topic: 'name',
      payload: 'data',
      key: rand.to_s,
      partition: 0,
      timestamp: Time.now,
      headers: {}
    }
  end

  context 'when message are valid' do
    it { expect(contract_result).to be_success }
  end

  context 'when we run topic validations' do
    context 'when topic is nil but present in options' do
      before { message[:topic] = nil }

      it { expect(contract_result).not_to be_success }
    end

    context 'when topic is not a string' do
      before { message[:topic] = rand }

      it { expect(contract_result).not_to be_success }
    end

    context 'when topic is a symbol' do
      before { message[:topic] = :symbol }

      it { expect(contract_result).not_to be_success }
    end

    context 'when topic has an invalid format' do
      before { message[:topic] = '%^&*(' }

      it { expect(contract_result).not_to be_success }
    end

    context 'when topic is not present in options' do
      before { message.delete(:topic) }

      it { expect(contract_result).not_to be_success }
    end
  end

  context 'when we run key validations' do
    context 'when key is nil but present in options' do
      before { message[:key] = nil }

      it { expect(contract_result).to be_success }
    end

    context 'when key is not a string' do
      before { message[:key] = rand }

      it { expect(contract_result).not_to be_success }
    end

    context 'when key is empty' do
      before { message[:key] = '' }

      it { expect(contract_result).not_to be_success }
    end

    context 'when key is valid' do
      before { message[:key] = rand.to_s }

      it { expect(contract_result).to be_success }
    end

    context 'when key is not present in options' do
      before { message.delete(:key) }

      it { expect(contract_result).to be_success }
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
    end

    context 'when partition is empty' do
      before { message[:partition] = '' }

      it { expect(contract_result).not_to be_success }
    end

    context 'when partition is valid' do
      before { message[:partition] = rand(100) }

      it { expect(contract_result).to be_success }
    end

    context 'when partition is not present in options' do
      before { message.delete(:partition) }

      it { expect(contract_result).to be_success }
    end

    context 'when partition is less than -1' do
      before { message[:partition] = rand(2..100) * -1 }

      it { expect(contract_result).not_to be_success }
    end
  end

  context 'when we run timestamp validations' do
    context 'when timestamp is nil but present in options' do
      before { message[:timestamp] = nil }

      it { expect(contract_result).to be_success }
    end

    context 'when timestamp is not a time' do
      before { message[:timestamp] = rand }

      it { expect(contract_result).not_to be_success }
    end

    context 'when timestamp is empty' do
      before { message[:timestamp] = '' }

      it { expect(contract_result).not_to be_success }
    end

    context 'when timestamp is valid time' do
      before { message[:timestamp] = Time.now }

      it { expect(contract_result).to be_success }
    end

    context 'when timestamp is valid integer' do
      before { message[:timestamp] = Time.now.to_i }

      it { expect(contract_result).to be_success }
    end

    context 'when timestamp is not present in options' do
      before { message.delete(:timestamp) }

      it { expect(contract_result).to be_success }
    end
  end

  context 'when we run headers validations' do
    context 'when headers is nil but present in options' do
      before { message[:headers] = nil }

      it { expect(contract_result).to be_success }
    end

    context 'when headers is not a hash' do
      before { message[:headers] = rand }

      it { expect(contract_result).not_to be_success }
    end

    context 'when headers is an empty hash' do
      before { message[:headers] = {} }

      it { expect(contract_result).to be_success }
    end

    context 'when headers is valid hash with data' do
      before { message[:headers] = { rand.to_s => rand.to_s } }

      it { expect(contract_result).to be_success }
    end

    context 'when headers is not present in options' do
      before { message.delete(:headers) }

      it { expect(contract_result).to be_success }
    end

    context 'when headers keys are not string' do
      before { message[:headers] = { rand => rand.to_s } }

      it { expect(contract_result).not_to be_success }
      it { expect { contract_result.errors }.not_to raise_error }
    end

    context 'when headers values are not string' do
      before { message[:headers] = { rand.to_s => rand } }

      it { expect(contract_result).not_to be_success }
      it { expect { contract_result.errors }.not_to raise_error }
    end
  end
end
