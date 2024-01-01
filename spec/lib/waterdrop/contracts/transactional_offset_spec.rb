# frozen_string_literal: true

RSpec.describe_current do
  subject(:contract_result) { described_class.new.call(input) }

  let(:consumer) { instance_double('Consumer', consumer_group_metadata_pointer: true) }
  let(:topic) { 'test_topic' }
  let(:partition) { 0 }
  let(:offset) { 10 }
  let(:offset_metadata) { 'metadata' }
  let(:input) do
    {
      consumer: consumer,
      topic: topic,
      partition: partition,
      offset: offset,
      offset_metadata: offset_metadata
    }
  end

  context 'when all inputs are valid' do
    it { expect(contract_result).to be_success }
  end

  context 'when consumer is invalid' do
    let(:consumer) { nil }

    it { expect(contract_result).not_to be_success }
  end

  context 'when topic is invalid' do
    context 'when topic is empty' do
      let(:topic) { '' }

      it { expect(contract_result).not_to be_success }
    end

    context 'when topic is not a string' do
      let(:topic) { 123 }

      it { expect(contract_result).not_to be_success }
    end
  end

  context 'when partition is invalid' do
    context 'when partition is negative' do
      let(:partition) { -1 }

      it { expect(contract_result).not_to be_success }
    end

    context 'when partition is not an integer' do
      let(:partition) { 'not_an_integer' }

      it { expect(contract_result).not_to be_success }
    end
  end

  context 'when offset is invalid' do
    context 'when offset is negative' do
      let(:offset) { -10 }

      it { expect(contract_result).not_to be_success }
    end

    context 'when offset is not an integer' do
      let(:offset) { 'not_an_integer' }

      it { expect(contract_result).not_to be_success }
    end
  end

  context 'when offset_metadata is invalid' do
    context 'when offset_metadata is not a string or nil' do
      let(:offset_metadata) { 123 }

      it { expect(contract_result).not_to be_success }
    end
  end
end
