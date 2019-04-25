# frozen_string_literal: true

RSpec.describe WaterDrop::Schemas do
  describe '#TOPIC_REGEXP' do
    subject(:match) { input.match? described_class::TOPIC_REGEXP }

    context 'valid topic' do
      let(:input) { 'name' }

      it { is_expected.to eq true }
    end

    context 'invalid topic' do
      let(:input) { '$%^&*' }

      it { is_expected.to eq false }
    end
  end
end
