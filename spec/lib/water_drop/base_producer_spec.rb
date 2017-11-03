# frozen_string_literal: true

RSpec.describe WaterDrop::BaseProducer do
  # call method is specked in the children producers context

  describe '#validate!' do
    subject(:validation) { described_class.send(:validate!, options) }

    context 'when provided with invalid options' do
      let(:options) { {} }

      it { expect { validation }.to raise_error(WaterDrop::Errors::InvalidMessageOptions) }
    end

    context 'when provided with valid options' do
      let(:options) { { topic: 'example-topic' } }

      it { expect { validation }.not_to raise_error }
    end
  end
end
