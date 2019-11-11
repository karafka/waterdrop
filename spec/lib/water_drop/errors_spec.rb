# frozen_string_literal: true

RSpec.describe WaterDrop::Errors do
  describe 'BaseError' do
    subject(:error) { described_class::BaseError }

    specify { expect(error).to be < StandardError }
  end

  describe 'InvalidConfiguration' do
    subject(:error) { described_class::InvalidConfiguration }

    specify { expect(error).to be < described_class::BaseError }
  end

  describe 'InvalidMessageOptions' do
    subject(:error) { described_class::InvalidMessageOptions }

    specify { expect(error).to be < described_class::BaseError }
  end
end
