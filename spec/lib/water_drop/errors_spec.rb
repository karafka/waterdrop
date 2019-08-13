# frozen_string_literal: true

RSpec.describe WaterDrop::Errors do
  describe 'BaseError' do
    subject(:error) { described_class::BaseError }

    specify { expect(error).to be < StandardError }
  end

  describe 'ConfigurationInvalidError' do
    subject(:error) { described_class::ConfigurationInvalidError }

    specify { expect(error).to be < described_class::BaseError }
  end

  describe 'MessageInvalidError' do
    subject(:error) { described_class::MessageInvalidError }

    specify { expect(error).to be < described_class::BaseError }
  end

  pending
end
