# frozen_string_literal: true

RSpec.describe_current do
  describe 'BaseError' do
    subject(:error) { described_class::BaseError }

    specify { expect(error).to be < StandardError }
  end

  describe 'ConfigurationInvalidError' do
    subject(:error) { described_class::ConfigurationInvalidError }

    specify { expect(error).to be < described_class::BaseError }
  end

  describe 'VariantInvalidError' do
    subject(:error) { described_class::VariantInvalidError }

    specify { expect(error).to be < described_class::BaseError }
  end

  describe 'ProducerNotConfiguredError' do
    subject(:error) { described_class::ProducerNotConfiguredError }

    specify { expect(error).to be < described_class::BaseError }
  end

  describe 'ProducerAlreadyConfiguredError' do
    subject(:error) { described_class::ProducerAlreadyConfiguredError }

    specify { expect(error).to be < described_class::BaseError }
  end

  describe 'ProducerUsedInParentProcess' do
    subject(:error) { described_class::ProducerUsedInParentProcess }

    specify { expect(error).to be < described_class::BaseError }
  end

  describe 'ProducerClosedError' do
    subject(:error) { described_class::ProducerClosedError }

    specify { expect(error).to be < described_class::BaseError }
  end

  describe 'ProducerTransactionalCloseAttemptError' do
    subject(:error) { described_class::ProducerTransactionalCloseAttemptError }

    specify { expect(error).to be < described_class::BaseError }
  end

  describe 'MessageInvalidError' do
    subject(:error) { described_class::MessageInvalidError }

    specify { expect(error).to be < described_class::BaseError }
  end

  describe 'StatusInvalidError' do
    subject(:error) { described_class::StatusInvalidError }

    specify { expect(error).to be < described_class::BaseError }
  end

  describe 'AbortTransaction' do
    subject(:error) { described_class::AbortTransaction }

    specify { expect(error).to be < described_class::BaseError }
  end

  describe 'TransactionRequiredError' do
    subject(:error) { described_class::TransactionRequiredError }

    specify { expect(error).to be < described_class::BaseError }
  end

  # Aliases for better DX
  describe 'root AbortTransaction' do
    subject(:error) { WaterDrop::AbortTransaction }

    specify { expect(error).to eq described_class::AbortTransaction }
  end
end
