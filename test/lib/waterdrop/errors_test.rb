# frozen_string_literal: true

describe_current do
  describe "BaseError" do
    before { @error = described_class::BaseError }

    it { assert_operator(@error, :<, StandardError) }
  end

  describe "ConfigurationInvalidError" do
    before { @error = described_class::ConfigurationInvalidError }

    it { assert_operator(@error, :<, described_class::BaseError) }
  end

  describe "VariantInvalidError" do
    before { @error = described_class::VariantInvalidError }

    it { assert_operator(@error, :<, described_class::BaseError) }
  end

  describe "ProducerNotConfiguredError" do
    before { @error = described_class::ProducerNotConfiguredError }

    it { assert_operator(@error, :<, described_class::BaseError) }
  end

  describe "ProducerAlreadyConfiguredError" do
    before { @error = described_class::ProducerAlreadyConfiguredError }

    it { assert_operator(@error, :<, described_class::BaseError) }
  end

  describe "ProducerUsedInParentProcess" do
    before { @error = described_class::ProducerUsedInParentProcess }

    it { assert_operator(@error, :<, described_class::BaseError) }
  end

  describe "ProducerClosedError" do
    before { @error = described_class::ProducerClosedError }

    it { assert_operator(@error, :<, described_class::BaseError) }
  end

  describe "ProducerTransactionalCloseAttemptError" do
    before { @error = described_class::ProducerTransactionalCloseAttemptError }

    it { assert_operator(@error, :<, described_class::BaseError) }
  end

  describe "MessageInvalidError" do
    before { @error = described_class::MessageInvalidError }

    it { assert_operator(@error, :<, described_class::BaseError) }
  end

  describe "StatusInvalidError" do
    before { @error = described_class::StatusInvalidError }

    it { assert_operator(@error, :<, described_class::BaseError) }
  end

  describe "AbortTransaction" do
    before { @error = described_class::AbortTransaction }

    it { assert_operator(@error, :<, described_class::BaseError) }
  end

  describe "TransactionRequiredError" do
    before { @error = described_class::TransactionRequiredError }

    it { assert_operator(@error, :<, described_class::BaseError) }
  end

  describe "PollerError" do
    before { @error = described_class::PollerError }

    it { assert_operator(@error, :<, described_class::BaseError) }
  end

  # Aliases for better DX
  describe "root AbortTransaction" do
    before { @error = WaterDrop::AbortTransaction }

    it { assert_equal(described_class::AbortTransaction, @error) }
  end
end
