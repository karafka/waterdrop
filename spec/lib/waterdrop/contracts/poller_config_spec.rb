# frozen_string_literal: true

RSpec.describe_current do
  subject(:contract) { described_class.new }

  let(:valid_config) do
    {
      thread_priority: 0,
      poll_timeout: 1_000,
      backoff_min: 100,
      backoff_max: 30_000
    }
  end

  describe "#call" do
    context "when config is valid" do
      it { expect(contract.call(valid_config)).to be_success }
    end

    context "when thread_priority is invalid" do
      it "fails when below -3" do
        config = valid_config.merge(thread_priority: -4)
        expect(contract.call(config)).not_to be_success
      end

      it "fails when above 3" do
        config = valid_config.merge(thread_priority: 4)
        expect(contract.call(config)).not_to be_success
      end

      it "fails when not an integer" do
        config = valid_config.merge(thread_priority: 1.5)
        expect(contract.call(config)).not_to be_success
      end

      it "succeeds at boundary -3" do
        config = valid_config.merge(thread_priority: -3)
        expect(contract.call(config)).to be_success
      end

      it "succeeds at boundary 3" do
        config = valid_config.merge(thread_priority: 3)
        expect(contract.call(config)).to be_success
      end
    end

    context "when poll_timeout is invalid" do
      it "fails when zero" do
        config = valid_config.merge(poll_timeout: 0)
        expect(contract.call(config)).not_to be_success
      end

      it "fails when negative" do
        config = valid_config.merge(poll_timeout: -1)
        expect(contract.call(config)).not_to be_success
      end

      it "fails when not an integer" do
        config = valid_config.merge(poll_timeout: 1.5)
        expect(contract.call(config)).not_to be_success
      end
    end

    context "when backoff_min is invalid" do
      it "fails when zero" do
        config = valid_config.merge(backoff_min: 0)
        expect(contract.call(config)).not_to be_success
      end

      it "fails when negative" do
        config = valid_config.merge(backoff_min: -1)
        expect(contract.call(config)).not_to be_success
      end
    end

    context "when backoff_max is invalid" do
      it "fails when zero" do
        config = valid_config.merge(backoff_max: 0)
        expect(contract.call(config)).not_to be_success
      end

      it "fails when less than backoff_min" do
        config = valid_config.merge(backoff_min: 1000, backoff_max: 100)
        expect(contract.call(config)).not_to be_success
      end

      it "succeeds when equal to backoff_min" do
        config = valid_config.merge(backoff_min: 500, backoff_max: 500)
        expect(contract.call(config)).to be_success
      end
    end
  end
end
