# frozen_string_literal: true

require "test_helper"

describe_current do
  before do
    @contract = described_class.new
    @valid_config = {
      thread_priority: 0,
      poll_timeout: 1_000,
      backoff_min: 100,
      backoff_max: 30_000
    }
  end

  describe "#call" do
    describe "when config is valid" do
      it { assert @contract.call(@valid_config).success? }
    end

    describe "when thread_priority is invalid" do
      it "fails when below -3" do
        config = @valid_config.merge(thread_priority: -4)
        refute @contract.call(config).success?
      end

      it "fails when above 3" do
        config = @valid_config.merge(thread_priority: 4)
        refute @contract.call(config).success?
      end

      it "fails when not an integer" do
        config = @valid_config.merge(thread_priority: 1.5)
        refute @contract.call(config).success?
      end

      it "succeeds at boundary -3" do
        config = @valid_config.merge(thread_priority: -3)
        assert @contract.call(config).success?
      end

      it "succeeds at boundary 3" do
        config = @valid_config.merge(thread_priority: 3)
        assert @contract.call(config).success?
      end
    end

    describe "when poll_timeout is invalid" do
      it "fails when zero" do
        config = @valid_config.merge(poll_timeout: 0)
        refute @contract.call(config).success?
      end

      it "fails when negative" do
        config = @valid_config.merge(poll_timeout: -1)
        refute @contract.call(config).success?
      end

      it "fails when not an integer" do
        config = @valid_config.merge(poll_timeout: 1.5)
        refute @contract.call(config).success?
      end
    end

    describe "when backoff_min is invalid" do
      it "fails when zero" do
        config = @valid_config.merge(backoff_min: 0)
        refute @contract.call(config).success?
      end

      it "fails when negative" do
        config = @valid_config.merge(backoff_min: -1)
        refute @contract.call(config).success?
      end
    end

    describe "when backoff_max is invalid" do
      it "fails when zero" do
        config = @valid_config.merge(backoff_max: 0)
        refute @contract.call(config).success?
      end

      it "fails when less than backoff_min" do
        config = @valid_config.merge(backoff_min: 1000, backoff_max: 100)
        refute @contract.call(config).success?
      end

      it "succeeds when equal to backoff_min" do
        config = @valid_config.merge(backoff_min: 500, backoff_max: 500)
        assert @contract.call(config).success?
      end
    end
  end
end
