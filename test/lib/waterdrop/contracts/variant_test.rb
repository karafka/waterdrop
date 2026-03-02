# frozen_string_literal: true

require "test_helper"

describe_current do
  before do
    @variant = {
      default: true,
      max_wait_timeout: 10,
      transactional: false,
      idempotent: false,
      topic_config: {
        "request.required.acks": -1,
        acks: "all",
        "request.timeout.ms": 5_000,
        "message.timeout.ms": 10_000,
        "delivery.timeout.ms": 15_000,
        partitioner: "consistent_random",
        "compression.codec": "gzip"
      }
    }
    @contract_result = described_class.new.call(@variant)
    @contract_errors = @contract_result.errors.to_h
  end

  describe "when context is valid" do
    it { assert_predicate @contract_result, :success? }
  end

  describe "when default is missing" do
    before {
      @variant.delete(:default)
      @contract_result = described_class.new.call(@variant)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:default] }
  end

  describe "when default is not a boolean" do
    before {
      @variant[:default] = "true"
      @contract_result = described_class.new.call(@variant)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:default] }
  end

  describe "when max_wait_timeout is missing" do
    before {
      @variant.delete(:max_wait_timeout)
      @contract_result = described_class.new.call(@variant)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:max_wait_timeout] }
  end

  describe "when max_wait_timeout is not a number" do
    before {
      @variant[:max_wait_timeout] = "10"
      @contract_result = described_class.new.call(@variant)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
    it { refute_empty @contract_errors[:max_wait_timeout] }
  end

  describe "when topic_config hash is present" do
    describe "when there is a non-symbol key setting" do
      before {
        @variant[:topic_config]["invalid_key"] = true
        @contract_result = described_class.new.call(@variant)
        @contract_errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
    end
  end

  describe "when topic_config has contains non per-topic keys" do
    before {
      @variant[:topic_config][:"batch.size"] = 1
      @contract_result = described_class.new.call(@variant)
      @contract_errors = @contract_result.errors.to_h
    }

    it { refute_predicate @contract_result, :success? }
  end

  describe "when producer is transactional and we try to redefine acks" do
    before do
      @variant[:transactional] = true
      @variant[:topic_config][:acks] = 1
      @contract_result = described_class.new.call(@variant)
      @contract_errors = @contract_result.errors.to_h
    end

    it { refute_predicate @contract_result, :success? }
  end

  describe "when producer is transactional and we try to redefine request.required.acks" do
    before do
      @variant[:transactional] = true
      @variant[:topic_config][:"request.required.acks"] = 1
      @contract_result = described_class.new.call(@variant)
      @contract_errors = @contract_result.errors.to_h
    end

    it { refute_predicate @contract_result, :success? }
  end

  describe "when producer is idempotent and we try to redefine acks" do
    before do
      @variant[:idempotent] = true
      @variant[:topic_config][:acks] = 1
      @contract_result = described_class.new.call(@variant)
      @contract_errors = @contract_result.errors.to_h
    end

    it { refute_predicate @contract_result, :success? }
  end

  describe "when producer is idempotent and we try to redefine request.required.acks" do
    before do
      @variant[:idempotent] = true
      @variant[:topic_config][:"request.required.acks"] = 1
      @contract_result = described_class.new.call(@variant)
      @contract_errors = @contract_result.errors.to_h
    end

    it { refute_predicate @contract_result, :success? }
  end
end
