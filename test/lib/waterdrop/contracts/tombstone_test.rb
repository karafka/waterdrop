# frozen_string_literal: true

describe_current do
  before do
    @message = {
      topic: "name",
      key: rand.to_s,
      partition: 0,
      payload: nil,
      headers: {}
    }
    @contract_result = described_class.new.call(@message)
    @errors = @contract_result.errors.to_h
  end

  describe "when message is valid" do
    it { assert_predicate @contract_result, :success? }
    it { assert_empty @errors }
  end

  describe "when we run key validations" do
    describe "when key is nil" do
      before {
        @message[:key] = nil
        @contract_result = described_class.new.call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:key] }
    end

    describe "when key is not a string" do
      before {
        @message[:key] = rand
        @contract_result = described_class.new.call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:key] }
    end

    describe "when key is empty" do
      before {
        @message[:key] = ""
        @contract_result = described_class.new.call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:key] }
    end

    describe "when key is not present in options" do
      before {
        @message.delete(:key)
        @contract_result = described_class.new.call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:key] }
    end

    describe "when key is valid" do
      before {
        @message[:key] = rand.to_s
        @contract_result = described_class.new.call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { assert_predicate @contract_result, :success? }
      it { assert_empty @errors }
    end
  end

  describe "when we run partition validations" do
    describe "when partition is nil" do
      before {
        @message[:partition] = nil
        @contract_result = described_class.new.call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:partition] }
    end

    describe "when partition is not an integer" do
      before {
        @message[:partition] = rand
        @contract_result = described_class.new.call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:partition] }
    end

    describe "when partition is negative" do
      before {
        @message[:partition] = -1
        @contract_result = described_class.new.call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:partition] }
    end

    describe "when partition is not present in options" do
      before {
        @message.delete(:partition)
        @contract_result = described_class.new.call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:partition] }
    end

    describe "when partition is valid" do
      before {
        @message[:partition] = rand(100)
        @contract_result = described_class.new.call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { assert_predicate @contract_result, :success? }
      it { assert_empty @errors }
    end
  end
end
