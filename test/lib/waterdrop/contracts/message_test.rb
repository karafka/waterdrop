# frozen_string_literal: true

require "test_helper"

describe_current do
  before do
    @max_payload_size = 1024
    @message = {
      topic: "name",
      payload: "data",
      key: rand.to_s,
      partition: 0,
      partition_key: rand.to_s,
      timestamp: Time.now,
      headers: {}
    }
    @contract_result = described_class.new(
      max_payload_size: @max_payload_size
    ).call(@message)
    @errors = @contract_result.errors.to_h
  end

  describe "when message is valid" do
    it { assert_predicate @contract_result, :success? }
    it { assert_empty @errors }
  end

  describe "when we run topic validations" do
    describe "when topic is nil but present in options" do
      before {
        @message[:topic] = nil
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:topic] }
    end

    describe "when topic is not a string" do
      before {
        @message[:topic] = rand
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:topic] }
    end

    describe "when topic is a valid symbol" do
      before {
        @message[:topic] = :symbol
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { assert_predicate @contract_result, :success? }
    end

    describe "when topic is a symbol that will not be a topic" do
      before {
        @message[:topic] = :"$%^&*()"
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:topic] }
    end

    describe "when topic has an invalid format" do
      before {
        @message[:topic] = "%^&*("
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:topic] }
    end

    describe "when topic is not present in options" do
      before {
        @message.delete(:topic)
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:topic] }
    end
  end

  describe "when we run payload validations" do
    describe "when payload is nil but present (tombstone)" do
      before {
        @message[:payload] = nil
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { assert_predicate @contract_result, :success? }
      it { assert_empty @errors }
    end

    describe "when payload is not a string" do
      before {
        @message[:payload] = rand
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:payload] }
    end

    describe "when payload is a symbol" do
      before {
        @message[:payload] = :symbol
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:payload] }
    end

    describe "when payload is not present in options" do
      before {
        @message.delete(:payload)
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:payload] }
    end

    describe "when payload size is more than allowed" do
      before {
        @message[:payload] = "X" * 2048
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:payload] }
    end
  end

  describe "when we run key validations" do
    describe "when key is nil but present in options" do
      before {
        @message[:key] = nil
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { assert_predicate @contract_result, :success? }
      it { assert_empty @errors }
    end

    describe "when key is not a string" do
      before {
        @message[:key] = rand
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:key] }
    end

    describe "when key is empty" do
      before {
        @message[:key] = ""
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:key] }
    end

    describe "when key is valid" do
      before {
        @message[:key] = rand.to_s
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { assert_predicate @contract_result, :success? }
      it { assert_empty @errors }
    end

    describe "when key is not present in options" do
      before {
        @message.delete(:key)
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { assert_predicate @contract_result, :success? }
      it { assert_empty @errors }
    end
  end

  describe "when we run partition validations" do
    describe "when partition is nil but present in options" do
      before {
        @message[:partition] = nil
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
    end

    describe "when partition is not an int" do
      before {
        @message[:partition] = rand
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:partition] }
    end

    describe "when partition is empty" do
      before {
        @message[:partition] = ""
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:partition] }
    end

    describe "when partition is valid" do
      before {
        @message[:partition] = rand(100)
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { assert_predicate @contract_result, :success? }
      it { assert_empty @errors }
    end

    describe "when partition is not present in options" do
      before {
        @message.delete(:partition)
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { assert_predicate @contract_result, :success? }
      it { assert_empty @errors }
    end

    describe "when partition is less than -1" do
      before {
        @message[:partition] = rand(2..100) * -1
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:partition] }
    end
  end

  describe "when we run partition_key validations" do
    describe "when partition_key is nil but present in options" do
      before {
        @message[:partition_key] = nil
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { assert_predicate @contract_result, :success? }
      it { assert_empty @errors }
    end

    describe "when partition_key is not a string" do
      before {
        @message[:partition_key] = rand
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:partition_key] }
    end

    describe "when partition_key is empty" do
      before {
        @message[:partition_key] = ""
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:partition_key] }
    end

    describe "when partition_key is valid" do
      before {
        @message[:partition_key] = rand.to_s
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { assert_predicate @contract_result, :success? }
      it { assert_empty @errors }
    end

    describe "when partition_key is not present in options" do
      before {
        @message.delete(:partition_key)
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { assert_predicate @contract_result, :success? }
      it { assert_empty @errors }
    end
  end

  describe "when we run timestamp validations" do
    describe "when timestamp is nil but present in options" do
      before {
        @message[:timestamp] = nil
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { assert_predicate @contract_result, :success? }
      it { assert_empty @errors }
    end

    describe "when timestamp is not a time" do
      before {
        @message[:timestamp] = rand
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:timestamp] }
    end

    describe "when timestamp is empty" do
      before {
        @message[:timestamp] = ""
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:timestamp] }
    end

    describe "when timestamp is valid time" do
      before {
        @message[:timestamp] = Time.now
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { assert_predicate @contract_result, :success? }
      it { assert_empty @errors }
    end

    describe "when timestamp is valid integer" do
      before {
        @message[:timestamp] = Time.now.to_i
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { assert_predicate @contract_result, :success? }
      it { assert_empty @errors }
    end

    describe "when timestamp is not present in options" do
      before {
        @message.delete(:timestamp)
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { assert_predicate @contract_result, :success? }
      it { assert_empty @errors }
    end
  end

  describe "when we run headers validations" do
    describe "when headers is nil but present in options" do
      before {
        @message[:headers] = nil
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { assert_predicate @contract_result, :success? }
      it { assert_empty @errors }
    end

    describe "when headers is not a hash" do
      before {
        @message[:headers] = rand
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:headers] }
    end

    describe "when headers key is not a string" do
      before {
        @message[:headers] = { rand => "value" }
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:headers] }
    end

    describe "when headers value is not a string or array of strings" do
      before {
        @message[:headers] = { "key" => rand }
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:headers] }
    end

    describe "when headers value is an array with non-string elements" do
      before {
        @message[:headers] = { "key" => ["value", rand] }
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { refute_predicate @contract_result, :success? }
      it { refute_empty @errors[:headers] }
    end

    describe "when headers value is a valid string" do
      before {
        @message[:headers] = { "key" => "value" }
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { assert_predicate @contract_result, :success? }
      it { assert_empty @errors }
    end

    describe "when headers value is a valid array of strings" do
      before {
        @message[:headers] = { "key" => %w[value1 value2] }
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { assert_predicate @contract_result, :success? }
      it { assert_empty @errors }
    end

    describe "when headers is not present in options" do
      before {
        @message.delete(:headers)
        @contract_result = described_class.new(max_payload_size: @max_payload_size).call(@message)
        @errors = @contract_result.errors.to_h
      }

      it { assert_predicate @contract_result, :success? }
      it { assert_empty @errors }
    end
  end
end
