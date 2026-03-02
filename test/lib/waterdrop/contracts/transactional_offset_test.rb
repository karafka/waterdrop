# frozen_string_literal: true

require "test_helper"

describe_current do
  before do
    @message_stub = Struct.new(:topic, :partition, :offset, keyword_init: true)
    @consumer = OpenStruct.new(consumer_group_metadata_pointer: true)
    @topic = "test_topic"
    @partition = 0
    @offset = 10
    @message = @message_stub.new(topic: @topic, partition: @partition, offset: @offset)
    @offset_metadata = "metadata"
    @input = {
      consumer: @consumer,
      message: @message,
      offset_metadata: @offset_metadata
    }
    @contract_result = described_class.new.call(@input)
  end

  describe "when all inputs are valid" do
    it { assert_predicate @contract_result, :success? }
  end

  describe "when consumer is invalid" do
    before do
      @input[:consumer] = nil
      @contract_result = described_class.new.call(@input)
    end

    it { refute_predicate @contract_result, :success? }
  end

  describe "when message is invalid" do
    describe "when message is a string" do
      before do
        @input[:message] = "test"
        @contract_result = described_class.new.call(@input)
      end

      it { refute_predicate @contract_result, :success? }
    end
  end

  describe "when offset_metadata is invalid" do
    describe "when offset_metadata is not a string or nil" do
      before do
        @input[:offset_metadata] = 123
        @contract_result = described_class.new.call(@input)
      end

      it { refute_predicate @contract_result, :success? }
    end
  end
end
