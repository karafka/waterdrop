# frozen_string_literal: true

RSpec.describe_current do
  subject(:contract_result) { described_class.new.call(input) }

  let(:message_stub) { Struct.new(:topic, :partition, :offset, keyword_init: true) }
  let(:consumer) { instance_double(Rdkafka::Consumer, consumer_group_metadata_pointer: true) }
  let(:topic) { "test_topic" }
  let(:partition) { 0 }
  let(:offset) { 10 }
  let(:message) { message_stub.new(topic: topic, partition: partition, offset: offset) }
  let(:offset_metadata) { "metadata" }
  let(:input) do
    {
      consumer: consumer,
      message: message,
      offset_metadata: offset_metadata
    }
  end

  context "when all inputs are valid" do
    it { expect(contract_result).to be_success }
  end

  context "when consumer is invalid" do
    let(:consumer) { nil }

    it { expect(contract_result).not_to be_success }
  end

  context "when message is invalid" do
    context "when message is a string" do
      let(:message) { "test" }

      it { expect(contract_result).not_to be_success }
    end
  end

  context "when offset_metadata is invalid" do
    context "when offset_metadata is not a string or nil" do
      let(:offset_metadata) { 123 }

      it { expect(contract_result).not_to be_success }
    end
  end
end
