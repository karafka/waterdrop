# frozen_string_literal: true

class WaterDropContractsTransactionalOffsetTest < WaterDropTest::Base
  def setup
    @message_stub = Struct.new(:topic, :partition, :offset, keyword_init: true)
    @consumer = Minitest::Mock.new
    @consumer.expect(:consumer_group_metadata_pointer, true)

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
  end

  def contract_result
    WaterDrop::Contracts::TransactionalOffset.new.call(@input)
  end

  def test_all_valid_inputs
    assert_predicate contract_result, :success?
  end

  def test_consumer_invalid
    @input[:consumer] = nil

    refute_predicate contract_result, :success?
  end

  def test_message_invalid_string
    @input[:message] = "test"

    refute_predicate contract_result, :success?
  end

  def test_offset_metadata_invalid_not_string_or_nil
    @input[:offset_metadata] = 123

    refute_predicate contract_result, :success?
  end
end
