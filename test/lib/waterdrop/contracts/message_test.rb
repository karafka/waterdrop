# frozen_string_literal: true

class WaterDropContractsMessageTest < WaterDropTest::Base
  def setup
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
  end

  def contract_result
    WaterDrop::Contracts::Message.new(max_payload_size: @max_payload_size).call(@message)
  end

  def errors
    contract_result.errors.to_h
  end

  # Valid message
  def test_valid_message
    assert_predicate contract_result, :success?
    assert_empty errors
  end

  # Topic validations
  def test_topic_nil
    @message[:topic] = nil

    refute_predicate contract_result, :success?
    refute_empty errors[:topic]
  end

  def test_topic_not_a_string
    @message[:topic] = rand

    refute_predicate contract_result, :success?
    refute_empty errors[:topic]
  end

  def test_topic_valid_symbol
    @message[:topic] = :symbol

    assert_predicate contract_result, :success?
  end

  def test_topic_invalid_symbol
    @message[:topic] = :"$%^&*()"

    refute_predicate contract_result, :success?
    refute_empty errors[:topic]
  end

  def test_topic_invalid_format
    @message[:topic] = "%^&*("

    refute_predicate contract_result, :success?
    refute_empty errors[:topic]
  end

  def test_topic_missing
    @message.delete(:topic)

    refute_predicate contract_result, :success?
    refute_empty errors[:topic]
  end

  # Payload validations
  def test_payload_nil_tombstone
    @message[:payload] = nil

    assert_predicate contract_result, :success?
    assert_empty errors
  end

  def test_payload_not_a_string
    @message[:payload] = rand

    refute_predicate contract_result, :success?
    refute_empty errors[:payload]
  end

  def test_payload_symbol
    @message[:payload] = :symbol

    refute_predicate contract_result, :success?
    refute_empty errors[:payload]
  end

  def test_payload_missing
    @message.delete(:payload)

    refute_predicate contract_result, :success?
    refute_empty errors[:payload]
  end

  def test_payload_too_large
    @message[:payload] = "X" * 2048

    refute_predicate contract_result, :success?
    refute_empty errors[:payload]
  end

  # Key validations
  def test_key_nil
    @message[:key] = nil

    assert_predicate contract_result, :success?
    assert_empty errors
  end

  def test_key_not_a_string
    @message[:key] = rand

    refute_predicate contract_result, :success?
    refute_empty errors[:key]
  end

  def test_key_empty
    @message[:key] = ""

    refute_predicate contract_result, :success?
    refute_empty errors[:key]
  end

  def test_key_valid
    @message[:key] = rand.to_s

    assert_predicate contract_result, :success?
    assert_empty errors
  end

  def test_key_missing
    @message.delete(:key)

    assert_predicate contract_result, :success?
    assert_empty errors
  end

  # Partition validations
  def test_partition_nil
    @message[:partition] = nil

    refute_predicate contract_result, :success?
  end

  def test_partition_not_an_int
    @message[:partition] = rand

    refute_predicate contract_result, :success?
    refute_empty errors[:partition]
  end

  def test_partition_empty_string
    @message[:partition] = ""

    refute_predicate contract_result, :success?
    refute_empty errors[:partition]
  end

  def test_partition_valid
    @message[:partition] = rand(100)

    assert_predicate contract_result, :success?
    assert_empty errors
  end

  def test_partition_missing
    @message.delete(:partition)

    assert_predicate contract_result, :success?
    assert_empty errors
  end

  def test_partition_less_than_negative_one
    @message[:partition] = rand(2..100) * -1

    refute_predicate contract_result, :success?
    refute_empty errors[:partition]
  end

  # Partition key validations
  def test_partition_key_nil
    @message[:partition_key] = nil

    assert_predicate contract_result, :success?
    assert_empty errors
  end

  def test_partition_key_not_a_string
    @message[:partition_key] = rand

    refute_predicate contract_result, :success?
    refute_empty errors[:partition_key]
  end

  def test_partition_key_empty
    @message[:partition_key] = ""

    refute_predicate contract_result, :success?
    refute_empty errors[:partition_key]
  end

  def test_partition_key_valid
    @message[:partition_key] = rand.to_s

    assert_predicate contract_result, :success?
    assert_empty errors
  end

  def test_partition_key_missing
    @message.delete(:partition_key)

    assert_predicate contract_result, :success?
    assert_empty errors
  end

  # Timestamp validations
  def test_timestamp_nil
    @message[:timestamp] = nil

    assert_predicate contract_result, :success?
    assert_empty errors
  end

  def test_timestamp_not_a_time
    @message[:timestamp] = rand

    refute_predicate contract_result, :success?
    refute_empty errors[:timestamp]
  end

  def test_timestamp_empty_string
    @message[:timestamp] = ""

    refute_predicate contract_result, :success?
    refute_empty errors[:timestamp]
  end

  def test_timestamp_valid_time
    @message[:timestamp] = Time.now

    assert_predicate contract_result, :success?
    assert_empty errors
  end

  def test_timestamp_valid_integer
    @message[:timestamp] = Time.now.to_i

    assert_predicate contract_result, :success?
    assert_empty errors
  end

  def test_timestamp_missing
    @message.delete(:timestamp)

    assert_predicate contract_result, :success?
    assert_empty errors
  end

  # Headers validations
  def test_headers_nil
    @message[:headers] = nil

    assert_predicate contract_result, :success?
    assert_empty errors
  end

  def test_headers_not_a_hash
    @message[:headers] = rand

    refute_predicate contract_result, :success?
    refute_empty errors[:headers]
  end

  def test_headers_key_not_a_string
    @message[:headers] = { rand => "value" }

    refute_predicate contract_result, :success?
    refute_empty errors[:headers]
  end

  def test_headers_value_not_a_string_or_array
    @message[:headers] = { "key" => rand }

    refute_predicate contract_result, :success?
    refute_empty errors[:headers]
  end

  def test_headers_value_array_with_non_string_elements
    @message[:headers] = { "key" => ["value", rand] }

    refute_predicate contract_result, :success?
    refute_empty errors[:headers]
  end

  def test_headers_value_valid_string
    @message[:headers] = { "key" => "value" }

    assert_predicate contract_result, :success?
    assert_empty errors
  end

  def test_headers_value_valid_array_of_strings
    @message[:headers] = { "key" => %w[value1 value2] }

    assert_predicate contract_result, :success?
    assert_empty errors
  end

  def test_headers_missing
    @message.delete(:headers)

    assert_predicate contract_result, :success?
    assert_empty errors
  end
end
