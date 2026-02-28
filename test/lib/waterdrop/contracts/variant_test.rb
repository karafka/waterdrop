# frozen_string_literal: true

class WaterDropContractsVariantTest < WaterDropTest::Base
  def setup
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
  end

  def contract_result
    WaterDrop::Contracts::Variant.new.call(@variant)
  end

  def contract_errors
    contract_result.errors.to_h
  end

  def test_valid_context
    assert_predicate contract_result, :success?
  end

  def test_default_missing
    @variant.delete(:default)

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:default]
  end

  def test_default_not_boolean
    @variant[:default] = "true"

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:default]
  end

  def test_max_wait_timeout_missing
    @variant.delete(:max_wait_timeout)

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:max_wait_timeout]
  end

  def test_max_wait_timeout_not_a_number
    @variant[:max_wait_timeout] = "10"

    refute_predicate contract_result, :success?
    refute_empty contract_errors[:max_wait_timeout]
  end

  def test_topic_config_non_symbol_key
    @variant[:topic_config]["invalid_key"] = true

    refute_predicate contract_result, :success?
  end

  def test_topic_config_non_per_topic_key
    @variant[:topic_config][:"batch.size"] = 1

    refute_predicate contract_result, :success?
  end

  def test_transactional_redefine_acks
    @variant[:transactional] = true
    @variant[:topic_config][:acks] = 1

    refute_predicate contract_result, :success?
  end

  def test_transactional_redefine_request_required_acks
    @variant[:transactional] = true
    @variant[:topic_config][:"request.required.acks"] = 1

    refute_predicate contract_result, :success?
  end

  def test_idempotent_redefine_acks
    @variant[:idempotent] = true
    @variant[:topic_config][:acks] = 1

    refute_predicate contract_result, :success?
  end

  def test_idempotent_redefine_request_required_acks
    @variant[:idempotent] = true
    @variant[:topic_config][:"request.required.acks"] = 1

    refute_predicate contract_result, :success?
  end
end
