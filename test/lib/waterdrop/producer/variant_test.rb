# frozen_string_literal: true

class ProducerVariantTest < WaterDropTest::Base
  def setup
    @producer = build(:producer)
    @config_error = WaterDrop::Errors::VariantInvalidError
    @produce_error = WaterDrop::Errors::ProduceError
    @rd_config_error = Rdkafka::Config::ConfigError
    @topic = "it-#{SecureRandom.uuid}"
  end

  def teardown
    @producer.close
    super
  end

  def test_allows_creating_variants
    @producer.variant(topic_config: { acks: 1 })
  end

  def test_reference_original_producer
    variant = @producer.with(topic_config: { acks: 1 })

    assert_equal @producer, variant.producer
  end

  def test_invalid_string_keys_raises_error
    assert_raises(@config_error) do
      @producer.with(topic_config: { "acks" => 1 })
    end
  end

  def test_non_per_topic_attribute_raises_error
    assert_raises(@config_error) do
      @producer.with(topic_config: { "batch.size": 1 })
    end
  end

  def test_variant_with_invalid_config_value_raises_error
    variant = @producer.with(topic_config: { "message.timeout.ms": -1_000 })

    assert_raises(@rd_config_error) do
      variant.produce_sync(topic: "test", payload: "")
    end
  end

  def test_original_producer_works_after_creating_invalid_variant
    @producer.with(topic_config: { "message.timeout.ms": -1_000 })

    @producer.produce_sync(topic: @topic, payload: "")
  end

  def test_variant_with_valid_value_works
    variant = @producer.with(topic_config: { "message.timeout.ms": 100_000 })

    variant.produce_sync(topic: @topic, payload: "")
  end

  def test_variant_with_max_wait_timeout
    variant = @producer.with(max_wait_timeout: 1)

    assert_raises(@produce_error) do
      variant.produce_sync(topic: @topic, payload: "")
    end
  end
end

class ProducerVariantTransactionalTest < WaterDropTest::Base
  def setup
    @producer = build(:transactional_producer)
    @config_error = WaterDrop::Errors::VariantInvalidError
    @topic = "it-#{SecureRandom.uuid}"
  end

  def teardown
    @producer.close
    super
  end

  def test_transactional_producer_with_alterations
    variant = @producer.with(topic_config: { "message.timeout.ms": 10_000 })

    variant.produce_sync(topic: @topic, payload: "")
  end

  def test_transactional_producer_overwrite_acks_raises_error
    assert_raises(@config_error) do
      @producer.with(topic_config: { acks: 1 })
    end
  end
end

class ProducerVariantIdempotentTest < WaterDropTest::Base
  def setup
    @producer = build(:idempotent_producer)
    @config_error = WaterDrop::Errors::VariantInvalidError
    @topic = "it-#{SecureRandom.uuid}"
  end

  def teardown
    @producer.close
    super
  end

  def test_idempotent_producer_with_alterations
    variant = @producer.with(topic_config: { "message.timeout.ms": 10_000 })

    variant.produce_sync(topic: @topic, payload: "")
  end

  def test_idempotent_producer_overwrite_acks_raises_error
    assert_raises(@config_error) do
      @producer.with(topic_config: { acks: 1 })
    end
  end

  def test_lowering_acks_on_idempotent_producer_raises_error
    assert_raises(@config_error) do
      @producer.variant(topic_config: { acks: 1 })
    end
  end
end
