# frozen_string_literal: true

describe_current do
  before do
    @producer = build(:producer)
    @config_error = WaterDrop::Errors::VariantInvalidError
    @produce_error = WaterDrop::Errors::ProduceError
    @rd_config_error = Rdkafka::Config::ConfigError
    @topic = generate_topic
  end

  after { @producer.close }

  describe "#variant" do
    it "expect to allow to create variants" do
      @producer.variant(topic_config: { acks: 1 })
    end
  end

  context "when referencing original producer back" do
    before do
      @variant = @producer.with(topic_config: { acks: 1 })
    end

    it "expect to be able to reference it" do
      assert_equal(@producer, @variant.producer)
    end
  end

  context "when trying to create variant with invalid string keys setup" do
    it "expect to raise the correct error" do
      assert_raises(@config_error) do
        @producer.with(topic_config: { "acks" => 1 })
      end
    end
  end

  context "when trying to create variant with attribute that is not per topic" do
    it "expect to raise the correct error" do
      assert_raises(@config_error) do
        @producer.with(topic_config: { "batch.size": 1 })
      end
    end
  end

  context "when having variant producer with a config with invalid value" do
    before do
      @variant = @producer.with(topic_config: { "message.timeout.ms": -1_000 })
    end

    it "expect to raise error" do
      assert_raises(@rd_config_error) { @variant.produce_sync(topic: "test", payload: "") }
    end

    it "expect to allow the original one" do
      _ = @variant
      @producer.produce_sync(topic: @topic, payload: "")
    end
  end

  context "when having variant producer with a valid value" do
    before do
      @variant = @producer.with(topic_config: { "message.timeout.ms": 100_000 })
    end

    it "expect to work" do
      @variant.produce_sync(topic: @topic, payload: "")
    end
  end

  context "when having a producer with variant max_wait_timeout" do
    before do
      # 1 ms will always be not enough to dispatch to a topic that needs to be created
      @variant = @producer.with(max_wait_timeout: 1)
    end

    it "expect to use this timeout it" do
      assert_raises(@produce_error) do
        @variant.produce_sync(topic: @topic, payload: "")
      end
    end
  end

  context "when having a transactional producer with alterations" do
    before do
      @producer = build(:transactional_producer)
      @variant = @producer.with(topic_config: { "message.timeout.ms": 10_000 })
    end

    it "expect to use the settings" do
      @variant.produce_sync(topic: @topic, payload: "")
    end

    context "when trying to overwrite acks on a transactional producer" do
      it "expect not to allow it" do
        assert_raises(@config_error) do
          @producer.with(topic_config: { acks: 1 })
        end
      end
    end
  end

  context "when having an idempotent producer with alterations" do
    before do
      @producer = build(:idempotent_producer)
      @variant = @producer.with(topic_config: { "message.timeout.ms": 10_000 })
    end

    it "expect to use the settings" do
      @variant.produce_sync(topic: @topic, payload: "")
    end

    context "when trying to overwrite acks on a idempotent producer" do
      it "expect not to allow it" do
        assert_raises(@config_error) do
          @producer.with(topic_config: { acks: 1 })
        end
      end
    end
  end

  context "when trying to lower the acks on an idempotent producer" do
    before do
      @producer = build(:idempotent_producer)
    end

    it "expect not to allow it" do
      assert_raises(@config_error) do
        @producer.variant(topic_config: { acks: 1 })
      end
    end
  end
end
