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

  context "when variant method completes" do
    before do
      @variant = @producer.with(topic_config: { acks: 1 })
    end

    it "expect to fully remove the producer key from fiber-local waterdrop_clients" do
      fiber = Fiber.new do
        @variant.produce_sync(topic: @topic, payload: "")

        clients = Fiber.current.waterdrop_clients

        refute clients.key?(@producer.id), "Expected producer key to be deleted, got: #{clients.inspect}"
      end

      fiber.resume
    end

    it "expect to not accumulate stale keys across multiple calls" do
      fiber = Fiber.new do
        3.times { @variant.produce_sync(topic: @topic, payload: "") }

        clients = Fiber.current.waterdrop_clients

        assert_equal 0, clients.size, "Expected empty hash, got: #{clients.inspect}"
      end

      fiber.resume
    end
  end

  # Regression for the nested-variant clobber: the only variant-wrapped method that yields user
  # code is `transaction`, so a nested same-producer variant call inside it (and a raw dispatch
  # within the scope) must keep resolving to the outer variant. Without save/restore in the
  # wrapper's ensure, the inner call deletes the shared fiber-local slot and the rest of the block
  # silently falls back to the default variant.
  context "when a nested same-producer call runs inside a variant transaction" do
    before do
      @producer = build(:transactional_producer)
      @variant = @producer.with(topic_config: { "message.timeout.ms": 10_000 })
    end

    it "keeps the outer variant active for the rest of the block" do
      after_inner_variant_call = nil
      after_raw_dispatch = nil

      @variant.transaction do
        @variant.produce_async(topic: @topic, payload: "1")
        after_inner_variant_call = @producer.send(:current_variant)

        @producer.produce_async(topic: @topic, payload: "2")
        after_raw_dispatch = @producer.send(:current_variant)
      end

      assert_same(@variant, after_inner_variant_call)
      assert_same(@variant, after_raw_dispatch)
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

  describe "buffering through a variant" do
    before do
      @acks_zero = @producer.with(topic_config: { acks: 0 })
      @variant_topic = generate_topic
      @default_topic = generate_topic
    end

    # With acks: 0 librdkafka reports -1001 counting up within a batch, so it is checked as
    # negative. The first report on a new topic may be -1001 even with acks, so only the later ones
    # are checked on the default topic.
    def buffer_both(variant_buffers:)
      3.times do
        @acks_zero.buffer(topic: @variant_topic, payload: "v") if variant_buffers
        @producer.buffer(topic: @default_topic, payload: "d")
      end
    end

    def offsets(handles, topic)
      handles.map(&:create_result).select { |result| result.topic_name == topic }.map(&:offset)
    end

    it "expect the producer flush_sync to keep the variant topic_config" do
      3.times { @acks_zero.buffer(topic: @variant_topic, payload: "v") }

      assert(offsets(@producer.flush_sync, @variant_topic).all?(&:negative?))
    end

    it "expect the producer flush_async to keep the variant topic_config" do
      3.times { @acks_zero.buffer_many([{ topic: @variant_topic, payload: "v" }]) }
      handles = @producer.flush_async
      handles.each(&:wait)

      assert(offsets(handles, @variant_topic).all?(&:negative?))
    end

    it "expect the variant flush_sync to send default messages with the default topic_config" do
      buffer_both(variant_buffers: false)

      assert(offsets(@acks_zero.flush_sync, @default_topic).drop(1).all? { |offset| offset >= 0 })
    end

    it "expect the variant flush_async to send default messages with the default topic_config" do
      buffer_both(variant_buffers: false)
      handles = @acks_zero.flush_async
      handles.each(&:wait)

      assert(offsets(handles, @default_topic).drop(1).all? { |offset| offset >= 0 })
    end

    it "expect a mixed buffer flushed once to send each message with its own topic_config" do
      buffer_both(variant_buffers: true)
      handles = @producer.flush_sync

      assert_equal(6, handles.size)
      assert(offsets(handles, @variant_topic).all?(&:negative?))
      assert(offsets(handles, @default_topic).drop(1).all? { |offset| offset >= 0 })
    end

    it "expect a mixed buffer flushed once by the variant to send each with its own topic_config" do
      buffer_both(variant_buffers: true)
      handles = @acks_zero.flush_sync

      assert_equal(6, handles.size)
      assert(offsets(handles, @variant_topic).all?(&:negative?))
      assert(offsets(handles, @default_topic).drop(1).all? { |offset| offset >= 0 })
    end
  end
end
