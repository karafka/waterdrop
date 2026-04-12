# frozen_string_literal: true

describe_current do
  before do
    @producer = build(:producer)
    @topic_name = generate_topic
    create_topic(@topic_name)
  end

  after { @producer.close }

  describe "#tombstone_sync" do
    context "when message has topic, key, and partition" do
      it "expect to return a delivery report" do
        result = @producer.tombstone_sync(topic: @topic_name, key: "user-123", partition: 0)

        assert_kind_of(Rdkafka::Producer::DeliveryReport, result)
      end
    end

    context "when message includes optional headers" do
      it "expect to return a delivery report" do
        result = @producer.tombstone_sync(
          topic: @topic_name,
          key: "user-123",
          partition: 0,
          headers: { "reason" => "deleted" }
        )

        assert_kind_of(Rdkafka::Producer::DeliveryReport, result)
      end
    end

    context "when message has payload passed" do
      it "expect to silently ignore it and produce a tombstone" do
        result = @producer.tombstone_sync(
          topic: @topic_name,
          key: "user-123",
          partition: 0,
          payload: "should be ignored"
        )

        assert_kind_of(Rdkafka::Producer::DeliveryReport, result)
      end
    end

    context "when key is missing" do
      it "expect to raise MessageInvalidError" do
        assert_raises(WaterDrop::Errors::MessageInvalidError) do
          @producer.tombstone_sync(topic: @topic_name, partition: 0)
        end
      end
    end

    context "when key is nil" do
      it "expect to raise MessageInvalidError" do
        assert_raises(WaterDrop::Errors::MessageInvalidError) do
          @producer.tombstone_sync(topic: @topic_name, key: nil, partition: 0)
        end
      end
    end

    context "when partition is missing" do
      it "expect to raise MessageInvalidError" do
        assert_raises(WaterDrop::Errors::MessageInvalidError) do
          @producer.tombstone_sync(topic: @topic_name, key: "user-123")
        end
      end
    end
  end

  describe "#tombstone_async" do
    context "when message has topic, key, and partition" do
      it "expect to return a delivery handle" do
        result = @producer.tombstone_async(topic: @topic_name, key: "user-123", partition: 0)

        assert_kind_of(Rdkafka::Producer::DeliveryHandle, result)
      end
    end

    context "when key is missing" do
      it "expect to raise MessageInvalidError" do
        assert_raises(WaterDrop::Errors::MessageInvalidError) do
          @producer.tombstone_async(topic: @topic_name, partition: 0)
        end
      end
    end

    context "when partition is missing" do
      it "expect to raise MessageInvalidError" do
        assert_raises(WaterDrop::Errors::MessageInvalidError) do
          @producer.tombstone_async(topic: @topic_name, key: "user-123")
        end
      end
    end
  end

  describe "#tombstone_many_sync" do
    context "when all messages are valid" do
      it "expect all results to be delivery handles" do
        messages = [
          { topic: @topic_name, key: "k1", partition: 0 },
          { topic: @topic_name, key: "k2", partition: 0 }
        ]

        results = @producer.tombstone_many_sync(messages)

        results.each do |result|
          assert_kind_of(Rdkafka::Producer::DeliveryHandle, result)
        end
      end
    end

    context "when one message is missing key" do
      it "expect to raise MessageInvalidError" do
        messages = [
          { topic: @topic_name, key: "k1", partition: 0 },
          { topic: @topic_name, partition: 0 }
        ]

        assert_raises(WaterDrop::Errors::MessageInvalidError) do
          @producer.tombstone_many_sync(messages)
        end
      end
    end
  end

  describe "#tombstone_many_async" do
    context "when all messages are valid" do
      it "expect all results to be delivery handles" do
        messages = [
          { topic: @topic_name, key: "k1", partition: 0 },
          { topic: @topic_name, key: "k2", partition: 0 }
        ]

        results = @producer.tombstone_many_async(messages)

        results.each do |result|
          assert_kind_of(Rdkafka::Producer::DeliveryHandle, result)
        end
      end
    end

    context "when one message is missing partition" do
      it "expect to raise MessageInvalidError" do
        messages = [
          { topic: @topic_name, key: "k1", partition: 0 },
          { topic: @topic_name, key: "k2" }
        ]

        assert_raises(WaterDrop::Errors::MessageInvalidError) do
          @producer.tombstone_many_async(messages)
        end
      end
    end
  end
end
