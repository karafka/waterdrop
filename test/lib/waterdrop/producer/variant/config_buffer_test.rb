# frozen_string_literal: true

describe_current do
  before do
    @producer = build(:producer)
    @acks_zero = @producer.with(topic_config: { acks: 0 })
    @variant_topic = generate_topic
    @default_topic = generate_topic
  end

  after { @producer.close }

  def buffer_both(variant_buffers:, variant_topic: @variant_topic, default_topic: @default_topic)
    3.times do
      @acks_zero.buffer(topic: variant_topic, payload: "v") if variant_buffers
      @producer.buffer(topic: default_topic, payload: "d")
    end
  end

  def offsets(handles, topic)
    handles.map(&:create_result).select { |result| result.topic_name == topic }.map(&:offset)
  end

  # With acks: 0 librdkafka reports -1001 counting up within a batch, so it is checked as negative
  def assert_sent_with_acks_zero(handles, topic)
    sent = offsets(handles, topic)

    assert_equal(3, sent.size)
    assert(sent.all?(&:negative?))
  end

  # The first report on a new topic may be -1001 even with acks, so only the later ones are checked
  def assert_sent_with_acks(handles, topic)
    sent = offsets(handles, topic)

    assert_equal(3, sent.size)
    assert(sent.drop(1).all? { |offset| offset >= 0 })
  end

  it "expect the producer flush_sync to keep the variant topic_config" do
    3.times { @acks_zero.buffer(topic: @variant_topic, payload: "v") }

    assert_sent_with_acks_zero(@producer.flush_sync, @variant_topic)
  end

  it "expect the producer flush_async to keep the variant topic_config" do
    3.times { @acks_zero.buffer_many([{ topic: @variant_topic, payload: "v" }]) }
    handles = @producer.flush_async
    handles.each(&:wait)

    assert_sent_with_acks_zero(handles, @variant_topic)
  end

  it "expect the variant flush_sync to send default messages with the default topic_config" do
    buffer_both(variant_buffers: false)

    assert_sent_with_acks(@acks_zero.flush_sync, @default_topic)
  end

  it "expect the variant flush_async to send default messages with the default topic_config" do
    buffer_both(variant_buffers: false)
    handles = @acks_zero.flush_async
    handles.each(&:wait)

    assert_sent_with_acks(handles, @default_topic)
  end

  it "expect a mixed buffer flushed once to send each message with its own topic_config" do
    buffer_both(variant_buffers: true)
    handles = @producer.flush_sync

    assert_sent_with_acks_zero(handles, @variant_topic)
    assert_sent_with_acks(handles, @default_topic)
  end

  it "expect a mixed buffer flushed once by the variant to send each with its own topic_config" do
    buffer_both(variant_buffers: true)
    handles = @acks_zero.flush_sync

    assert_sent_with_acks_zero(handles, @variant_topic)
    assert_sent_with_acks(handles, @default_topic)
  end

  it "expect buffer and buffer_many not to change the user messages" do
    message = { topic: @variant_topic, payload: "v" }.freeze
    messages = [message, { topic: @variant_topic, payload: "v" }.freeze].freeze

    @acks_zero.buffer(message)
    @acks_zero.buffer_many(messages)

    refute(message.key?(:topic_config))
    refute(messages.any? { |msg| msg.key?(:topic_config) })
    assert_sent_with_acks_zero(@producer.flush_sync, @variant_topic)
  end

  context "when topics are symbols" do
    before do
      buffer_both(
        variant_buffers: true,
        variant_topic: @variant_topic.to_sym,
        default_topic: @default_topic.to_sym
      )
    end

    it "expect the producer flush to send each message with its own topic_config" do
      handles = @producer.flush_sync

      assert_sent_with_acks_zero(handles, @variant_topic)
      assert_sent_with_acks(handles, @default_topic)
    end

    it "expect the variant flush to send each message with its own topic_config" do
      handles = @acks_zero.flush_sync

      assert_sent_with_acks_zero(handles, @variant_topic)
      assert_sent_with_acks(handles, @default_topic)
    end
  end
end
