# frozen_string_literal: true

describe_current do
  before do
    @buffer = StringIO.new
    @logger = Logger.new(@buffer)
    @listener = described_class.new(@logger)
    @producer = build(:producer)
    @message = build(:valid_message)
    @messages = [@message, build(:valid_message)]
    @details = {
      message: @message,
      messages: @messages,
      producer_id: @producer.id,
      time: rand(100),
      error: Rdkafka::RdkafkaError,
      dispatched: [@messages[0]]
    }
    @event = Karafka::Core::Monitoring::Event.new("event", @details)
    @logged_data = nil
  end

  # Helper to get logged data after listener method is called
  def logged_data
    @logged_data ||= @buffer.tap(&:rewind).read.split("\n")
  end

  after { @producer.close }

  describe "#on_message_produced_async" do
    before { @listener.on_message_produced_async(@event) }

    it { assert_includes(logged_data[0], @producer.id) }
    it { assert_includes(logged_data[0], "INFO") }
    it { assert_includes(logged_data[0], "topic was delegated to a dispatch queue") }
    it { assert_includes(logged_data[0], @message[:topic]) }
    it { assert_includes(logged_data[1], @producer.id) }
    it { assert_includes(logged_data[1], "DEBUG") }
    it { assert_includes(logged_data[1], @message.to_s) }

    describe "when we do not want to log messages content" do
      before do
        @listener = described_class.new(@logger, log_messages: false)
        @buffer = StringIO.new
        @logger = Logger.new(@buffer)
        @listener = described_class.new(@logger, log_messages: false)
        @listener.on_message_produced_async(@event)
        @logged_data = nil
      end

      it { assert_includes(logged_data[0], @producer.id) }
      it { assert_includes(logged_data[0], "INFO") }
      it { assert_includes(logged_data[0], "topic was delegated to a dispatch queue") }
      it { assert_includes(logged_data[0], @message[:topic]) }
      it { assert_nil(logged_data[1]) }
    end
  end

  describe "#on_message_produced_sync" do
    before { @listener.on_message_produced_sync(@event) }

    it { assert_includes(logged_data[0], @producer.id) }
    it { assert_includes(logged_data[0], "INFO") }
    it { assert_includes(logged_data[0], "Sync producing of a message to") }
    it { assert_includes(logged_data[0], @message[:topic]) }
    it { assert_includes(logged_data[1], @producer.id) }
    it { assert_includes(logged_data[1], "DEBUG") }
    it { assert_includes(logged_data[1], @message.to_s) }

    describe "when we do not want to log messages content" do
      before do
        @buffer = StringIO.new
        @logger = Logger.new(@buffer)
        @listener = described_class.new(@logger, log_messages: false)
        @listener.on_message_produced_sync(@event)
        @logged_data = nil
      end

      it { assert_includes(logged_data[0], @producer.id) }
      it { assert_includes(logged_data[0], "INFO") }
      it { assert_includes(logged_data[0], "Sync producing of a message to") }
      it { assert_includes(logged_data[0], @message[:topic]) }
      it { assert_nil(logged_data[1]) }
    end
  end

  describe "#on_messages_produced_async" do
    before do
      @dispatched_msg = "2 messages to 2 topics were delegated to a dispatch queue"
      @listener.on_messages_produced_async(@event)
    end

    it { assert_includes(logged_data[0], @producer.id) }
    it { assert_includes(logged_data[0], "INFO") }
    it { assert_includes(logged_data[0], @dispatched_msg) }
    it { assert_includes(logged_data[1], @producer.id) }
    it { assert_includes(logged_data[1], "DEBUG") }
    it { assert_includes(logged_data[1], @message.to_s) }

    describe "when we do not want to log messages content" do
      before do
        @buffer = StringIO.new
        @logger = Logger.new(@buffer)
        @listener = described_class.new(@logger, log_messages: false)
        @dispatched_msg = "2 messages to 2 topics were delegated to a dispatch queue"
        @listener.on_messages_produced_async(@event)
        @logged_data = nil
      end

      it { assert_includes(logged_data[0], @producer.id) }
      it { assert_includes(logged_data[0], "INFO") }
      it { assert_includes(logged_data[0], @dispatched_msg) }
      it { assert_nil(logged_data[1]) }
    end
  end

  describe "#on_messages_produced_sync" do
    before { @listener.on_messages_produced_sync(@event) }

    it { assert_includes(logged_data[0], @producer.id) }
    it { assert_includes(logged_data[0], "INFO") }
    it { assert_includes(logged_data[0], "Sync producing of 2 messages to 2 topics") }
    it { assert_includes(logged_data[1], @producer.id) }
    it { assert_includes(logged_data[1], "DEBUG") }
    it { assert_includes(logged_data[1], @message.to_s) }

    describe "when we do not want to log messages content" do
      before do
        @buffer = StringIO.new
        @logger = Logger.new(@buffer)
        @listener = described_class.new(@logger, log_messages: false)
        @listener.on_messages_produced_sync(@event)
        @logged_data = nil
      end

      it { assert_includes(logged_data[0], @producer.id) }
      it { assert_includes(logged_data[0], "INFO") }
      it { assert_includes(logged_data[0], "Sync producing of 2 messages to 2 topics") }
      it { assert_nil(logged_data[1]) }
    end
  end

  describe "#on_message_buffered" do
    before { @listener.on_message_buffered(@event) }

    it { assert_includes(logged_data[0], @producer.id) }
    it { assert_includes(logged_data[0], "INFO") }
    it { assert_includes(logged_data[0], "Buffering of a message to ") }
    it { assert_includes(logged_data[0], @message[:topic]) }
    it { assert_includes(logged_data[1], @producer.id) }
    it { assert_includes(logged_data[1], "DEBUG") }
    it { assert_includes(logged_data[1], @message.to_s) }

    describe "when we do not want to log messages content" do
      before do
        @buffer = StringIO.new
        @logger = Logger.new(@buffer)
        @listener = described_class.new(@logger, log_messages: false)
        @listener.on_message_buffered(@event)
        @logged_data = nil
      end

      it { assert_includes(logged_data[0], @producer.id) }
      it { assert_includes(logged_data[0], "INFO") }
      it { assert_includes(logged_data[0], "Buffering of a message to ") }
      it { assert_includes(logged_data[0], @message[:topic]) }
      it { assert_nil(logged_data[1]) }
    end
  end

  describe "#on_messages_buffered" do
    before { @listener.on_messages_buffered(@event) }

    it { assert_includes(logged_data[0], @producer.id) }
    it { assert_includes(logged_data[0], "INFO") }
    it { assert_includes(logged_data[0], "Buffering of 2 messages ") }
    it { assert_includes(logged_data[1], @producer.id) }
    it { assert_includes(logged_data[1], "DEBUG") }
    it { assert_includes(logged_data[1], @message.to_s) }

    describe "when we do not want to log messages content" do
      before do
        @buffer = StringIO.new
        @logger = Logger.new(@buffer)
        @listener = described_class.new(@logger, log_messages: false)
        @listener.on_messages_buffered(@event)
        @logged_data = nil
      end

      it { assert_includes(logged_data[0], @producer.id) }
      it { assert_includes(logged_data[0], "INFO") }
      it { assert_includes(logged_data[0], "Buffering of 2 messages ") }
      it { assert_nil(logged_data[1]) }
    end
  end

  describe "#on_buffer_flushed_async" do
    before { @listener.on_buffer_flushed_async(@event) }

    it { assert_includes(logged_data[0], @producer.id) }
    it { assert_includes(logged_data[0], "INFO") }
    it { assert_includes(logged_data[0], "Async flushing of 2 messages from the buffer") }
    it { assert_includes(logged_data[1], @producer.id) }
    it { assert_includes(logged_data[1], "DEBUG") }
    it { assert_includes(logged_data[1], @message.to_s) }
    it { assert_includes(logged_data[1], @messages[0].to_s) }
    it { assert_includes(logged_data[1], @messages[1].to_s) }

    describe "when we do not want to log messages content" do
      before do
        @buffer = StringIO.new
        @logger = Logger.new(@buffer)
        @listener = described_class.new(@logger, log_messages: false)
        @listener.on_buffer_flushed_async(@event)
        @logged_data = nil
      end

      it { assert_includes(logged_data[0], @producer.id) }
      it { assert_includes(logged_data[0], "INFO") }
      it { assert_includes(logged_data[0], "Async flushing of 2 messages from the buffer") }
      it { assert_nil(logged_data[1]) }
    end
  end

  describe "#on_buffer_flushed_sync" do
    before { @listener.on_buffer_flushed_sync(@event) }

    it { assert_includes(logged_data[0], @producer.id) }
    it { assert_includes(logged_data[0], "INFO") }
    it { assert_includes(logged_data[0], "Sync flushing of 2 messages from the buffer") }
    it { assert_includes(logged_data[1], @producer.id) }
    it { assert_includes(logged_data[1], "DEBUG") }
    it { assert_includes(logged_data[1], @messages[0].to_s) }
    it { assert_includes(logged_data[1], @messages[1].to_s) }

    describe "when we do not want to log messages content" do
      before do
        @buffer = StringIO.new
        @logger = Logger.new(@buffer)
        @listener = described_class.new(@logger, log_messages: false)
        @listener.on_buffer_flushed_sync(@event)
        @logged_data = nil
      end

      it { assert_includes(logged_data[0], @producer.id) }
      it { assert_includes(logged_data[0], "INFO") }
      it { assert_includes(logged_data[0], "Sync flushing of 2 messages from the buffer") }
      it { assert_nil(logged_data[1]) }
    end
  end

  describe "#on_buffer_purged" do
    before { @listener.on_buffer_purged(@event) }

    it { assert_includes(logged_data[0], @producer.id) }
    it { assert_includes(logged_data[0], "INFO") }
    it { assert_includes(logged_data[0], "Successfully purging buffer") }
    it { assert_nil(logged_data[1]) }
  end

  describe "#on_producer_closing" do
    before { @listener.on_producer_closing(@event) }

    it { assert_includes(logged_data[0], @producer.id) }
    it { assert_includes(logged_data[0], "INFO") }
    it { assert_includes(logged_data[0], "Closing producer") }
  end

  describe "#on_producer_closed" do
    before { @listener.on_producer_closed(@event) }

    it { assert_includes(logged_data[0], @producer.id) }
    it { assert_includes(logged_data[0], "INFO") }
    it { assert_includes(logged_data[0], "Closing producer") }
  end

  describe "#on_producer_disconnecting" do
    before { @listener.on_producer_disconnecting(@event) }

    it { assert_includes(logged_data[0], @producer.id) }
    it { assert_includes(logged_data[0], "INFO") }
    it { assert_includes(logged_data[0], "Disconnecting producer") }
  end

  describe "#on_producer_disconnected" do
    before { @listener.on_producer_disconnected(@event) }

    it { assert_includes(logged_data[0], @producer.id) }
    it { assert_includes(logged_data[0], "INFO") }
    it { assert_includes(logged_data[0], "Disconnected producer") }
  end

  describe "#on_producer_reloaded" do
    before { @listener.on_producer_reloaded(@event) }

    it { assert_includes(logged_data[0], @producer.id) }
    it { assert_includes(logged_data[0], "INFO") }
    it { assert_includes(logged_data[0], "Producer successfully reloaded") }
  end

  describe "#on_error_occurred" do
    before do
      @details[:type] = "error.type"
      @listener.on_error_occurred(@event)
    end

    it { assert_includes(logged_data[0], @producer.id) }
    it { assert_includes(logged_data[0], "ERROR") }
    it { assert_includes(logged_data[0], "Error occurred") }
  end

  describe "#on_transaction_started" do
    before { @listener.on_transaction_started(@event) }

    it { assert_includes(logged_data[0], @producer.id) }
    it { assert_includes(logged_data[0], "INFO") }
    it { assert_includes(logged_data[0], "Starting transaction") }
  end

  describe "#on_transaction_aborted" do
    before { @listener.on_transaction_aborted(@event) }

    it { assert_includes(logged_data[0], @producer.id) }
    it { assert_includes(logged_data[0], "INFO") }
    it { assert_includes(logged_data[0], "Aborting transaction") }
  end

  describe "#on_transaction_committed" do
    before { @listener.on_transaction_committed(@event) }

    it { assert_includes(logged_data[0], @producer.id) }
    it { assert_includes(logged_data[0], "INFO") }
    it { assert_includes(logged_data[0], "Committing transaction") }
  end

  describe "#on_transaction_marked_as_consumed" do
    before do
      test_message = Struct.new(:topic, :partition, :offset, keyword_init: true)

      @details[:message] = test_message.new(
        topic: rand.to_s,
        partition: 0,
        offset: 100
      )

      @listener.on_transaction_marked_as_consumed(@event)
    end

    it { assert_includes(logged_data[0], @producer.id) }
    it { assert_includes(logged_data[0], "INFO") }
    it { assert_includes(logged_data[0], "Marking message") }
  end
end
