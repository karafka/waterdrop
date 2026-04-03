# frozen_string_literal: true

describe_current do
  before do
    @producer = build(:producer)
    @topic_name = generate_topic
    create_topic(@topic_name)
  end

  after { @producer.close }

  describe "#produce_sync" do
    context "when message is invalid" do
      before do
        @message = build(:invalid_message)
      end

      it { assert_raises(WaterDrop::Errors::MessageInvalidError) { @producer.produce_sync(@message) } }
    end

    context "when message is valid" do
      before do
        @message = build(:valid_message)
      end

      it { assert_kind_of(Rdkafka::Producer::DeliveryReport, @producer.produce_sync(@message)) }
    end

    context "when message has array headers" do
      before do
        @message = build(:valid_message, headers: { "a" => %w[b c] })
      end

      it { assert_kind_of(Rdkafka::Producer::DeliveryReport, @producer.produce_sync(@message)) }
    end

    context "when message has invalid headers" do
      before do
        @message = build(:valid_message, headers: { "a" => %i[b c] })
      end

      it { assert_raises(WaterDrop::Errors::MessageInvalidError) { @producer.produce_sync(@message) } }
    end

    context "when message is valid and with label" do
      before do
        @message = build(:valid_message, label: "test")
      end

      it { assert_equal("test", @producer.produce_sync(@message).label) }
    end

    context "when producing with topic as a symbol" do
      before do
        @message = build(:valid_message)
        @message[:topic] = @message[:topic].to_sym
      end

      it { assert_kind_of(Rdkafka::Producer::DeliveryReport, @producer.produce_sync(@message)) }
    end

    context "when producing sync to an unreachable cluster" do
      before do
        @message = build(:valid_message)
        @producer = build(:unreachable_producer)
      end

      it "expect to raise final error" do
        error = assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_sync(@message) }
        assert_match(/msg_timed_out/, error.message)
      end
    end

    context "when producing sync to a topic that does not exist with partition_key" do
      before do
        @message = build(:valid_message, partition_key: "test", key: "test")
      end

      it "expect not to raise error and work correctly as the topic should be created" do
        assert_kind_of(Rdkafka::Producer::DeliveryReport, @producer.produce_sync(@message))
      end
    end

    context "when allow.auto.create.topics is set to false" do
      before do
        @message = build(:valid_message)
        @producer = build(
          :producer,
          kafka: {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "allow.auto.create.topics": false,
            "message.timeout.ms": 500
          }
        )
      end

      it "expect to raise final error" do
        error = assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_sync(@message) }
        assert_match(/msg_timed_out/, error.message)
      end
    end

    context "when allow.auto.create.topics is set to false and we use partition key" do
      before do
        @message = build(:valid_message, partition_key: "test", key: "test")
        @producer = build(
          :producer,
          kafka: {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "allow.auto.create.topics": false,
            "message.timeout.ms": 500
          }
        )
      end

      it "expect to raise final error" do
        error = assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_sync(@message) }
        assert_match(/msg_timed_out/, error.message)
      end
    end

    context "when inline error occurs in librdkafka" do
      before do
        @errors = []
        @occurred = []
        @producer = build(:limited_producer)

        @producer.monitor.subscribe("error.occurred") do |event|
          # Avoid side effects
          event.payload[:error] = event[:error].dup
          @occurred << event
        end

        message = build(:valid_message, label: "test")
        create_topic(message[:topic])
        threads = Array.new(20) do
          Thread.new do
            @producer.produce_sync(message)
          rescue => e
            @errors << e
          end
        end

        threads.each(&:join)
        @error = @errors.first
      end

      it { assert_kind_of(WaterDrop::Errors::ProduceError, @error) }
      it { assert_equal(@error.cause.inspect, @error.message) }
      it { assert_kind_of(Rdkafka::RdkafkaError, @error.cause) }
      it { assert_kind_of(Rdkafka::RdkafkaError, @occurred.first.payload[:error].cause) }
      it { assert_equal("message.produce_sync", @occurred.first.payload[:type]) }
      # We expect this to be nil because the error was raised by the code that was attempting to
      # produce, hence there is a chance of not even having a handler
      it { assert_nil(@occurred.first.payload[:label]) }
    end
  end

  describe "#produce_sync with partition key" do
    before do
      @message = build(:valid_message, partition_key: rand.to_s, topic: @topic_name)
      @producer.produce_sync(topic: @topic_name, payload: "1")
    end

    it { assert_kind_of(Rdkafka::Producer::DeliveryReport, @producer.produce_sync(@message)) }
  end

  describe "#produce_many_sync" do
    context "when we have several invalid messages" do
      before do
        @messages = Array.new(10) { build(:invalid_message) }
      end

      it { assert_raises(WaterDrop::Errors::MessageInvalidError) { @producer.produce_many_sync(@messages) } }
    end

    context "when the last message out of a batch is invalid" do
      before do
        @messages = [build(:valid_message), build(:invalid_message)]
      end

      it { assert_raises(WaterDrop::Errors::MessageInvalidError) { @producer.produce_many_sync(@messages) } }
    end

    context "when we have several valid messages" do
      before do
        @messages = Array.new(10) { build(:valid_message) }
      end

      it "expect all the results to be delivery handles" do
        @producer.produce_many_sync(@messages).each do |e|
          assert_kind_of(Rdkafka::Producer::DeliveryHandle, e)
        end
      end
    end

    context "when we have several valid messages with array headers" do
      before do
        @messages = Array.new(10) { build(:valid_message, headers: { "a" => %w[b c] }) }
      end

      it "expect all the results to be delivery handles" do
        @producer.produce_many_sync(@messages).each do |e|
          assert_kind_of(Rdkafka::Producer::DeliveryHandle, e)
        end
      end
    end

    context "when producing to multiple topics with invalid partition key" do
      before do
        @topic1 = @topic_name
        @topic2 = "#{@topic1}-2"
        create_topic(@topic2)

        @messages = [
          { topic: @topic1, payload: "message1", partition: 0 },
          { topic: @topic1, payload: "message2", partition: 0 },
          { topic: @topic2, payload: "message3", partition: 1 },
          { topic: @topic2, payload: "message4", partition: 0 },
          { topic: @topic2, payload: "message5", partition: 0 }
        ]

        @producer.produce_sync(topic: @topic1, payload: "setup1", partition: 0)
        @producer.produce_sync(topic: @topic2, payload: "setup2", partition: 0)
      end

      it "expect to raise unknown partition error" do
        error = assert_raises(WaterDrop::Errors::ProduceManyError) { @producer.produce_many_sync(@messages) }
        assert_match(/unknown_partition/, error.message)
      end
    end
  end

  context "when using compression.codec" do
    context "when it is gzip" do
      before do
        @producer = build(
          :producer,
          kafka: {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "compression.codec": "gzip"
          }
        )
        @message = build(:valid_message)
      end

      it { assert_kind_of(Rdkafka::Producer::DeliveryReport, @producer.produce_sync(@message)) }
    end

    context "when it is installed zstd" do
      before do
        @producer = build(
          :producer,
          kafka: {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "compression.codec": "zstd"
          }
        )
        @message = build(:valid_message)
      end

      it { assert_kind_of(Rdkafka::Producer::DeliveryReport, @producer.produce_sync(@message)) }
    end

    context "when it is installed lz4" do
      before do
        @producer = build(
          :producer,
          kafka: {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "compression.codec": "lz4"
          }
        )
        @message = build(:valid_message)
      end

      it { assert_kind_of(Rdkafka::Producer::DeliveryReport, @producer.produce_sync(@message)) }
    end

    context "when it is installed snappy" do
      before do
        @producer = build(
          :producer,
          kafka: {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "compression.codec": "snappy"
          }
        )
        @message = build(:valid_message)
      end

      it { assert_kind_of(Rdkafka::Producer::DeliveryReport, @producer.produce_sync(@message)) }
    end
  end

  context "when producing and disconnected in a loop" do
    before do
      @message = build(:valid_message)
    end

    it "expect to always disconnect and reconnect and continue to produce" do
      100.times do |i|
        assert_equal(i, @producer.produce_sync(@message).offset)
        @producer.disconnect
      end
    end
  end

  describe "fatal error testing with produce_sync" do
    before do
      @producer = build(
        :idempotent_producer,
        reload_on_idempotent_fatal_error: true,
        max_attempts_on_idempotent_fatal_error: 3,
        wait_backoff_on_idempotent_fatal_error: 100
      )
      @message = build(:valid_message, topic: @topic_name)
      @producer.singleton_class.include(WaterDrop::Producer::Testing)
    end

    context "when producing after fatal error is triggered" do
      it "detects fatal error state and recovers via reload on subsequent produce_sync" do
        # First verify producer works
        report = @producer.produce_sync(@message)

        assert_kind_of(Rdkafka::Producer::DeliveryReport, report)
        assert_nil(report.error)

        # Trigger a fatal error
        @producer.trigger_test_fatal_error(47, "Fatal error for produce_sync test")

        # Verify fatal error is present
        fatal_error = @producer.fatal_error

        refute_nil(fatal_error)
        assert_equal(47, fatal_error[:error_code])

        # Now try to produce after fatal error - should succeed after reload
        report = @producer.produce_sync(@message)

        assert_kind_of(Rdkafka::Producer::DeliveryReport, report)
        assert_nil(report.error)
      end

      it "can produce successfully before fatal error injection" do
        # Produce multiple messages successfully
        5.times do
          report = @producer.produce_sync(@message)

          assert_kind_of(Rdkafka::Producer::DeliveryReport, report)
          assert_nil(report.error)
        end

        # Verify no fatal error before injection
        assert_nil(@producer.fatal_error)
      end

      it "multiple produce_sync calls succeed after fatal error via reload" do
        # Trigger fatal error
        @producer.trigger_test_fatal_error(47, "Multiple calls test")

        # Multiple attempts should all succeed after reload
        3.times do
          report = @producer.produce_sync(@message)

          assert_kind_of(Rdkafka::Producer::DeliveryReport, report)
          assert_nil(report.error)
        end
      end
    end
  end

  describe "fatal error testing with produce_many_sync" do
    before do
      @producer = build(
        :idempotent_producer,
        reload_on_idempotent_fatal_error: true,
        max_attempts_on_idempotent_fatal_error: 3,
        wait_backoff_on_idempotent_fatal_error: 100
      )
      @messages = Array.new(3) { build(:valid_message, topic: @topic_name) }
      @producer.singleton_class.include(WaterDrop::Producer::Testing)
    end

    context "when producing batch after fatal error is triggered" do
      it "detects fatal error state and recovers via reload on subsequent produce_many_sync" do
        # First verify producer works with batches
        # produce_many_sync returns DeliveryHandles (already waited)
        handles = @producer.produce_many_sync(@messages)

        assert_kind_of(Array, handles)
        assert_equal(3, handles.size)
        handles.each { |h| assert_kind_of(Rdkafka::Producer::DeliveryHandle, h) }

        # Trigger a fatal error
        @producer.trigger_test_fatal_error(47, "Fatal error for produce_many_sync test")

        # Verify fatal error is present
        fatal_error = @producer.fatal_error

        refute_nil(fatal_error)
        assert_equal(47, fatal_error[:error_code])

        # Now try to produce batch after fatal error - should succeed after reload
        handles = @producer.produce_many_sync(@messages)

        assert_kind_of(Array, handles)
        assert_equal(3, handles.size)
        handles.each { |h| assert_kind_of(Rdkafka::Producer::DeliveryHandle, h) }
      end

      it "can produce batches successfully before fatal error injection" do
        # Produce multiple batches successfully
        3.times do
          handles = @producer.produce_many_sync(@messages)

          assert_equal(3, handles.size)
          handles.each { |h| assert_kind_of(Rdkafka::Producer::DeliveryHandle, h) }
        end

        # Verify no fatal error before injection
        assert_nil(@producer.fatal_error)
      end
    end

    context "when testing batch size variations with fatal error" do
      it "works with different batch sizes before fatal error" do
        # Small batch
        small_batch = [build(:valid_message, topic: @topic_name)]
        reports = @producer.produce_many_sync(small_batch)

        assert_equal(1, reports.size)

        # Medium batch
        medium_batch = Array.new(5) { build(:valid_message, topic: @topic_name) }
        reports = @producer.produce_many_sync(medium_batch)

        assert_equal(5, reports.size)

        # Large batch
        large_batch = Array.new(10) { build(:valid_message, topic: @topic_name) }
        reports = @producer.produce_many_sync(large_batch)

        assert_equal(10, reports.size)

        # No fatal error yet
        assert_nil(@producer.fatal_error)
      end
    end
  end

  describe "fatal error testing without reload enabled" do
    before do
      @producer = build(
        :idempotent_producer,
        reload_on_idempotent_fatal_error: false
      )
      @message = build(:valid_message, topic: @topic_name)
      @messages = Array.new(3) { build(:valid_message, topic: @topic_name) }
      @reload_events = []
      @reloaded_events = []

      @producer.singleton_class.include(WaterDrop::Producer::Testing)
      @producer.monitor.subscribe("producer.reload") { |event| @reload_events << event }
      @producer.monitor.subscribe("producer.reloaded") { |event| @reloaded_events << event }
    end

    context "when produce_sync is called after fatal error without reload" do
      it "raises error consistently without attempting reload" do
        # Trigger fatal error
        @producer.trigger_test_fatal_error(47, "No reload test")

        # Multiple produce_sync attempts should all fail with fatal error
        3.times do
          error = assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_sync(@message) }
          assert_kind_of(Rdkafka::RdkafkaError, error.cause)
          assert_predicate(error.cause, :fatal?)

          # Fatal error should persist after each attempt
          refute_nil(@producer.fatal_error)
          assert_equal(47, @producer.fatal_error[:error_code])
        end

        # No reload events should have been emitted
        assert_empty(@reload_events)
        assert_empty(@reloaded_events)
      end
    end

    context "when produce_many_sync is called after fatal error without reload" do
      it "raises error consistently without attempting reload" do
        # Trigger fatal error
        @producer.trigger_test_fatal_error(47, "No reload batch test")

        # Multiple produce_many_sync attempts should all fail with fatal error
        3.times do
          error = assert_raises(WaterDrop::Errors::ProduceManyError) { @producer.produce_many_sync(@messages) }
          assert_kind_of(Rdkafka::RdkafkaError, error.cause)
          assert_predicate(error.cause, :fatal?)

          # Fatal error should persist after each attempt
          refute_nil(@producer.fatal_error)
          assert_equal(47, @producer.fatal_error[:error_code])
        end

        # No reload events should have been emitted
        assert_empty(@reload_events)
        assert_empty(@reloaded_events)
      end
    end
  end
end
