# frozen_string_literal: true

describe_current do
  before do
    @producer = build(:producer)
  end

  after { @producer.close }

  describe "#produce_async" do
    context "when message is invalid" do
      before do
        @message = build(:invalid_message)
      end

      it { assert_raises(WaterDrop::Errors::MessageInvalidError) { @producer.produce_async(@message) } }
    end

    context "when message is valid" do
      before do
        @message = build(:valid_message)
      end

      it { assert_kind_of(Rdkafka::Producer::DeliveryHandle, @producer.produce_async(@message)) }
    end

    context "when message is valid with array headers" do
      before do
        @message = build(:valid_message, headers: { "a" => %w[b c] })
      end

      it { assert_kind_of(Rdkafka::Producer::DeliveryHandle, @producer.produce_async(@message)) }
    end

    context "when message is valid and with label" do
      before do
        @message = build(:valid_message, label: "test")
      end

      it { assert_equal("test", @producer.produce_async(@message).label) }
    end

    context "when sending a tombstone message" do
      before do
        @message = build(:valid_message, payload: nil)
      end

      it { assert_kind_of(Rdkafka::Producer::DeliveryHandle, @producer.produce_async(@message)) }
    end

    context "when producing with good middleware" do
      before do
        @message = build(:valid_message, payload: nil)

        @producer.produce_sync(topic: @message[:topic], payload: nil)

        mid = lambda do |msg|
          msg[:partition_key] = "1"
          msg
        end

        @producer.middleware.append mid
      end

      it { assert_kind_of(Rdkafka::Producer::DeliveryHandle, @producer.produce_async(@message)) }
    end

    context "when producing with corrupted middleware" do
      before do
        @message = build(:valid_message, payload: nil)

        mid = lambda do |msg|
          msg[:partition_key] = -1
          msg
        end

        @producer.middleware.append mid
      end

      it { assert_raises(WaterDrop::Errors::MessageInvalidError) { @producer.produce_async(@message) } }
    end

    context "when inline error occurs in librdkafka and we do not retry on queue full" do
      before do
        @errors = []
        @occurred = []
        @producer = build(:limited_producer)

        @producer.monitor.subscribe("error.occurred") do |event|
          # Avoid side effects
          event.payload[:error] = event[:error].dup
          @occurred << event
        end

        begin
          message = build(:valid_message)
          100.times { @producer.produce_async(message) }
        rescue WaterDrop::Errors::ProduceError => e
          @errors << e
        end

        @error = @errors.first
      end

      it { assert_kind_of(WaterDrop::Errors::ProduceError, @error) }
      it { assert_equal(@error.cause.inspect, @error.message) }
      it { assert_kind_of(Rdkafka::RdkafkaError, @error.cause) }
      it { assert_kind_of(Rdkafka::RdkafkaError, @occurred.first.payload[:error].cause) }
      it { assert_equal("message.produce_async", @occurred.first.payload[:type]) }
    end

    context "when inline error occurs in librdkafka and we retry on queue full" do
      before do
        @errors = []
        @occurred = []
        @producer = build(:slow_producer, wait_on_queue_full: true)

        @producer.config.wait_on_queue_full = true

        @producer.monitor.subscribe("error.occurred") do |event|
          @occurred << event
        end

        begin
          message = build(:valid_message, label: "test")
          5.times { @producer.produce_async(message) }
        rescue WaterDrop::Errors::ProduceError => e
          @errors << e
        end
      end

      it { assert_empty(@errors) }
      it { assert_kind_of(Rdkafka::RdkafkaError, @occurred.first.payload[:error].cause) }
      it { assert_equal("message.produce_async", @occurred.first.payload[:type]) }
      it { assert_nil(@occurred.first.payload[:label]) }
    end

    context "when linger is longer than shutdown" do
      before do
        @occurred = []

        while @occurred.empty?
          # On fast CPUs we may actually be fast enough to dispatch it to local kafka with ack
          # within 1ms and no errors will occur. That's why we do repeat it
          producer = build(
            :slow_producer,
            kafka: {
              "bootstrap.servers": BOOTSTRAP_SERVERS,
              "queue.buffering.max.ms": 0,
              "message.timeout.ms": 1
            }
          )

          producer.monitor.subscribe("error.occurred") do |event|
            @occurred << event
          end

          message = build(:valid_message, label: "test")
          100.times { producer.produce_async(message) }
          producer.close
        end

        @error = @occurred.first[:error]
      end

      it { refute_empty(@occurred) }
      it { assert_kind_of(Rdkafka::RdkafkaError, @error) }
      # On slower systems (e.g., macOS CI), we may get :transport instead of :msg_timed_out
      it { assert_includes(%i[msg_timed_out transport], @error.code) }
    end

    context "when inline error occurs and we retry on queue full but instrumentation off" do
      before do
        @errors = []
        @occurred = []
        @producer = build(:slow_producer, wait_on_queue_full: true)

        @producer.config.wait_on_queue_full = true
        @producer.config.instrument_on_wait_queue_full = false

        @producer.monitor.subscribe("error.occurred") do |event|
          @occurred << event
        end

        begin
          message = build(:valid_message, label: "test")
          5.times { @producer.produce_async(message) }
        rescue WaterDrop::Errors::ProduceError => e
          @errors << e
        end
      end

      it { assert_empty(@errors) }
      it { assert_empty(@occurred) }
    end

    context "when inline error occurs in librdkafka and we go beyond max wait on queue full" do
      before do
        @errors = []
        @occurred = []
        @producer = build(
          :slow_producer,
          wait_on_queue_full: true,
          wait_timeout_on_queue_full: 0.5
        )

        @producer.config.wait_on_queue_full = true

        @producer.monitor.subscribe("error.occurred") do |event|
          @occurred << event
        end

        begin
          message = build(:valid_message, label: "test")
          5.times { @producer.produce_async(message) }
        rescue WaterDrop::Errors::ProduceError => e
          @errors << e
        end
      end

      it { refute_empty(@errors) }
      it { assert_kind_of(Rdkafka::RdkafkaError, @occurred.first.payload[:error].cause) }
      it { assert_equal("message.produce_async", @occurred.first.payload[:type]) }
      it { assert_nil(@occurred.first.payload[:label]) }
    end
  end

  describe "#produce_many_async" do
    context "when we have several invalid messages" do
      before do
        @messages = Array.new(10) { build(:invalid_message) }
      end

      it { assert_raises(WaterDrop::Errors::MessageInvalidError) { @producer.produce_many_async(@messages) } }
    end

    context "when the last message out of a batch is invalid" do
      before do
        @messages = [build(:valid_message), build(:invalid_message)]
      end

      it { assert_raises(WaterDrop::Errors::MessageInvalidError) { @producer.produce_many_async(@messages) } }
    end

    context "when we have several valid messages" do
      before do
        @messages = Array.new(10) { build(:valid_message) }
      end

      it "expect all the results to be delivery handles" do
        @producer.produce_many_async(@messages).each do |e|
          assert_kind_of(Rdkafka::Producer::DeliveryHandle, e)
        end
      end
    end

    context "when inline error occurs in librdkafka" do
      before do
        @errors = []
        @messages = Array.new(100) { build(:valid_message) }
        @producer = build(:limited_producer)

        begin
          # Intercept the error so it won't bubble up as we want to check the notifications pipeline
          @producer.produce_many_async(@messages)
        rescue WaterDrop::Errors::ProduceError => e
          @errors << e
        end

        @error = @errors.first
      end

      it { assert_equal(1, @error.dispatched.size) }
      it { assert_kind_of(Rdkafka::Producer::DeliveryHandle, @error.dispatched.first) }
      it { assert_kind_of(WaterDrop::Errors::ProduceError, @error) }
      it { assert_equal(@error.cause.inspect, @error.message) }
      it { assert_kind_of(Rdkafka::RdkafkaError, @error.cause) }
    end

    context "when there are dispatched messages not in kafka yet" do
      before do
        @producer = build(
          :slow_producer,
          kafka: {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "queue.buffering.max.ms": 5_000,
            "queue.buffering.max.messages": 2_000
          }
        )
      end

      it "expect not to allow for disconnect" do
        refute(@producer.disconnect)
      end

      it "expect to allow disconnect after they are dispatched" do
        message = build(:valid_message, label: "test")
        dispatched = Array.new(1_000) { @producer.produce_async(message) }
        dispatched.each(&:wait)

        assert(@producer.disconnect)
      end
    end
  end

  describe "fatal error testing with produce_async" do
    before do
      @producer = build(
        :idempotent_producer,
        reload_on_idempotent_fatal_error: true,
        max_attempts_on_idempotent_fatal_error: 3,
        wait_backoff_on_idempotent_fatal_error: 100
      )
      @topic_name = generate_topic
      @message = build(:valid_message, topic: @topic_name)
      @producer.singleton_class.include(WaterDrop::Producer::Testing)
    end

    context "when producing after fatal error is triggered" do
      it "detects fatal error state and recovers via reload on subsequent produce_async" do
        # First verify producer works
        handle = @producer.produce_async(@message)

        assert_kind_of(Rdkafka::Producer::DeliveryHandle, handle)
        report = handle.wait

        assert_nil(report.error)

        # Trigger a fatal error
        @producer.trigger_test_fatal_error(47, "Fatal error for produce_async test")

        # Verify fatal error is present
        fatal_error = @producer.fatal_error

        refute_nil(fatal_error)
        assert_equal(47, fatal_error[:error_code])

        # Now try to produce after fatal error - should succeed after reload
        handle = @producer.produce_async(@message)

        assert_kind_of(Rdkafka::Producer::DeliveryHandle, handle)
        report = handle.wait

        assert_nil(report.error)
      end

      it "can produce async successfully before fatal error injection" do
        handles = []

        # Produce multiple messages asynchronously
        5.times do
          handle = @producer.produce_async(@message)

          assert_kind_of(Rdkafka::Producer::DeliveryHandle, handle)
          handles << handle
        end

        # Wait for all deliveries
        handles.each do |handle|
          report = handle.wait

          assert_nil(report.error)
        end

        # Verify no fatal error before injection
        assert_nil(@producer.fatal_error)
      end
    end

    context "when fatal error occurs during async operations" do
      it "maintains fatal error state across multiple queries" do
        # Trigger fatal error
        @producer.trigger_test_fatal_error(64, "Async fatal error state test")

        # Query fatal error multiple times
        first = @producer.fatal_error
        second = @producer.fatal_error
        third = @producer.fatal_error

        assert_equal(first, second)
        assert_equal(second, third)
        assert_equal(64, first[:error_code])
      end
    end
  end

  describe "fatal error testing with produce_many_async" do
    before do
      @producer = build(
        :idempotent_producer,
        reload_on_idempotent_fatal_error: true,
        max_attempts_on_idempotent_fatal_error: 3,
        wait_backoff_on_idempotent_fatal_error: 100
      )
      @topic_name = generate_topic
      @messages = Array.new(3) { build(:valid_message, topic: @topic_name) }
      @producer.singleton_class.include(WaterDrop::Producer::Testing)
    end

    context "when producing batch after fatal error is triggered" do
      it "detects fatal error state and recovers via reload on subsequent produce_many_async" do
        # First verify producer works with async batches
        handles = @producer.produce_many_async(@messages)

        assert_kind_of(Array, handles)
        assert_equal(3, handles.size)

        # Wait for all deliveries
        reports = handles.map(&:wait)

        reports.each do |report|
          assert_nil(report.error)
        end

        # Trigger a fatal error
        @producer.trigger_test_fatal_error(47, "Fatal error for produce_many_async test")

        # Verify fatal error is present
        fatal_error = @producer.fatal_error

        refute_nil(fatal_error)
        assert_equal(47, fatal_error[:error_code])

        # Now try to produce batch after fatal error - should succeed after reload
        handles = @producer.produce_many_async(@messages)

        assert_kind_of(Array, handles)
        assert_equal(3, handles.size)

        # All deliveries should succeed
        reports = handles.map(&:wait)

        reports.each do |report|
          assert_nil(report.error)
        end
      end

      it "can produce async batches successfully before fatal error injection" do
        all_handles = []

        # Produce multiple batches asynchronously
        3.times do
          handles = @producer.produce_many_async(@messages)

          assert_equal(3, handles.size)
          all_handles.concat(handles)
        end

        # Wait for all deliveries
        all_handles.each do |handle|
          report = handle.wait

          assert_nil(report.error)
        end

        # Verify no fatal error before injection
        assert_nil(@producer.fatal_error)
      end
    end

    context "when testing async batch operations with various sizes" do
      it "handles different batch sizes before fatal error" do
        # Small async batch
        small_batch = [build(:valid_message, topic: @topic_name)]
        handles = @producer.produce_many_async(small_batch)

        assert_equal(1, handles.size)
        handles.each { |h| assert_nil(h.wait.error) }

        # Medium async batch
        medium_batch = Array.new(5) { build(:valid_message, topic: @topic_name) }
        handles = @producer.produce_many_async(medium_batch)

        assert_equal(5, handles.size)
        handles.each { |h| assert_nil(h.wait.error) }

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
      @topic_name = generate_topic
      @message = build(:valid_message, topic: @topic_name)
      @messages = Array.new(3) { build(:valid_message, topic: @topic_name) }
      @reload_events = []
      @reloaded_events = []

      @producer.singleton_class.include(WaterDrop::Producer::Testing)
      @producer.monitor.subscribe("producer.reload") { |event| @reload_events << event }
      @producer.monitor.subscribe("producer.reloaded") { |event| @reloaded_events << event }
    end

    context "when produce_async is called after fatal error without reload" do
      it "raises error consistently without attempting reload" do
        # Trigger fatal error
        @producer.trigger_test_fatal_error(47, "No reload async test")

        # Multiple produce_async attempts should all fail with fatal error
        3.times do
          error = assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_async(@message) }
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

    context "when produce_many_async is called after fatal error without reload" do
      it "raises error consistently without attempting reload" do
        # Trigger fatal error
        @producer.trigger_test_fatal_error(47, "No reload async batch test")

        # Multiple produce_many_async attempts should all fail with fatal error
        3.times do
          error = assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_many_async(@messages) }
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
