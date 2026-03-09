# frozen_string_literal: true

describe_current do
  before do
    @producer = build(:producer)
    @timeout_ms = 1000 # 1 second for fast tests
    @disconnected_events = []
    @topic_name = "it-#{SecureRandom.uuid}"
    @listener = described_class.new(@producer, disconnect_timeout: @timeout_ms)

    @producer.monitor.subscribe("producer.disconnected") do |event|
      @disconnected_events << event
    end

    # Initialize producer connection
    2.times { @producer.produce_sync(topic: @topic_name, payload: "msg") }
  end

  after { @producer.close }

  describe "#call" do
    before do
      @statistics = { "txmsgs" => 100 }
      @event = { statistics: @statistics }
    end

    describe "when messages are being transmitted" do
      it "expect not to disconnect producer" do
        @listener.on_statistics_emitted(@event)

        assert_empty(@disconnected_events)
      end

      describe "with increasing message count" do
        it "expect not to disconnect producer" do
          @listener.on_statistics_emitted({ statistics: { "txmsgs" => 50 } })
          @listener.on_statistics_emitted({ statistics: { "txmsgs" => 100 } })
          @listener.on_statistics_emitted({ statistics: { "txmsgs" => 150 } })

          assert_empty(@disconnected_events)
        end
      end
    end

    describe "when no new messages are being transmitted" do
      before do
        @old_time = @listener.send(:monotonic_now) - @timeout_ms - 100
        # Set up listener with old activity time to simulate timeout
        @listener.instance_variable_set(:@last_activity_time, @old_time)
        @listener.instance_variable_set(:@last_txmsgs, 100)
      end

      describe "when producer can be disconnected" do
        it "expect to disconnect the producer" do
          @listener.on_statistics_emitted(@event)
          sleep(0.2) # a bit of time because disconnect happens async

          assert_equal(1, @disconnected_events.size)
        end

        it "expect to emit disconnect event with producer id" do
          @listener.on_statistics_emitted(@event)
          sleep(0.2) # a bit of time because disconnect happens async

          assert_equal(@producer.id, @disconnected_events.first[:producer_id])
        end

        it "expect not to disconnect again on subsequent calls with same txmsgs" do
          @listener.on_statistics_emitted(@event)
          @listener.on_statistics_emitted(@event)

          sleep(0.2) # a bit of time because disconnect happens async

          assert_equal(1, @disconnected_events.size)
        end
      end

      describe "when producer is not disconnectable" do
        before do
          # Mock disconnectable? to return false (producer busy with transaction, operations, etc)
          @original_disconnectable = @producer.method(:disconnectable?)
        end

        it "expect not to attempt disconnect at all" do
          @producer.stubs(:disconnectable?).returns(false)
          @producer.expects(:disconnect).never
          @listener.on_statistics_emitted(@event)

          assert_empty(@disconnected_events)
        end

        it "expect to still reset activity time" do
          old_activity_time = @listener.instance_variable_get(:@last_activity_time)
          @producer.stubs(:disconnectable?).returns(false)
          @producer.stubs(:disconnect).returns(nil)
          @listener.on_statistics_emitted(@event)
          new_activity_time = @listener.instance_variable_get(:@last_activity_time)

          assert_operator(new_activity_time, :>, old_activity_time)
        end
      end

      describe "when disconnect fails with an error in the thread" do
        before do
          @error_events = []
          @test_error = StandardError.new("disconnect failed")

          @producer.monitor.subscribe("error.occurred") do |event|
            @error_events << event
          end
        end

        it "expect to handle error gracefully and instrument it" do
          @producer.stubs(:disconnectable?).returns(true)
          @producer.stubs(:disconnect).raises(@test_error)
          @listener.on_statistics_emitted(@event)
          sleep(0.1) # Give thread time to complete and handle error

          assert_empty(@disconnected_events)
          refute_empty(@error_events)
          assert_equal("producer.disconnect.error", @error_events.first[:type])
          assert_equal(@test_error, @error_events.first[:error])
          assert_equal(@producer.id, @error_events.first[:producer_id])
        end

        it "expect to still reset activity time even after error" do
          old_activity_time = @listener.instance_variable_get(:@last_activity_time)
          @producer.stubs(:disconnectable?).returns(true)
          @producer.stubs(:disconnect).raises(@test_error)
          @listener.on_statistics_emitted(@event)
          sleep(0.1) # Give thread time to complete
          new_activity_time = @listener.instance_variable_get(:@last_activity_time)

          assert_operator(new_activity_time, :>, old_activity_time)
        end
      end
    end

    describe "when timeout has not been exceeded" do
      before do
        @recent_time = @listener.send(:monotonic_now) - (@timeout_ms / 2)
        @listener.instance_variable_set(:@last_activity_time, @recent_time)
        @listener.instance_variable_set(:@last_txmsgs, 100)
      end

      it "expect not to disconnect producer" do
        @listener.on_statistics_emitted(@event)

        assert_empty(@disconnected_events)
      end
    end

    describe "when statistics contain no txmsgs field" do
      before do
        @statistics = {}
        @event = { statistics: @statistics }
      end

      it "expect to handle missing txmsgs gracefully" do
        @listener.on_statistics_emitted(@event)
      end

      it "expect to treat as 0 messages" do
        # Should detect activity change from -1 to 0
        @listener.on_statistics_emitted(@event)

        assert_empty(@disconnected_events)
      end
    end
  end

  describe "#on_statistics_emitted" do
    before do
      @event = { statistics: { "txmsgs" => 150 } }
      @call_args = []
    end

    it "expect to delegate to call method with statistics" do
      @listener.stubs(:call).with do |stats|
        @call_args << stats
        true
      end
      @listener.on_statistics_emitted(@event)

      assert_equal([{ "txmsgs" => 150 }], @call_args)
    end
  end

  describe "integration with producer monitoring" do
    before do
      @events = []

      @producer.monitor.subscribe(@listener)

      @producer.monitor.subscribe("statistics.emitted") do |event|
        @events << event
      end
    end

    it "expect to respond to statistics events" do
      assert_respond_to(@listener, :on_statistics_emitted)
    end

    describe "with default timeout" do
      before do
        @long_timeout_listener = described_class.new(@producer)
      end

      it "expect to use 5 minute default timeout" do
        # This is hard to test behaviorally, but we can verify it doesn't disconnect immediately
        @long_timeout_listener.on_statistics_emitted({ statistics: { "txmsgs" => 100 } })
        # Same count, no activity
        @long_timeout_listener.on_statistics_emitted({ statistics: { "txmsgs" => 100 } })

        assert_empty(@disconnected_events)
      end
    end
  end
end
