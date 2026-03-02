# frozen_string_literal: true

require "test_helper"

describe_current do
  before do
    @producer = build(:producer)
  end

  after { @producer.close }

  describe "#idempotent?" do
    context "when producer is transactional" do
      before do
        @producer = build(:transactional_producer)
      end

      it "expect to return true as all transactional producers are idempotent" do
        assert_equal(true, @producer.idempotent?)
      end
    end

    context "when producer has enable.idempotence set to true" do
      before do
        @producer = build(:idempotent_producer)
      end

      it "expect to return true" do
        assert_equal(true, @producer.idempotent?)
      end
    end

    context "when producer has enable.idempotence set to false" do
      before do
        @producer = build(
          :producer,
          kafka: { "bootstrap.servers": BOOTSTRAP_SERVERS, "enable.idempotence": false }
        )
      end

      it "expect to return false" do
        assert_equal(false, @producer.idempotent?)
      end
    end

    context "when producer does not have enable.idempotence configured" do
      before do
        @producer = build(:producer)
      end

      it "expect to return false by default" do
        assert_equal(false, @producer.idempotent?)
      end
    end

    context "when idempotent? is called multiple times" do
      before do
        @producer = build(:idempotent_producer)
      end

      it "expect to cache the result" do
        first_result = @producer.idempotent?
        assert_equal(first_result, @producer.idempotent?)
        assert_equal(true, @producer.instance_variable_defined?(:@idempotent))
      end
    end
  end

  describe "fatal error handling" do
    before do
      @topic_name = "it-#{SecureRandom.uuid}"
      @message = build(:valid_message, topic: @topic_name)
    end

    context "when fatal error occurs with reload_on_idempotent_fatal_error enabled" do
      before do
        @producer = build(:idempotent_producer, reload_on_idempotent_fatal_error: true)
        @fatal_error = Rdkafka::RdkafkaError.new(-150, fatal: true)
        @reloaded_events = []

        @producer.monitor.subscribe("producer.reloaded") { |event| @reloaded_events << event }
      end

      it "expect to instrument producer.reload and producer.reloaded events during reload" do
        reload_events = []
        @producer.monitor.subscribe("producer.reload") { |event| reload_events << event }

        call_count = 0

        # Stub the initial client's produce method
        original_client = @producer.client
        produce_stub = lambda do |*_args, **_kwargs|
          call_count += 1
          raise @fatal_error if call_count == 1

          # Return a successful delivery handle
          OpenStruct.new(wait: nil)
        end

        # Create a mock builder instance
        mock_builder = OpenStruct.new
        mock_builder.define_singleton_method(:call) do |_prod, _config|
          mock_client = OpenStruct.new(
            delivery_callback: nil,
            closed?: false
          )
          mock_client.define_singleton_method(:produce) do |*_args, **_kwargs|
            call_count += 1
            handle = OpenStruct.new
            handle.define_singleton_method(:wait) { |*| nil }
            handle
          end
          mock_client.define_singleton_method(:flush) { |*_| nil }
          mock_client.define_singleton_method(:close) { nil }
          mock_client.define_singleton_method(:purge) { nil }
          mock_client.define_singleton_method(:delivery_callback=) { |_| nil }
          mock_client
        end

        original_client.stub(:produce, produce_stub) do
          WaterDrop::Producer::Builder.stub(:new, ->(*) { mock_builder }) do
            # This should trigger one reload and succeed on retry
            @producer.produce_sync(@message)
          end
        end

        # Verify producer.reload event was emitted
        assert_equal(1, reload_events.size)
        assert_equal(@producer.id, reload_events.first[:producer_id])
        assert_equal(@fatal_error, reload_events.first[:error])
        assert_equal(1, reload_events.first[:attempt])
        assert_equal(@producer, reload_events.first[:caller])

        # Verify producer.reloaded event was emitted
        assert_equal(1, @reloaded_events.size)
        assert_equal(@producer.id, @reloaded_events.first[:producer_id])
        assert_equal(1, @reloaded_events.first[:attempt])
      end
    end

    context "when fatal error occurs with reload_on_idempotent_fatal_error disabled" do
      before do
        @producer = build(:idempotent_producer, reload_on_idempotent_fatal_error: false)
        @fatal_error = Rdkafka::RdkafkaError.new(-150, fatal: true)
      end

      it "expect to raise the fatal error without reload" do
        @producer.client.stub(:produce, ->(*_a, **_kw) { raise @fatal_error }) do
          assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_sync(@message) }
        end
      end
    end

    context "when non-reloadable fatal error (fenced) occurs" do
      before do
        @producer = build(:idempotent_producer, reload_on_idempotent_fatal_error: true)
        # Fenced error - code -144 from librdkafka for producer fenced
        @fenced_error = Rdkafka::RdkafkaError.new(-144, fatal: true)
        @reloaded_events = []

        @producer.monitor.subscribe("producer.reloaded") { |event| @reloaded_events << event }
      end

      it "expect not to reload and raise the error" do
        @producer.client.stub(:produce, ->(*_a, **_kw) { raise @fenced_error }) do
          # This should raise because fenced errors are non-reloadable by default
          assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_sync(@message) }
        end
      end

      it "expect not to emit producer.reloaded event" do
        @producer.client.stub(:produce, ->(*_a, **_kw) { raise @fenced_error }) do
          begin
            @producer.produce_sync(@message)
          rescue WaterDrop::Errors::ProduceError
            nil
          end
        end

        assert_empty(@reloaded_events)
      end
    end

    context "when fatal error occurs on transactional producer" do
      before do
        @producer = build(:transactional_producer, reload_on_idempotent_fatal_error: true)
        @fatal_error = Rdkafka::RdkafkaError.new(-150, fatal: true)
      end

      it "expect not to use idempotent reload path" do
        @producer.client.stub(:produce, ->(*_a, **_kw) { raise @fatal_error }) do
          # Transactional producers should not use the idempotent reload path
          assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_sync(@message) }
        end
      end
    end

    context "when fatal error occurs on non-idempotent producer" do
      before do
        @producer = build(:producer, reload_on_idempotent_fatal_error: true)
        @fatal_error = Rdkafka::RdkafkaError.new(-150, fatal: true)
      end

      it "expect not to reload as producer is not idempotent" do
        @producer.client.stub(:produce, ->(*_a, **_kw) { raise @fatal_error }) do
          assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_sync(@message) }
        end
      end
    end

    context "when non-fatal error occurs" do
      before do
        @producer = build(:idempotent_producer, reload_on_idempotent_fatal_error: true)
        # Non-fatal error like queue_full
        @queue_full_error = Rdkafka::RdkafkaError.new(-184, fatal: false)
      end

      it "expect not to reload for non-fatal errors" do
        @producer.client.stub(:produce, ->(*_a, **_kw) { raise @queue_full_error }) do
          # Should follow normal error handling path, not reload
          assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_sync(@message) }
        end
      end
    end

    context "when using Testing helper to trigger real fatal errors" do
      before do
        @producer = build(:idempotent_producer, reload_on_idempotent_fatal_error: true)
        # Include the Testing module to enable fatal error injection
        @producer.singleton_class.include(WaterDrop::Producer::Testing)
      end

      it "expect to trigger a fatal error and have it queryable" do
        # Trigger a real fatal error using the testing helper
        result = @producer.trigger_test_fatal_error(47, "Test producer epoch error")

        # Verify the trigger was successful
        assert_equal(0, result)

        # Verify the fatal error is now present and queryable
        fatal_error = @producer.fatal_error
        refute_nil(fatal_error)
        assert_equal(47, fatal_error[:error_code])
        assert_includes(fatal_error[:error_string], "test_fatal_error")
      end

      it "expect to detect fatal error state after triggering" do
        # Trigger a real fatal error
        @producer.trigger_test_fatal_error(47, "Injected test error")

        # Verify fatal error is present and has correct error code
        fatal_error = @producer.fatal_error
        refute_nil(fatal_error)
        assert_equal(47, fatal_error[:error_code])
      end
    end

    context "with real fatal error injection and reload testing" do
      before do
        @producer = build(:idempotent_producer, reload_on_idempotent_fatal_error: true)
        @producer.singleton_class.include(WaterDrop::Producer::Testing)
      end

      it "can inject fatal errors with various error codes" do
        # Error code 47 (INVALID_PRODUCER_EPOCH)
        result = @producer.trigger_test_fatal_error(47, "Simulated producer fencing")
        assert_equal(0, result)

        fatal_error = @producer.fatal_error
        refute_nil(fatal_error)
        assert_equal(47, fatal_error[:error_code])
        assert_kind_of(String, fatal_error[:error_string])
        refute_empty(fatal_error[:error_string])
      end

      it "persists fatal error state across multiple queries" do
        @producer.trigger_test_fatal_error(47, "Test error persistence")

        # Query multiple times
        first_query = @producer.fatal_error
        second_query = @producer.fatal_error
        third_query = @producer.fatal_error

        assert_equal(first_query, second_query)
        assert_equal(second_query, third_query)
        assert_equal(47, first_query[:error_code])
      end

      it "maintains fatal error state after triggering" do
        # Trigger fatal error
        @producer.trigger_test_fatal_error(47, "Test fatal error state")

        # Verify error is present
        refute_nil(@producer.fatal_error)

        # Error state should persist
        sleep 0.1
        refute_nil(@producer.fatal_error)
        assert_equal(47, @producer.fatal_error[:error_code])
      end

      it "detects real fatal errors correctly" do
        # First, produce a message successfully to ensure producer is working
        report = @producer.produce_sync(@message)
        assert_kind_of(Rdkafka::Producer::DeliveryReport, report)
        assert_nil(report.error)

        # Now inject a real fatal error
        @producer.trigger_test_fatal_error(47, "Test reload trigger")

        # Verify the fatal error is present
        fatal_error = @producer.fatal_error
        refute_nil(fatal_error)
        assert_equal(47, fatal_error[:error_code])
      end

      it "can create new producer after fatal error for recovery" do
        # Trigger fatal error on first producer
        @producer.trigger_test_fatal_error(47, "First producer fatal")
        refute_nil(@producer.fatal_error)

        # Close it
        @producer.close

        # Create new producer - should work fine
        new_producer = build(:idempotent_producer)
        new_producer.singleton_class.include(WaterDrop::Producer::Testing)

        # Verify new producer has no fatal error
        assert_nil(new_producer.fatal_error)

        # Should be able to produce
        new_producer.produce_sync(@message)

        new_producer.close
      end
    end

    context "with real fatal error injection and reload event instrumentation" do
      before do
        @producer = build(
          :idempotent_producer,
          reload_on_idempotent_fatal_error: true,
          max_attempts_on_idempotent_fatal_error: 3,
          wait_backoff_on_idempotent_fatal_error: 100
        )
        @reload_events = []
        @reloaded_events = []
        @error_events = []

        @producer.singleton_class.include(WaterDrop::Producer::Testing)
        @producer.monitor.subscribe("producer.reload") { |event| @reload_events << event }
        @producer.monitor.subscribe("producer.reloaded") { |event| @reloaded_events << event }
        @producer.monitor.subscribe("error.occurred") { |event| @error_events << event }
      end

      it "emits producer.reload and producer.reloaded events when fatal error triggers reload" do
        # Trigger real fatal error using Testing API
        @producer.trigger_test_fatal_error(47, "Test reload with real fatal error")

        # Verify fatal error is present
        refute_nil(@producer.fatal_error)
        assert_equal(47, @producer.fatal_error[:error_code])

        # Produce should trigger reload and succeed
        report = @producer.produce_sync(@message)
        assert_kind_of(Rdkafka::Producer::DeliveryReport, report)

        # Verify producer.reload event was emitted
        assert_operator(@reload_events.size, :>=, 1)
        assert_equal(@producer.id, @reload_events.first[:producer_id])
        assert_kind_of(Rdkafka::RdkafkaError, @reload_events.first[:error])
        assert_equal(true, @reload_events.first[:error].fatal?)
        assert_equal(1, @reload_events.first[:attempt])
        assert_equal(@producer, @reload_events.first[:caller])

        # Verify producer.reloaded event was emitted
        assert_operator(@reloaded_events.size, :>=, 1)
        assert_equal(@producer.id, @reloaded_events.first[:producer_id])
        assert_equal(1, @reloaded_events.first[:attempt])
      end

      it "emits reload events when producer recovers from fatal error" do
        # Trigger fatal error using Testing API
        @producer.trigger_test_fatal_error(47, "Test reload event emission")

        # Verify fatal error is present
        refute_nil(@producer.fatal_error)
        assert_equal(47, @producer.fatal_error[:error_code])

        # Produce should trigger reload and succeed
        report = @producer.produce_sync(@message)
        assert_kind_of(Rdkafka::Producer::DeliveryReport, report)

        # Verify reload events were emitted
        assert_operator(@reload_events.size, :>=, 1)
        assert_equal(1, @reload_events.first[:attempt])
        assert_equal(@producer.id, @reload_events.first[:producer_id])

        # Verify reloaded events were emitted
        assert_operator(@reloaded_events.size, :>=, 1)
        assert_equal(1, @reloaded_events.first[:attempt])
        assert_equal(@producer.id, @reloaded_events.first[:producer_id])
      end

      it "includes error.occurred event with fatal error details" do
        # Trigger fatal error using Testing API
        @producer.trigger_test_fatal_error(47, "Test error.occurred event")

        # Produce should trigger reload and succeed
        report = @producer.produce_sync(@message)
        assert_kind_of(Rdkafka::Producer::DeliveryReport, report)

        # Verify error.occurred event was emitted
        assert_operator(@error_events.size, :>=, 1)

        fatal_error_events = @error_events.select do |e|
          e[:type] == "librdkafka.idempotent_fatal_error"
        end

        refute_empty(fatal_error_events)
        assert_equal(@producer.id, fatal_error_events.first[:producer_id])
        assert_kind_of(Rdkafka::RdkafkaError, fatal_error_events.first[:error])
        assert_equal(1, fatal_error_events.first[:attempt])
      end

      it "allows config modification in producer.reload event with real fatal errors" do
        config_modified = false
        modified_config = nil

        # Subscribe to reload event and capture config modification
        @producer.monitor.subscribe("producer.reload") do |event|
          # Modify a valid kafka config setting
          event[:caller].config.kafka[:"enable.idempotence"] = false
          config_modified = true
          modified_config = event[:caller].config
        end

        # Trigger fatal error using Testing API
        @producer.trigger_test_fatal_error(47, "Test config modification")

        # Produce should trigger reload and succeed
        report = @producer.produce_sync(@message)
        assert_kind_of(Rdkafka::Producer::DeliveryReport, report)

        # Verify config modification event was called
        assert_equal(true, config_modified)
        refute_nil(modified_config)
        assert_equal(false, modified_config.kafka[:"enable.idempotence"])
        assert_operator(@reload_events.size, :>=, 1)
        assert_operator(@reloaded_events.size, :>=, 1)
      end
    end

    context "when reload is disabled for idempotent producer" do
      before do
        @producer = build(
          :idempotent_producer,
          reload_on_idempotent_fatal_error: false,
          max_attempts_on_idempotent_fatal_error: 3,
          wait_backoff_on_idempotent_fatal_error: 100
        )
        @reload_events = []
        @reloaded_events = []

        @producer.singleton_class.include(WaterDrop::Producer::Testing)
        @producer.monitor.subscribe("producer.reload") { |event| @reload_events << event }
        @producer.monitor.subscribe("producer.reloaded") { |event| @reloaded_events << event }
      end

      it "keeps throwing errors without reload when reload is disabled" do
        initial_client = @producer.client

        # Trigger fatal error
        @producer.trigger_test_fatal_error(47, "Fatal error with reload disabled")

        # Stub to always raise fatal error
        fatal = Rdkafka::RdkafkaError.new(-150, fatal: true)
        initial_client.stub(:produce, ->(*_a, **_kw) { raise fatal }) do
          # First attempt should raise
          assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_sync(@message) }

          # Second attempt should also raise (no reload)
          assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_sync(@message) }

          # Third attempt should also raise (no reload)
          assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_sync(@message) }
        end

        # Verify no reload events were emitted
        assert_empty(@reload_events)
        assert_empty(@reloaded_events)
      end

      it "does not attempt reload even with multiple produce attempts" do
        initial_client = @producer.client
        produce_attempts = 0

        # Trigger fatal error
        @producer.trigger_test_fatal_error(47, "Fatal error without reload")

        # Stub to count produce attempts
        fatal = Rdkafka::RdkafkaError.new(-150, fatal: true)
        counter_stub = lambda do |*_a, **_kw|
          produce_attempts += 1
          raise fatal
        end

        initial_client.stub(:produce, counter_stub) do
          # Multiple produce attempts
          5.times do
            @producer.produce_sync(@message)
          rescue WaterDrop::Errors::ProduceError
            # Expected to fail
          end
        end

        # All attempts should have hit the same client (no reload)
        assert_equal(5, produce_attempts)
        assert_empty(@reload_events)
        assert_empty(@reloaded_events)

        # Fatal error should still be present
        refute_nil(@producer.fatal_error)
        assert_equal(47, @producer.fatal_error[:error_code])
      end
    end

    context "when calling produce methods after fatal state is reached" do
      before do
        @producer = build(
          :idempotent_producer,
          reload_on_idempotent_fatal_error: false
        )
        @messages = Array.new(3) { build(:valid_message, topic: @topic_name) }

        @producer.singleton_class.include(WaterDrop::Producer::Testing)
        # Trigger fatal error to put producer in fatal state
        @producer.trigger_test_fatal_error(47, "Producer in fatal state")
      end

      it "produce_sync raises error when producer is in fatal state" do
        error = nil
        begin
          @producer.produce_sync(@message)
        rescue WaterDrop::Errors::ProduceError => e
          error = e
        end

        refute_nil(error)
        assert_kind_of(Rdkafka::RdkafkaError, error.cause)
        assert_equal(true, error.cause.fatal?)

        # Verify fatal error persists
        refute_nil(@producer.fatal_error)
        assert_equal(47, @producer.fatal_error[:error_code])
      end

      it "produce_async raises error when producer is in fatal state" do
        error = nil
        begin
          @producer.produce_async(@message)
        rescue WaterDrop::Errors::ProduceError => e
          error = e
        end

        refute_nil(error)
        assert_kind_of(Rdkafka::RdkafkaError, error.cause)
        assert_equal(true, error.cause.fatal?)

        # Verify fatal error persists
        refute_nil(@producer.fatal_error)
      end

      it "produce_many_sync raises error when producer is in fatal state" do
        error = nil
        begin
          @producer.produce_many_sync(@messages)
        rescue WaterDrop::Errors::ProduceManyError => e
          error = e
        end

        refute_nil(error)
        assert_kind_of(Rdkafka::RdkafkaError, error.cause)
        assert_equal(true, error.cause.fatal?)

        # Verify fatal error persists
        refute_nil(@producer.fatal_error)
      end

      it "produce_many_async raises error when producer is in fatal state" do
        error = nil
        begin
          @producer.produce_many_async(@messages)
        rescue WaterDrop::Errors::ProduceManyError => e
          error = e
        end

        refute_nil(error)
        assert_kind_of(Rdkafka::RdkafkaError, error.cause)
        assert_equal(true, error.cause.fatal?)

        # Verify fatal error persists
        refute_nil(@producer.fatal_error)
      end

      it "all produce methods fail consistently in fatal state" do
        # Try each method multiple times to verify consistent behavior
        3.times do
          error = assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_sync(@message) }
          assert_kind_of(Rdkafka::RdkafkaError, error.cause)
          assert_equal(true, error.cause.fatal?)

          error = assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_async(@message) }
          assert_kind_of(Rdkafka::RdkafkaError, error.cause)
          assert_equal(true, error.cause.fatal?)

          error = assert_raises(WaterDrop::Errors::ProduceManyError) { @producer.produce_many_sync(@messages) }
          assert_kind_of(Rdkafka::RdkafkaError, error.cause)
          assert_equal(true, error.cause.fatal?)

          error = assert_raises(WaterDrop::Errors::ProduceManyError) { @producer.produce_many_async(@messages) }
          assert_kind_of(Rdkafka::RdkafkaError, error.cause)
          assert_equal(true, error.cause.fatal?)
        end

        # Fatal error should still be present after all attempts
        refute_nil(@producer.fatal_error)
        assert_equal(47, @producer.fatal_error[:error_code])
      end

      it "documents that producer remains unusable after fatal error without reload" do
        # First attempt fails
        assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_sync(@message) }

        # Producer is still in fatal state
        refute_nil(@producer.fatal_error)

        # Subsequent attempts also fail
        assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_sync(@message) }
        assert_raises(WaterDrop::Errors::ProduceError) { @producer.produce_async(@message) }

        # Fatal error persists - producer needs reload or recreation
        assert_equal(47, @producer.fatal_error[:error_code])
      end
    end
  end
end
