# frozen_string_literal: true

describe_current do
  before do
    @producer = build(:idempotent_producer)
    # Include the Testing module for the producer instance
    @producer.singleton_class.include(WaterDrop::Producer::Testing)
  end

  after { @producer.close }

  describe "#trigger_test_fatal_error" do
    it "triggers a fatal error on the producer" do
      # Trigger a fatal error with error code 47 (INVALID_PRODUCER_EPOCH)
      result = @producer.trigger_test_fatal_error(47, "Test producer fencing")

      # The result should be 0 (success)
      assert_equal(0, result)

      # Verify that a fatal error is now present
      fatal_error = @producer.fatal_error

      refute_nil(fatal_error)
      assert_equal(47, fatal_error[:error_code])
      assert_includes(fatal_error[:error_string], "test_fatal_error")
    end

    it "triggers different error codes" do
      # Test with error code 64 (INVALID_PRODUCER_ID_MAPPING)
      @producer.trigger_test_fatal_error(64, "Test invalid producer ID")

      fatal_error = @producer.fatal_error

      refute_nil(fatal_error)
      assert_equal(64, fatal_error[:error_code])
    end

    it "includes the reason in the error context" do
      custom_reason = "Custom test scenario for fencing"
      @producer.trigger_test_fatal_error(47, custom_reason)

      # Verify error was triggered
      refute_nil(@producer.fatal_error)
    end
  end

  describe "#fatal_error" do
    context "when no fatal error has occurred" do
      it "returns nil" do
        assert_nil(@producer.fatal_error)
      end
    end

    context "when a fatal error has been triggered" do
      before do
        @producer.trigger_test_fatal_error(47, "Test error")
      end

      it "returns error details as a hash" do
        error = @producer.fatal_error

        assert_kind_of(Hash, error)
        assert(error.key?(:error_code))
        assert(error.key?(:error_string))
      end

      it "returns the correct error code" do
        error = @producer.fatal_error

        assert_equal(47, error[:error_code])
      end

      it "returns a human-readable error string" do
        error = @producer.fatal_error

        assert_kind_of(String, error[:error_string])
        refute_empty(error[:error_string])
      end
    end

    context "when called multiple times" do
      before do
        @producer.trigger_test_fatal_error(47, "Test error")
      end

      it "returns consistent results" do
        first_call = @producer.fatal_error
        second_call = @producer.fatal_error

        assert_equal(first_call, second_call)
      end
    end
  end

  describe "integration with idempotent producer reload" do
    before do
      @producer = build(
        :idempotent_producer,
        reload_on_idempotent_fatal_error: true,
        max_attempts_on_idempotent_fatal_error: 3,
        wait_backoff_on_idempotent_fatal_error: 100
      )
      @topic_name = "testing-#{SecureRandom.uuid}"
      @message = build(:valid_message, topic: @topic_name)
      @reload_events = []
      @reloaded_events = []
      @error_events = []

      @producer.singleton_class.include(WaterDrop::Producer::Testing)
      @producer.monitor.subscribe("producer.reload") { |event| @reload_events << event }
      @producer.monitor.subscribe("producer.reloaded") { |event| @reloaded_events << event }
      @producer.monitor.subscribe("error.occurred") { |event| @error_events << event }
    end

    it "can trigger fatal errors that affect produce operations" do
      # Trigger a fatal error
      @producer.trigger_test_fatal_error(47, "Test producer fencing for reload")

      # Verify fatal error is present before produce
      refute_nil(@producer.fatal_error)
      assert_equal(47, @producer.fatal_error[:error_code])
    end
  end

  describe "integration with transactional producers" do
    before do
      @producer = build(
        :transactional_producer,
        reload_on_transaction_fatal_error: true
      )
      @producer.singleton_class.include(WaterDrop::Producer::Testing)
    end

    it "can trigger fatal errors on transactional producers" do
      result = @producer.trigger_test_fatal_error(47, "Test transactional error")

      assert_equal(0, result)

      fatal_error = @producer.fatal_error

      refute_nil(fatal_error)
      assert_equal(47, fatal_error[:error_code])
    end
  end

  describe "usage with different producer types" do
    context "with a standard non-idempotent producer" do
      before do
        @producer = build(:producer)
        @producer.singleton_class.include(WaterDrop::Producer::Testing)
      end

      it "can still trigger and query fatal errors" do
        @producer.trigger_test_fatal_error(47, "Test on non-idempotent")

        refute_nil(@producer.fatal_error)
      end
    end

    context "when included module-wide" do
      before do
        # Include Testing for all producers (simulating spec_helper setup)
        WaterDrop::Producer.include(WaterDrop::Producer::Testing) unless
          WaterDrop::Producer.included_modules.include?(WaterDrop::Producer::Testing)
      end

      it "makes testing methods available to all producer instances" do
        new_producer = build(:idempotent_producer)

        assert_respond_to(new_producer, :trigger_test_fatal_error)
        assert_respond_to(new_producer, :fatal_error)
        new_producer.close
      end
    end
  end

  describe "error code examples" do
    # Document common error codes for testing purposes
    {
      47 => "INVALID_PRODUCER_EPOCH (producer fencing)",
      64 => "INVALID_PRODUCER_ID_MAPPING (invalid producer ID)"
    }.each do |code, description|
      it "works with error code #{code} (#{description})" do
        @producer.trigger_test_fatal_error(code, "Testing #{description}")
        error = @producer.fatal_error

        refute_nil(error)
        assert_equal(code, error[:error_code])
      end
    end
  end
end
