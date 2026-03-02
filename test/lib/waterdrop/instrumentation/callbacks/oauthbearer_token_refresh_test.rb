# frozen_string_literal: true

require "test_helper"

describe_current do
  before do
    @bearer = OpenStruct.new(name: "test_bearer")
    @monitor = WaterDrop::Instrumentation::Monitor.new
    @callback = described_class.new(@bearer, @monitor)
    @rd_config = Rdkafka::Config.new
    @bearer_name = "test_bearer"
  end

  describe "#call" do
    describe "when the bearer name matches" do
      it "instruments an oauthbearer.token_refresh event" do
        instrumented_calls = []

        @monitor.stub(:instrument, lambda { |event_name, **kwargs|
          instrumented_calls << [event_name, kwargs]
        }) do
          @callback.call(@rd_config, @bearer_name)
        end

        assert_equal(1, instrumented_calls.size)
        assert_equal("oauthbearer.token_refresh", instrumented_calls.first[0])
        assert_equal(@bearer, instrumented_calls.first[1][:bearer])
        assert_equal(@callback, instrumented_calls.first[1][:caller])
      end
    end

    describe "when the bearer name does not match" do
      before do
        @bearer_name = "different_bearer"
      end

      it "does not instrument any event" do
        instrumented_calls = []

        @monitor.stub(:instrument, lambda { |event_name, **kwargs|
          instrumented_calls << [event_name, kwargs]
        }) do
          @callback.call(@rd_config, @bearer_name)
        end

        assert_empty(instrumented_calls)
      end
    end

    describe "when oauth bearer handler contains error" do
      before do
        @tracked_errors = []

        @monitor.subscribe("oauthbearer.token_refresh") do
          raise
        end

        local_errors = @tracked_errors

        @monitor.subscribe("error.occurred") do |event|
          local_errors << event
        end
      end

      it "expect to contain in, notify and continue as we do not want to crash rdkafka" do
        @callback.call(@rd_config, @bearer_name)
        assert_equal(1, @tracked_errors.size)
        assert_equal("callbacks.oauthbearer_token_refresh.error", @tracked_errors.first[:type])
      end
    end
  end
end
