# frozen_string_literal: true

describe_current do
  it { assert_operator(described_class, :<, Karafka::Core::Monitoring::Monitor) }

  describe "#freeze_statistics_listeners!" do
    before { @monitor = described_class.new }

    it "expect statistics_listeners_frozen? to start as false" do
      refute_predicate(@monitor, :statistics_listeners_frozen?)
    end

    it "expect to flip statistics_listeners_frozen? to true" do
      @monitor.freeze_statistics_listeners!

      assert_predicate(@monitor, :statistics_listeners_frozen?)
    end
  end

  describe "#subscribe with statistics.emitted after freezing" do
    before { @monitor = described_class.new }

    describe "block-based subscription" do
      it "expect to raise StatisticsNotEnabledError when frozen" do
        @monitor.freeze_statistics_listeners!

        assert_raises(WaterDrop::Errors::StatisticsNotEnabledError) do
          @monitor.subscribe("statistics.emitted") { |_event| }
        end
      end

      it "expect to allow subscribing to other events while frozen" do
        @monitor.freeze_statistics_listeners!

        @monitor.subscribe("error.occurred") { |_event| }

        refute_empty(@monitor.listeners["error.occurred"])
      end

      it "expect not to raise when not frozen" do
        @monitor.subscribe("statistics.emitted") { |_event| }

        refute_empty(@monitor.listeners["statistics.emitted"])
      end
    end

    describe "listener-object subscription" do
      before do
        @stats_listener = Class.new do
          def on_statistics_emitted(_event)
          end
        end.new

        @non_stats_listener = Class.new do
          def on_error_occurred(_event)
          end
        end.new

        @mixed_listener = Class.new do
          def on_statistics_emitted(_event)
          end

          def on_error_occurred(_event)
          end
        end.new
      end

      it "expect to raise when listener responds to on_statistics_emitted and frozen" do
        @monitor.freeze_statistics_listeners!

        assert_raises(WaterDrop::Errors::StatisticsNotEnabledError) do
          @monitor.subscribe(@stats_listener)
        end
      end

      it "expect to raise for mixed listeners too, since they still target stats" do
        @monitor.freeze_statistics_listeners!

        assert_raises(WaterDrop::Errors::StatisticsNotEnabledError) do
          @monitor.subscribe(@mixed_listener)
        end
      end

      it "expect to allow listeners that do not respond to on_statistics_emitted" do
        @monitor.freeze_statistics_listeners!

        @monitor.subscribe(@non_stats_listener)

        assert_includes(@monitor.listeners["error.occurred"], @non_stats_listener)
      end

      it "expect not to raise when not frozen" do
        @monitor.subscribe(@stats_listener)

        assert_includes(@monitor.listeners["statistics.emitted"], @stats_listener)
      end
    end
  end
end
