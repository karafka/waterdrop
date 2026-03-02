# frozen_string_literal: true

describe_current do
  after do
    # Reset to defaults after each test
    WaterDrop::Polling::Config.setup do |config|
      config.thread_priority = 0
      config.poll_timeout = 1_000
      config.backoff_min = 100
      config.backoff_max = 30_000
    end
  end

  describe ".setup" do
    it "delegates to Config.setup" do
      described_class.setup do |config|
        config.thread_priority = -2
        config.poll_timeout = 250
      end

      assert_equal(-2, WaterDrop::Polling::Config.config.thread_priority)
      assert_equal(250, WaterDrop::Polling::Config.config.poll_timeout)
    end

    it "validates configuration" do
      assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
        described_class.setup do |config|
          config.thread_priority = 10
        end
      end
    end
  end
end
