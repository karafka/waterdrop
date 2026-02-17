# frozen_string_literal: true

RSpec.describe_current do
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

      expect(WaterDrop::Polling::Config.config.thread_priority).to eq(-2)
      expect(WaterDrop::Polling::Config.config.poll_timeout).to eq(250)
    end

    it "validates configuration" do
      expect do
        described_class.setup do |config|
          config.thread_priority = 10
        end
      end.to raise_error(WaterDrop::Errors::ConfigurationInvalidError)
    end
  end
end
