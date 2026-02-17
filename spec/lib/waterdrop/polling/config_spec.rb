# frozen_string_literal: true

RSpec.describe_current do
  # Store original values
  let(:original_thread_priority) { described_class.config.thread_priority }
  let(:original_poll_timeout) { described_class.config.poll_timeout }
  let(:original_backoff_min) { described_class.config.backoff_min }
  let(:original_backoff_max) { described_class.config.backoff_max }

  after do
    # Reset to defaults after each test
    described_class.setup do |config|
      config.thread_priority = 0
      config.poll_timeout = 1_000
      config.backoff_min = 100
      config.backoff_max = 30_000
    end
  end

  describe ".setup" do
    it "allows configuration via block" do
      described_class.setup do |config|
        config.thread_priority = -1
        config.poll_timeout = 500
      end

      expect(described_class.config.thread_priority).to eq(-1)
      expect(described_class.config.poll_timeout).to eq(500)
    end

    it "validates configuration" do
      expect do
        described_class.setup do |config|
          config.thread_priority = 10 # Invalid: must be -3 to 3
        end
      end.to raise_error(WaterDrop::Errors::ConfigurationInvalidError)
    end
  end

  describe "config.thread_priority" do
    it "defaults to 0" do
      expect(described_class.config.thread_priority).to eq(0)
    end
  end

  describe "config.poll_timeout" do
    it "defaults to 1000" do
      expect(described_class.config.poll_timeout).to eq(1_000)
    end
  end

  describe "config.backoff_min" do
    it "defaults to 100" do
      expect(described_class.config.backoff_min).to eq(100)
    end
  end

  describe "config.backoff_max" do
    it "defaults to 30_000" do
      expect(described_class.config.backoff_max).to eq(30_000)
    end
  end

  describe "validation" do
    it "rejects thread_priority below -3" do
      expect do
        described_class.setup do |config|
          config.thread_priority = -4
        end
      end.to raise_error(WaterDrop::Errors::ConfigurationInvalidError)
    end

    it "rejects thread_priority above 3" do
      expect do
        described_class.setup do |config|
          config.thread_priority = 4
        end
      end.to raise_error(WaterDrop::Errors::ConfigurationInvalidError)
    end

    it "accepts thread_priority at boundary -3" do
      expect do
        described_class.setup do |config|
          config.thread_priority = -3
        end
      end.not_to raise_error
    end

    it "accepts thread_priority at boundary 3" do
      expect do
        described_class.setup do |config|
          config.thread_priority = 3
        end
      end.not_to raise_error
    end

    it "rejects poll_timeout below 1" do
      expect do
        described_class.setup do |config|
          config.poll_timeout = 0
        end
      end.to raise_error(WaterDrop::Errors::ConfigurationInvalidError)
    end

    it "rejects non-integer poll_timeout" do
      expect do
        described_class.setup do |config|
          config.poll_timeout = 1.5
        end
      end.to raise_error(WaterDrop::Errors::ConfigurationInvalidError)
    end

    it "rejects backoff_min below 1" do
      expect do
        described_class.setup do |config|
          config.backoff_min = 0
        end
      end.to raise_error(WaterDrop::Errors::ConfigurationInvalidError)
    end

    it "rejects backoff_max below 1" do
      expect do
        described_class.setup do |config|
          config.backoff_max = 0
        end
      end.to raise_error(WaterDrop::Errors::ConfigurationInvalidError)
    end

    it "rejects backoff_max less than backoff_min" do
      expect do
        described_class.setup do |config|
          config.backoff_min = 1000
          config.backoff_max = 100
        end
      end.to raise_error(WaterDrop::Errors::ConfigurationInvalidError)
    end

    it "accepts backoff_max equal to backoff_min" do
      expect do
        described_class.setup do |config|
          config.backoff_min = 500
          config.backoff_max = 500
        end
      end.not_to raise_error
    end
  end
end
