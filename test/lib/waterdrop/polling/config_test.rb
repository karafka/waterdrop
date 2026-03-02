# frozen_string_literal: true

describe_current do
  before do
    @original_thread_priority = described_class.config.thread_priority
    @original_poll_timeout = described_class.config.poll_timeout
    @original_backoff_min = described_class.config.backoff_min
    @original_backoff_max = described_class.config.backoff_max
  end

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

      assert_equal(-1, described_class.config.thread_priority)
      assert_equal(500, described_class.config.poll_timeout)
    end

    it "validates configuration" do
      assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
        described_class.setup do |config|
          config.thread_priority = 10 # Invalid: must be -3 to 3
        end
      end
    end
  end

  describe "config.thread_priority" do
    it "defaults to 0" do
      assert_equal(0, described_class.config.thread_priority)
    end
  end

  describe "config.poll_timeout" do
    it "defaults to 1000" do
      assert_equal(1_000, described_class.config.poll_timeout)
    end
  end

  describe "config.backoff_min" do
    it "defaults to 100" do
      assert_equal(100, described_class.config.backoff_min)
    end
  end

  describe "config.backoff_max" do
    it "defaults to 30_000" do
      assert_equal(30_000, described_class.config.backoff_max)
    end
  end

  describe "validation" do
    it "rejects thread_priority below -3" do
      assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
        described_class.setup do |config|
          config.thread_priority = -4
        end
      end
    end

    it "rejects thread_priority above 3" do
      assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
        described_class.setup do |config|
          config.thread_priority = 4
        end
      end
    end

    it "accepts thread_priority at boundary -3" do
      described_class.setup do |config|
        config.thread_priority = -3
      end
    end

    it "accepts thread_priority at boundary 3" do
      described_class.setup do |config|
        config.thread_priority = 3
      end
    end

    it "rejects poll_timeout below 1" do
      assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
        described_class.setup do |config|
          config.poll_timeout = 0
        end
      end
    end

    it "rejects non-integer poll_timeout" do
      assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
        described_class.setup do |config|
          config.poll_timeout = 1.5
        end
      end
    end

    it "rejects backoff_min below 1" do
      assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
        described_class.setup do |config|
          config.backoff_min = 0
        end
      end
    end

    it "rejects backoff_max below 1" do
      assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
        described_class.setup do |config|
          config.backoff_max = 0
        end
      end
    end

    it "rejects backoff_max less than backoff_min" do
      assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
        described_class.setup do |config|
          config.backoff_min = 1000
          config.backoff_max = 100
        end
      end
    end

    it "accepts backoff_max equal to backoff_min" do
      described_class.setup do |config|
        config.backoff_min = 500
        config.backoff_max = 500
      end
    end
  end
end
