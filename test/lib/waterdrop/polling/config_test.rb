# frozen_string_literal: true

class WaterDropPollingConfigTest < WaterDropTest::Base
  def teardown
    WaterDrop::Polling::Config.setup do |config|
      config.thread_priority = 0
      config.poll_timeout = 1_000
      config.backoff_min = 100
      config.backoff_max = 30_000
    end

    super
  end

  def test_setup_allows_configuration_via_block
    WaterDrop::Polling::Config.setup do |config|
      config.thread_priority = -1
      config.poll_timeout = 500
    end

    assert_equal(-1, WaterDrop::Polling::Config.config.thread_priority)
    assert_equal 500, WaterDrop::Polling::Config.config.poll_timeout
  end

  def test_setup_validates_configuration
    assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
      WaterDrop::Polling::Config.setup do |config|
        config.thread_priority = 10
      end
    end
  end

  def test_thread_priority_defaults_to_zero
    assert_equal 0, WaterDrop::Polling::Config.config.thread_priority
  end

  def test_poll_timeout_defaults_to_1000
    assert_equal 1_000, WaterDrop::Polling::Config.config.poll_timeout
  end

  def test_backoff_min_defaults_to_100
    assert_equal 100, WaterDrop::Polling::Config.config.backoff_min
  end

  def test_backoff_max_defaults_to_30000
    assert_equal 30_000, WaterDrop::Polling::Config.config.backoff_max
  end

  def test_rejects_thread_priority_below_negative_three
    assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
      WaterDrop::Polling::Config.setup do |config|
        config.thread_priority = -4
      end
    end
  end

  def test_rejects_thread_priority_above_three
    assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
      WaterDrop::Polling::Config.setup do |config|
        config.thread_priority = 4
      end
    end
  end

  def test_accepts_thread_priority_at_boundary_negative_three
    WaterDrop::Polling::Config.setup do |config|
      config.thread_priority = -3
    end
  end

  def test_accepts_thread_priority_at_boundary_three
    WaterDrop::Polling::Config.setup do |config|
      config.thread_priority = 3
    end
  end

  def test_rejects_poll_timeout_below_one
    assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
      WaterDrop::Polling::Config.setup do |config|
        config.poll_timeout = 0
      end
    end
  end

  def test_rejects_non_integer_poll_timeout
    assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
      WaterDrop::Polling::Config.setup do |config|
        config.poll_timeout = 1.5
      end
    end
  end

  def test_rejects_backoff_min_below_one
    assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
      WaterDrop::Polling::Config.setup do |config|
        config.backoff_min = 0
      end
    end
  end

  def test_rejects_backoff_max_below_one
    assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
      WaterDrop::Polling::Config.setup do |config|
        config.backoff_max = 0
      end
    end
  end

  def test_rejects_backoff_max_less_than_backoff_min
    assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
      WaterDrop::Polling::Config.setup do |config|
        config.backoff_min = 1000
        config.backoff_max = 100
      end
    end
  end

  def test_accepts_backoff_max_equal_to_backoff_min
    WaterDrop::Polling::Config.setup do |config|
      config.backoff_min = 500
      config.backoff_max = 500
    end
  end
end
