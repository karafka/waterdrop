# frozen_string_literal: true

class WaterDropPollingSetupTest < WaterDropTest::Base
  def teardown
    WaterDrop::Polling::Config.setup do |config|
      config.thread_priority = 0
      config.poll_timeout = 1_000
      config.backoff_min = 100
      config.backoff_max = 30_000
    end

    super
  end

  def test_setup_delegates_to_config_setup
    WaterDrop::Polling.setup do |config|
      config.thread_priority = -2
      config.poll_timeout = 250
    end

    assert_equal(-2, WaterDrop::Polling::Config.config.thread_priority)
    assert_equal 250, WaterDrop::Polling::Config.config.poll_timeout
  end

  def test_setup_validates_configuration
    assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
      WaterDrop::Polling.setup do |config|
        config.thread_priority = 10
      end
    end
  end
end
