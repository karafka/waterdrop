# frozen_string_literal: true

class WaterDropContractsPollerConfigTest < WaterDropTest::Base
  def setup
    @contract = WaterDrop::Contracts::PollerConfig.new
    @valid_config = {
      thread_priority: 0,
      poll_timeout: 1_000,
      backoff_min: 100,
      backoff_max: 30_000
    }
  end

  def test_valid_config
    assert_predicate @contract.call(@valid_config), :success?
  end

  def test_thread_priority_below_negative_three
    config = @valid_config.merge(thread_priority: -4)

    refute_predicate @contract.call(config), :success?
  end

  def test_thread_priority_above_three
    config = @valid_config.merge(thread_priority: 4)

    refute_predicate @contract.call(config), :success?
  end

  def test_thread_priority_not_integer
    config = @valid_config.merge(thread_priority: 1.5)

    refute_predicate @contract.call(config), :success?
  end

  def test_thread_priority_boundary_negative_three
    config = @valid_config.merge(thread_priority: -3)

    assert_predicate @contract.call(config), :success?
  end

  def test_thread_priority_boundary_three
    config = @valid_config.merge(thread_priority: 3)

    assert_predicate @contract.call(config), :success?
  end

  def test_poll_timeout_zero
    config = @valid_config.merge(poll_timeout: 0)

    refute_predicate @contract.call(config), :success?
  end

  def test_poll_timeout_negative
    config = @valid_config.merge(poll_timeout: -1)

    refute_predicate @contract.call(config), :success?
  end

  def test_poll_timeout_not_integer
    config = @valid_config.merge(poll_timeout: 1.5)

    refute_predicate @contract.call(config), :success?
  end

  def test_backoff_min_zero
    config = @valid_config.merge(backoff_min: 0)

    refute_predicate @contract.call(config), :success?
  end

  def test_backoff_min_negative
    config = @valid_config.merge(backoff_min: -1)

    refute_predicate @contract.call(config), :success?
  end

  def test_backoff_max_zero
    config = @valid_config.merge(backoff_max: 0)

    refute_predicate @contract.call(config), :success?
  end

  def test_backoff_max_less_than_backoff_min
    config = @valid_config.merge(backoff_min: 1000, backoff_max: 100)

    refute_predicate @contract.call(config), :success?
  end

  def test_backoff_max_equal_to_backoff_min
    config = @valid_config.merge(backoff_min: 500, backoff_max: 500)

    assert_predicate @contract.call(config), :success?
  end
end
