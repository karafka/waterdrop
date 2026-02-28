# frozen_string_literal: true

require "connection_pool"

# Tests for WaterDrop::ConnectionPool#initialize
class WaterDropConnectionPoolInitializeTest < WaterDropTest::Base
  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_creates_pool_with_default_settings
    pool = WaterDrop::ConnectionPool.new do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end

    assert_equal 5, pool.size
    assert_equal 5, pool.available
  end

  def test_creates_pool_with_custom_settings
    pool = WaterDrop::ConnectionPool.new(size: 10, timeout: 3000) do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end

    assert_equal 10, pool.size
    assert_equal 10, pool.available
  end

  def test_configures_producers_correctly
    pool = WaterDrop::ConnectionPool.new(size: 2) do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.id = SecureRandom.uuid
    end

    pool.with do |producer|
      assert_kind_of WaterDrop::Producer, producer
      assert_same true, producer.status.configured?
      assert_same false, producer.config.deliver
    end
  end

  def test_supports_per_producer_configuration_with_index
    producer_ids = []

    pool = WaterDrop::ConnectionPool.new(size: 3) do |config, index|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.id = "producer-#{index}"
    end

    mutex = Mutex.new
    threads = Array.new(3) do
      Thread.new do
        pool.with do |producer|
          mutex.synchronize { producer_ids << producer.id }
          sleep(0.1)
        end
      end
    end
    threads.each(&:join)

    assert_equal 3, producer_ids.uniq.size
    assert_equal %w[producer-1 producer-2 producer-3], producer_ids.sort
  end

  def test_supports_transactional_producers_with_unique_ids
    transaction_ids = []

    pool = WaterDrop::ConnectionPool.new(size: 2) do |config, index|
      config.deliver = false
      config.kafka = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "transactional.id": "tx-#{index}"
      }
    end

    mutex = Mutex.new
    threads = Array.new(2) do
      Thread.new do
        pool.with do |producer|
          tx_id = producer.config.kafka[:"transactional.id"]
          mutex.synchronize { transaction_ids << tx_id }
          sleep(0.1)
        end
      end
    end
    threads.each(&:join)

    assert_equal 2, transaction_ids.uniq.size
    assert_equal %w[tx-1 tx-2], transaction_ids.sort
  end

  def test_backward_compatibility_with_single_parameter_blocks
    pool = WaterDrop::ConnectionPool.new(size: 2) do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end

    pool.with do |producer|
      assert_kind_of WaterDrop::Producer, producer
      assert_same true, producer.status.configured?
      assert_same false, producer.config.deliver
    end
  end
end

# Tests for WaterDrop::ConnectionPool#with
class WaterDropConnectionPoolWithTest < WaterDropTest::Base
  def setup
    @pool = WaterDrop::ConnectionPool.new(size: 2) do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_yields_a_configured_producer
    @pool.with do |producer|
      assert_kind_of WaterDrop::Producer, producer
      assert_same true, producer.status.configured?
    end
  end

  def test_returns_the_result_of_the_block
    result = @pool.with { |producer| "producer-#{producer.id}" }

    assert_match(/producer-waterdrop-\w{12}/, result)
  end

  def test_handles_exceptions_properly
    assert_raises(StandardError) do
      @pool.with { |_producer| raise StandardError, "test error" }
    end
  end

  def test_ensures_producer_returned_to_pool_after_exception
    assert_equal 2, @pool.available

    begin
      @pool.with { |_producer| raise StandardError, "test error" }
    rescue
      # Expected
    end

    assert_equal 2, @pool.available
  end
end

# Tests for WaterDrop::ConnectionPool#stats
class WaterDropConnectionPoolStatsTest < WaterDropTest::Base
  def setup
    @pool = WaterDrop::ConnectionPool.new(size: 3) do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_returns_pool_statistics
    stats = @pool.stats

    assert_kind_of Hash, stats
    assert_equal 3, stats[:size]
    assert_equal 3, stats[:available]
  end

  def test_reflects_pool_usage
    @pool.with do |_producer|
      stats = @pool.stats

      assert_equal 3, stats[:size]
      assert_equal 2, stats[:available]
    end
  end
end

# Tests for WaterDrop::ConnectionPool#shutdown
class WaterDropConnectionPoolShutdownTest < WaterDropTest::Base
  def setup
    @pool = WaterDrop::ConnectionPool.new(size: 2) do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_shuts_down_all_producers
    @pool.with(&:id)
    @pool.shutdown
  end

  def test_calls_close_on_active_producers
    producers = []
    2.times do
      @pool.with { |producer| producers << producer }
    end

    @pool.shutdown

    # After shutdown, producers should be closed
    producers.each do |producer|
      # Producer may be in closed or other non-active state
      assert_same false, producer.status.active?
    end
  end

  def test_handles_inactive_producers
    producer_ref = nil
    @pool.with { |p| producer_ref = p }

    status = OpenStruct.new(active?: false)
    producer_ref.stub(:status, status) do
      @pool.shutdown
    end
  end

  def test_handles_nil_status
    producer_ref = nil
    @pool.with { |p| producer_ref = p }

    producer_ref.stub(:status, nil) do
      @pool.shutdown
    end
  end
end

# Tests for WaterDrop::ConnectionPool#close
class WaterDropConnectionPoolCloseTest < WaterDropTest::Base
  def setup
    @pool = WaterDrop::ConnectionPool.new(size: 2) do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_is_alias_for_shutdown
    @pool.with(&:id)
    @pool.close
  end

  def test_calls_close_on_active_producers
    producers = []
    2.times do
      @pool.with { |producer| producers << producer }
    end

    @pool.close

    producers.each do |producer|
      assert_same false, producer.status.active?
    end
  end

  def test_handles_inactive_producers
    producer_ref = nil
    @pool.with { |p| producer_ref = p }

    status = OpenStruct.new(active?: false)
    producer_ref.stub(:status, status) do
      @pool.close
    end
  end

  def test_handles_nil_status
    producer_ref = nil
    @pool.with { |p| producer_ref = p }

    producer_ref.stub(:status, nil) do
      @pool.close
    end
  end
end

# Tests for WaterDrop::ConnectionPool#reload
class WaterDropConnectionPoolReloadTest < WaterDropTest::Base
  def setup
    @pool = WaterDrop::ConnectionPool.new(size: 2) do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_reloads_all_connections
    @pool.reload
  end

  def test_handles_inactive_producers
    producer_ref = nil
    @pool.with { |p| producer_ref = p }

    status = OpenStruct.new(active?: false)
    producer_ref.stub(:status, status) do
      @pool.reload
    end
  end

  def test_handles_nil_status
    producer_ref = nil
    @pool.with { |p| producer_ref = p }

    producer_ref.stub(:status, nil) do
      @pool.reload
    end
  end
end

# Tests for WaterDrop::ConnectionPool#pool
class WaterDropConnectionPoolPoolAccessorTest < WaterDropTest::Base
  def setup
    @pool = WaterDrop::ConnectionPool.new do |config|
      config.deliver = false
    end
  end

  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_returns_underlying_connection_pool_instance
    assert_kind_of ::ConnectionPool, @pool.pool
  end
end

# Tests for WaterDrop::ConnectionPool class methods - .setup
class WaterDropConnectionPoolClassSetupTest < WaterDropTest::Base
  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_sets_up_global_pool_with_default_settings
    WaterDrop::ConnectionPool.setup do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end

    assert_kind_of WaterDrop::ConnectionPool, WaterDrop::ConnectionPool.default_pool
    assert_equal 5, WaterDrop::ConnectionPool.default_pool.size
  end

  def test_sets_up_global_pool_with_custom_settings
    WaterDrop::ConnectionPool.setup(size: 15, timeout: 3000) do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end

    assert_kind_of WaterDrop::ConnectionPool, WaterDrop::ConnectionPool.default_pool
    assert_equal 15, WaterDrop::ConnectionPool.default_pool.size
  end

  def test_requires_connection_pool_gem
    WaterDrop::ConnectionPool.setup { |config| config.deliver = false }
  end

  def test_supports_per_producer_configuration_with_index_in_global_setup
    WaterDrop::ConnectionPool.setup(size: 2) do |config, index|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.id = "global-#{index}"
    end

    producer_ids = []
    mutex = Mutex.new
    threads = Array.new(2) do
      Thread.new do
        WaterDrop::ConnectionPool.with do |producer|
          mutex.synchronize { producer_ids << producer.id }
          sleep(0.1)
        end
      end
    end
    threads.each(&:join)

    assert_equal 2, producer_ids.uniq.size
    assert_equal %w[global-1 global-2], producer_ids.sort
  end
end

# Tests for WaterDrop::ConnectionPool class methods - .with
class WaterDropConnectionPoolClassWithConfiguredTest < WaterDropTest::Base
  def setup
    WaterDrop::ConnectionPool.setup(size: 3) do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_yields_a_producer_from_global_pool
    WaterDrop::ConnectionPool.with do |producer|
      assert_kind_of WaterDrop::Producer, producer
      assert_same true, producer.status.configured?
    end
  end

  def test_returns_the_result_of_the_block
    result = WaterDrop::ConnectionPool.with { |producer| "global-#{producer.id}" }

    assert_match(/global-waterdrop-\w{12}/, result)
  end
end

class WaterDropConnectionPoolClassWithNotConfiguredTest < WaterDropTest::Base
  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_raises_error_when_not_configured
    assert_raises(RuntimeError) do
      WaterDrop::ConnectionPool.with { |_producer| "test" }
    end
  end
end

# Tests for WaterDrop::ConnectionPool class methods - .stats
class WaterDropConnectionPoolClassStatsConfiguredTest < WaterDropTest::Base
  def setup
    WaterDrop::ConnectionPool.setup(size: 7) do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_returns_global_pool_statistics
    stats = WaterDrop::ConnectionPool.stats

    assert_kind_of Hash, stats
    assert_equal 7, stats[:size]
    assert_equal 7, stats[:available]
  end

  def test_reflects_global_pool_usage
    WaterDrop::ConnectionPool.with do |_producer|
      stats = WaterDrop::ConnectionPool.stats

      assert_equal 7, stats[:size]
      assert_equal 6, stats[:available]
    end
  end
end

class WaterDropConnectionPoolClassStatsNotConfiguredTest < WaterDropTest::Base
  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_returns_nil_when_not_configured
    assert_nil WaterDrop::ConnectionPool.stats
  end

  def test_handles_nil_pool_after_shutdown
    WaterDrop::ConnectionPool.setup do |config|
      config.deliver = false
    end

    WaterDrop::ConnectionPool.shutdown

    assert_nil WaterDrop::ConnectionPool.stats
  end
end

# Tests for WaterDrop::ConnectionPool class methods - .shutdown
class WaterDropConnectionPoolClassShutdownConfiguredTest < WaterDropTest::Base
  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_shuts_down_the_global_pool
    WaterDrop::ConnectionPool.setup do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end

    WaterDrop::ConnectionPool.shutdown

    assert_nil WaterDrop::ConnectionPool.default_pool
  end

  def test_allows_shutdown_to_be_called_multiple_times
    WaterDrop::ConnectionPool.setup do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end

    WaterDrop::ConnectionPool.shutdown
    WaterDrop::ConnectionPool.shutdown
  end
end

class WaterDropConnectionPoolClassShutdownNotConfiguredTest < WaterDropTest::Base
  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_does_not_raise_error
    WaterDrop::ConnectionPool.shutdown
  end
end

# Tests for WaterDrop::ConnectionPool class methods - .close
class WaterDropConnectionPoolClassCloseConfiguredTest < WaterDropTest::Base
  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_closes_the_global_pool
    WaterDrop::ConnectionPool.setup do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end

    WaterDrop::ConnectionPool.close

    assert_nil WaterDrop::ConnectionPool.default_pool
  end

  def test_allows_close_to_be_called_multiple_times
    WaterDrop::ConnectionPool.setup do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end

    WaterDrop::ConnectionPool.close
    WaterDrop::ConnectionPool.close
  end
end

class WaterDropConnectionPoolClassCloseNotConfiguredTest < WaterDropTest::Base
  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_does_not_raise_error
    WaterDrop::ConnectionPool.close
  end
end

# Tests for WaterDrop::ConnectionPool class methods - .reload
class WaterDropConnectionPoolClassReloadConfiguredTest < WaterDropTest::Base
  def setup
    WaterDrop::ConnectionPool.setup do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_reloads_the_global_pool
    WaterDrop::ConnectionPool.reload
  end
end

class WaterDropConnectionPoolClassReloadNotConfiguredTest < WaterDropTest::Base
  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_does_not_raise_error
    WaterDrop::ConnectionPool.reload
  end

  def test_handles_nil_pool_gracefully
    WaterDrop::ConnectionPool.instance_variable_set(:@default_pool, nil)
    WaterDrop::ConnectionPool.reload
  end
end

# Tests for WaterDrop::ConnectionPool class methods - .active?
class WaterDropConnectionPoolClassActiveTest < WaterDropTest::Base
  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_returns_false_when_not_configured
    assert_same false, WaterDrop::ConnectionPool.active?
  end

  def test_returns_true_when_configured
    WaterDrop::ConnectionPool.setup do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end

    assert_same true, WaterDrop::ConnectionPool.active?
  end

  def test_returns_false_after_shutdown
    WaterDrop::ConnectionPool.setup do |config|
      config.deliver = false
    end
    WaterDrop::ConnectionPool.shutdown

    assert_same false, WaterDrop::ConnectionPool.active?
  end
end

# Tests for method delegation
class WaterDropConnectionPoolDelegationTest < WaterDropTest::Base
  def setup
    @pool = WaterDrop::ConnectionPool.new(size: 3) do |config|
      config.deliver = false
    end
  end

  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_delegates_size_to_underlying_pool
    assert_equal 3, @pool.size
  end

  def test_delegates_available_to_underlying_pool
    assert_equal 3, @pool.available
  end

  def test_delegates_with_and_yields_producer
    yielded = nil
    @pool.with { |producer| yielded = producer }

    assert_kind_of WaterDrop::Producer, yielded
  end
end

# Tests for WaterDrop module convenience methods
class WaterDropModuleWithConfiguredTest < WaterDropTest::Base
  def setup
    WaterDrop::ConnectionPool.setup do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_waterdrop_with_delegates_to_connection_pool
    WaterDrop.with do |producer|
      assert_kind_of WaterDrop::Producer, producer
      assert_same true, producer.status.configured?
    end
  end

  def test_waterdrop_with_returns_result
    result = WaterDrop.with { |producer| "module-#{producer.id}" }

    assert_match(/module-waterdrop-\w{12}/, result)
  end
end

class WaterDropModuleWithNotConfiguredTest < WaterDropTest::Base
  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_raises_error_when_not_configured
    assert_raises(RuntimeError) do
      WaterDrop.with { |_producer| "test" }
    end
  end
end

class WaterDropModulePoolTest < WaterDropTest::Base
  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_returns_global_pool_when_configured
    WaterDrop::ConnectionPool.setup do |config|
      config.deliver = false
    end

    assert_kind_of WaterDrop::ConnectionPool, WaterDrop.pool
    assert_equal WaterDrop::ConnectionPool.default_pool, WaterDrop.pool
  end

  def test_returns_nil_when_not_configured
    assert_nil WaterDrop.pool
  end
end

# Tests for concurrency
class WaterDropConnectionPoolConcurrencyTest < WaterDropTest::Base
  def setup
    @pool = WaterDrop::ConnectionPool.new(size: 3) do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_handles_concurrent_access_correctly
    results = []
    threads = []

    5.times do |i|
      threads << Thread.new do
        @pool.with do |producer|
          sleep(0.01)
          results << "thread-#{i}-#{producer.id}"
        end
      end
    end

    threads.each(&:join)

    assert_equal 5, results.size
    results.each { |r| assert_match(/thread-\d+-waterdrop-\w{12}/, r) }
  end

  def test_respects_pool_size_limits
    in_use_count = []
    threads = []
    mutex = Mutex.new

    8.times do
      threads << Thread.new do
        @pool.with do |_producer|
          mutex.synchronize { in_use_count << @pool.stats[:available] }
          sleep(0.05)
        end
      end
    end

    sleep(0.01)
    threads.each(&:join)

    in_use_count.each { |c| assert_operator c, :>=, 0 }
    in_use_count.each { |c| assert_operator c, :<=, 3 }
  end
end

# Tests for error handling
class WaterDropConnectionPoolErrorHandlingTest < WaterDropTest::Base
  def setup
    @pool = WaterDrop::ConnectionPool.new(size: 2) do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_handles_producer_configuration_errors
    broken_pool = WaterDrop::ConnectionPool.new(size: 1) do |config|
      config.deliver = false
      config.max_payload_size = -1
    end

    assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
      broken_pool.with(&:id)
    end
  end

  def test_maintains_pool_integrity_on_failure
    assert_equal 2, @pool.available

    assert_raises(StandardError) do
      @pool.with do |_producer|
        raise StandardError, "Producer operation failed"
      end
    end

    assert_equal 2, @pool.available
  end

  def test_raises_timeout_error_when_all_connections_busy
    small_pool = WaterDrop::ConnectionPool.new(size: 1, timeout: 100) do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end

    grabbed = Queue.new

    thread1 = Thread.new do
      small_pool.with do |_producer|
        grabbed << true
        sleep(0.3)
      end
    end

    grabbed.pop

    assert_raises(::ConnectionPool::TimeoutError) do
      small_pool.with { |_producer| "should timeout" }
    end

    thread1.join
  end
end

# Tests for fiber safety
class WaterDropConnectionPoolFiberSafetyTest < WaterDropTest::Base
  def setup
    @fiber_pool = WaterDrop::ConnectionPool.new(size: 3, timeout: 2000) do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_handles_fiber_based_concurrent_access
    results = []
    fibers = []

    5.times do |i|
      fibers << Fiber.new do
        @fiber_pool.with do |producer|
          results << "fiber-#{i}-#{producer.id}"
          producer.id
        end
      end
    end

    fibers.each(&:resume)

    assert_equal 5, results.size
    results.each { |r| assert_match(/fiber-\d+-waterdrop-\w{12}/, r) }
  end

  def test_maintains_pool_integrity_with_fiber_yielding
    assert_equal 3, @fiber_pool.available

    fiber = Fiber.new do
      @fiber_pool.with do |producer|
        assert_equal 2, @fiber_pool.available
        Fiber.yield(producer.id)

        assert_equal 2, @fiber_pool.available
        producer.id
      end
    end

    producer_id1 = fiber.resume
    producer_id2 = fiber.resume

    assert_equal producer_id1, producer_id2
    assert_equal 3, @fiber_pool.available
  end

  def test_handles_exceptions_in_fibers
    assert_equal 3, @fiber_pool.available

    fiber = Fiber.new do
      @fiber_pool.with do |_producer|
        Fiber.yield
        raise StandardError, "Fiber error"
      end
    end

    fiber.resume

    assert_raises(StandardError) do
      fiber.resume
    end

    assert_equal 3, @fiber_pool.available
  end

  def test_supports_nested_fiber_operations
    results = []

    outer_fiber = Fiber.new do
      @fiber_pool.with do |outer_producer|
        results << "outer-start-#{outer_producer.id}"

        inner_fiber = Fiber.new do
          @fiber_pool.with do |inner_producer|
            results << "inner-#{inner_producer.id}"
            inner_producer.id
          end
        end

        inner_result = inner_fiber.resume
        results << "outer-end-#{outer_producer.id}"

        [outer_producer.id, inner_result]
      end
    end

    outer_result, inner_result = outer_fiber.resume

    assert_equal 3, results.size
    assert_match(/outer-start-waterdrop-\w{12}/, results[0])
    assert_match(/inner-waterdrop-\w{12}/, results[1])
    assert_match(/outer-end-waterdrop-\w{12}/, results[2])
    refute_equal outer_result, inner_result
  end
end

# Tests for global pool fiber safety
class WaterDropConnectionPoolGlobalFiberSafetyTest < WaterDropTest::Base
  def setup
    WaterDrop::ConnectionPool.setup(size: 2, timeout: 1000) do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_handles_fiber_based_access_to_global_pool
    results = []
    fibers = []

    3.times do |i|
      fibers << Fiber.new do
        WaterDrop.with do |producer|
          results << "global-fiber-#{i}-#{producer.id}"
        end
      end
    end

    fibers.each(&:resume)

    assert_equal 3, results.size
    results.each { |r| assert_match(/global-fiber-\d+-waterdrop-\w{12}/, r) }
  end

  def test_maintains_global_pool_stats_consistency
    initial_stats = WaterDrop::ConnectionPool.stats

    assert_equal 2, initial_stats[:size]
    assert_equal 2, initial_stats[:available]

    fiber = Fiber.new do
      WaterDrop.with do |producer|
        stats = WaterDrop::ConnectionPool.stats

        assert_equal 2, stats[:size]
        assert_equal 1, stats[:available]

        Fiber.yield(producer.id)

        stats = WaterDrop::ConnectionPool.stats

        assert_equal 2, stats[:size]
        assert_equal 1, stats[:available]
      end
    end

    fiber.resume
    fiber.resume

    final_stats = WaterDrop::ConnectionPool.stats

    assert_equal 2, final_stats[:size]
    assert_equal 2, final_stats[:available]
  end
end

# Tests for mixed thread and fiber operations
class WaterDropConnectionPoolMixedConcurrencyTest < WaterDropTest::Base
  def setup
    @mixed_pool = WaterDrop::ConnectionPool.new(size: 4, timeout: 1000) do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_handles_mixed_thread_and_fiber_access
    results = []
    mutex = Mutex.new

    threads = Array.new(2) do |i|
      Thread.new do
        @mixed_pool.with do |producer|
          sleep(0.01)
          mutex.synchronize { results << "thread-#{i}-#{producer.id}" }
        end
      end
    end

    fibers = Array.new(2) do |i|
      Fiber.new do
        @mixed_pool.with do |producer|
          Fiber.yield
          mutex.synchronize { results << "fiber-#{i}-#{producer.id}" }
        end
      end
    end

    fibers.each(&:resume)
    fibers.each(&:resume)
    threads.each(&:join)

    assert_equal 4, results.size
    thread_results = results.select { |r| r.start_with?("thread-") }
    fiber_results = results.select { |r| r.start_with?("fiber-") }

    assert_equal 2, thread_results.size
    assert_equal 2, fiber_results.size
  end

  def test_maintains_pool_capacity_limits
    available_counts = []
    mutex = Mutex.new

    operations = []

    2.times do
      operations << Thread.new do
        @mixed_pool.with do |_producer|
          mutex.synchronize { available_counts << @mixed_pool.available }
          sleep(0.05)
        end
      end
    end

    2.times do
      operations << Fiber.new do
        @mixed_pool.with do |_producer|
          mutex.synchronize { available_counts << @mixed_pool.available }
          Fiber.yield
          sleep(0.05)
        end
      end
    end

    operations.select { |op| op.is_a?(Fiber) }.each(&:resume)
    sleep(0.01)
    operations.select { |op| op.is_a?(Fiber) }.each(&:resume)
    operations.select { |op| op.is_a?(Thread) }.each(&:join)

    available_counts.each { |c| assert_includes 0..4, c }
  end
end

# Tests for Connection Pool Events
class WaterDropConnectionPoolEventsLifecycleTest < WaterDropTest::Base
  def setup
    @events = []
    @instrumentation = WaterDrop.instrumentation

    @instrumentation.subscribe("connection_pool.created") { |event| @events << event }
    @instrumentation.subscribe("connection_pool.setup") { |event| @events << event }
    @instrumentation.subscribe("connection_pool.shutdown") { |event| @events << event }
    @instrumentation.subscribe("connection_pool.reload") { |event| @events << event }
    @instrumentation.subscribe("connection_pool.reloaded") { |event| @events << event }
  end

  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_emits_created_event_on_new_pool
    pool = WaterDrop::ConnectionPool.new(size: 2) do |config|
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.deliver = false
    end

    created_event = @events.find { |e| e.id == "connection_pool.created" }

    refute_nil created_event
    assert_equal pool, created_event[:pool]
    assert_equal 2, created_event[:size]
    assert_equal 5000, created_event[:timeout]
  end

  def test_emits_setup_event_on_global_setup
    pool = WaterDrop::ConnectionPool.setup(size: 3, timeout: 10_000) do |config|
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.deliver = false
    end

    setup_event = @events.find { |e| e.id == "connection_pool.setup" }

    refute_nil setup_event
    assert_equal pool, setup_event[:pool]
    assert_equal 3, setup_event[:size]
    assert_equal 10_000, setup_event[:timeout]
  end

  def test_emits_shutdown_event_on_global_shutdown
    WaterDrop::ConnectionPool.setup(size: 2) do |config|
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.deliver = false
    end

    WaterDrop::ConnectionPool.shutdown

    shutdown_event = @events.find { |e| e.id == "connection_pool.shutdown" }

    refute_nil shutdown_event
    refute_nil shutdown_event[:pool]
  end

  def test_emits_reload_event_on_global_reload
    WaterDrop::ConnectionPool.setup(size: 2) do |config|
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.deliver = false
    end

    WaterDrop::ConnectionPool.reload

    reload_event = @events.find { |e| e.id == "connection_pool.reload" }

    refute_nil reload_event
    refute_nil reload_event[:pool]
  end

  def test_emits_shutdown_event_on_instance_shutdown
    pool = WaterDrop::ConnectionPool.new(size: 2) do |config|
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.deliver = false
    end

    pool.shutdown

    shutdown_event = @events.find { |e| e.id == "connection_pool.shutdown" }

    refute_nil shutdown_event
    assert_equal pool, shutdown_event[:pool]
  end

  def test_emits_reloaded_event_on_instance_reload
    pool = WaterDrop::ConnectionPool.new(size: 2) do |config|
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.deliver = false
    end

    pool.reload

    reloaded_event = @events.find { |e| e.id == "connection_pool.reloaded" }

    refute_nil reloaded_event
    assert_equal pool, reloaded_event[:pool]

    pool.shutdown
  end
end

# Tests for event ordering
class WaterDropConnectionPoolEventOrderingTest < WaterDropTest::Base
  def setup
    @events = []
    @instrumentation = WaterDrop.instrumentation

    @instrumentation.subscribe("connection_pool.created") { |event| @events << event }
    @instrumentation.subscribe("connection_pool.setup") { |event| @events << event }
    @instrumentation.subscribe("connection_pool.shutdown") { |event| @events << event }
    @instrumentation.subscribe("connection_pool.reload") { |event| @events << event }
    @instrumentation.subscribe("connection_pool.reloaded") { |event| @events << event }
  end

  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_emits_events_in_correct_order
    pool = WaterDrop::ConnectionPool.new(size: 1) do |config|
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.deliver = false
    end

    pool.with { |producer| producer }
    pool.reload
    pool.shutdown

    event_ids = @events.map(&:id)

    assert_equal "connection_pool.created", event_ids.first

    reload_index = event_ids.index("connection_pool.reloaded")

    assert_operator reload_index, :>, 0

    shutdown_index = event_ids.index("connection_pool.shutdown")

    assert_equal event_ids.size - 1, shutdown_index
  end
end

# Tests for WaterDrop::ConnectionPool#transaction
class WaterDropConnectionPoolTransactionTest < WaterDropTest::Base
  def setup
    @pool = WaterDrop::ConnectionPool.new(size: 2) do |config, index|
      config.kafka = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "transactional.id": "test-tx-#{index}"
      }
      config.deliver = false
    end
  end

  def teardown
    @pool.shutdown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_transaction_succeeds_and_returns_result
    result = @pool.transaction do |producer|
      assert_kind_of WaterDrop::Producer, producer
      "success"
    end

    assert_equal "success", result
  end

  def test_transaction_raises_exception_from_block
    assert_raises(StandardError) do
      @pool.transaction do |_producer|
        raise StandardError, "test error"
      end
    end
  end
end

# Tests for WaterDrop::ConnectionPool.transaction (class method)
class WaterDropConnectionPoolClassTransactionConfiguredTest < WaterDropTest::Base
  def setup
    WaterDrop::ConnectionPool.setup(size: 2) do |config, index|
      config.kafka = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "transactional.id": "test-global-tx-#{index}"
      }
      config.deliver = false
    end
  end

  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_executes_transaction_using_global_pool
    result = WaterDrop::ConnectionPool.transaction do |producer|
      assert_kind_of WaterDrop::Producer, producer
      "global_success"
    end

    assert_equal "global_success", result
  end
end

class WaterDropConnectionPoolClassTransactionNotConfiguredTest < WaterDropTest::Base
  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_raises_error_when_not_configured
    assert_raises(RuntimeError) do
      WaterDrop::ConnectionPool.transaction { |_producer| nil }
    end
  end
end

# Tests for WaterDrop.transaction convenience method
class WaterDropTransactionConvenienceMethodTest < WaterDropTest::Base
  def setup
    WaterDrop::ConnectionPool.setup(size: 2) do |config, index|
      config.kafka = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "transactional.id": "test-convenience-tx-#{index}"
      }
      config.deliver = false
    end
  end

  def teardown
    WaterDrop::ConnectionPool.shutdown
    super
  end

  def test_delegates_to_connection_pool_transaction
    result = WaterDrop.transaction do |producer|
      assert_kind_of WaterDrop::Producer, producer
      "convenience_success"
    end

    assert_equal "convenience_success", result
  end
end
