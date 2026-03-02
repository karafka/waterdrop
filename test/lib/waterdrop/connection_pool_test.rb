# frozen_string_literal: true

require "test_helper"

describe_current do
  after { described_class.shutdown }

  before do
    @instrumentation = WaterDrop.instrumentation
  end

  describe "#initialize" do
    context "when connection_pool gem is available" do
      before do
        require "connection_pool"
      end

      it "creates a connection pool with default settings" do
        pool = described_class.new do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end

        assert_equal(5, pool.size)
        assert_equal(5, pool.available)
      end

      it "creates a connection pool with custom settings" do
        pool = described_class.new(size: 10, timeout: 3000) do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end

        assert_equal(10, pool.size)
        assert_equal(10, pool.available)
      end

      it "configures producers correctly in the pool" do
        pool = described_class.new(size: 2) do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          config.id = SecureRandom.uuid
        end

        pool.with do |producer|
          assert_kind_of(WaterDrop::Producer, producer)
          assert_predicate(producer.status, :configured?)
          refute(producer.config.deliver)
        end
      end

      it "supports per-producer configuration with index parameter" do
        producer_ids = []

        pool = described_class.new(size: 3) do |config, index|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          config.id = "producer-#{index}"
        end

        # Force all producers to be created by using them simultaneously
        mutex = Mutex.new
        threads = Array.new(3) do
          Thread.new do
            pool.with do |producer|
              mutex.synchronize { producer_ids << producer.id }
              sleep(0.1) # Hold the producer longer to force pool to create others
            end
          end
        end
        threads.each(&:join)

        assert_equal(3, producer_ids.uniq.size)
        assert_equal(%w[producer-1 producer-2 producer-3], producer_ids.sort)
      end

      it "supports transactional producers with unique transaction IDs" do
        transaction_ids = []

        pool = described_class.new(size: 2) do |config, index|
          config.deliver = false
          config.kafka = {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "transactional.id": "tx-#{index}"
          }
        end

        # Force all producers to be created by using them simultaneously
        mutex = Mutex.new
        threads = Array.new(2) do
          Thread.new do
            pool.with do |producer|
              tx_id = producer.config.kafka[:"transactional.id"]
              mutex.synchronize do
                transaction_ids << tx_id
              end
              sleep(0.1) # Hold the producer longer to force pool to create others
            end
          end
        end
        threads.each(&:join)

        assert_equal(2, transaction_ids.uniq.size)
        assert_equal(%w[tx-1 tx-2], transaction_ids.sort)
      end

      it "maintains backward compatibility with single-parameter blocks" do
        pool = described_class.new(size: 2) do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end

        pool.with do |producer|
          assert_kind_of(WaterDrop::Producer, producer)
          assert_predicate(producer.status, :configured?)
          refute(producer.config.deliver)
        end
      end
    end
  end

  describe "#with" do
    before do
      require "connection_pool"
      @pool = described_class.new(size: 2) do |config|
        config.deliver = false
        config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      end
    end

    it "yields a configured producer" do
      @pool.with do |producer|
        assert_kind_of(WaterDrop::Producer, producer)
        assert_predicate(producer.status, :configured?)
      end
    end

    it "returns the result of the block" do
      result = @pool.with { |producer| "producer-#{producer.id}" }

      assert_match(/producer-waterdrop-\w{12}/, result)
    end

    it "handles exceptions properly" do
      assert_raises(StandardError) do
        @pool.with { |_producer| raise StandardError, "test error" }
      end
    end

    it "ensures producer is returned to pool after exception" do
      assert_equal(2, @pool.available)

      begin
        @pool.with { |_producer| raise StandardError, "test error" }
      rescue
        # Expected
      end

      assert_equal(2, @pool.available)
    end
  end

  describe "#stats" do
    before do
      require "connection_pool"
      @pool = described_class.new(size: 3) do |config|
        config.deliver = false
        config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      end
    end

    it "returns pool statistics" do
      stats = @pool.stats

      assert_kind_of(Hash, stats)
      assert_equal(3, stats[:size])
      assert_equal(3, stats[:available])
    end

    it "reflects pool usage" do
      @pool.with do |_producer|
        stats = @pool.stats

        assert_equal(3, stats[:size])
        assert_equal(2, stats[:available])
      end
    end
  end

  describe "#shutdown" do
    before do
      require "connection_pool"
      @pool = described_class.new(size: 2) do |config|
        config.deliver = false
        config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      end
    end

    it "shuts down all producers in the pool" do
      # Access a producer to ensure it's created
      @pool.with(&:id)

      @pool.shutdown
    end

    it "calls close! on active producers during shutdown" do
      producers = []

      # Capture producers by using them
      2.times do
        @pool.with { |producer| producers << producer }
      end

      @pool.shutdown
    end

    it "handles inactive producers during shutdown" do
      @pool.shutdown
    end
  end

  describe "#close" do
    before do
      require "connection_pool"
      @pool = described_class.new(size: 2) do |config|
        config.deliver = false
        config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      end
    end

    it "is an alias for shutdown and behaves identically" do
      # Access a producer to ensure it's created
      @pool.with(&:id)

      @pool.close
    end

    it "calls close! on active producers during close (same as shutdown)" do
      producers = []

      # Capture producers by using them
      2.times do
        @pool.with { |producer| producers << producer }
      end

      @pool.close
    end

    it "handles inactive producers during close" do
      @pool.close
    end
  end

  describe "#reload" do
    before do
      require "connection_pool"
      @pool = described_class.new(size: 2) do |config|
        config.deliver = false
        config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      end
    end

    it "reloads all connections in the pool" do
      @pool.reload
    end
  end

  describe "#pool" do
    before do
      require "connection_pool"
      @pool = described_class.new do |config|
        config.deliver = false
      end
    end

    it "returns the underlying connection pool instance" do
      assert_kind_of(ConnectionPool, @pool.pool)
    end
  end

  describe "class methods" do
    before { require "connection_pool" }

    describe ".setup" do
      after { described_class.shutdown }

      it "sets up a global connection pool with default settings" do
        described_class.setup do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end

        assert_kind_of(described_class, described_class.default_pool)
        assert_equal(5, described_class.default_pool.size)
      end

      it "sets up a global connection pool with custom settings" do
        described_class.setup(size: 15, timeout: 3000) do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end

        assert_kind_of(described_class, described_class.default_pool)
        assert_equal(15, described_class.default_pool.size)
      end

      it "requires connection_pool gem when called" do
        described_class.setup { |config| config.deliver = false }
      end

      it "supports per-producer configuration with index in global setup" do
        described_class.setup(size: 2) do |config, index|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          config.id = "global-#{index}"
        end

        producer_ids = []

        # Force all producers to be created by using them simultaneously
        mutex = Mutex.new
        threads = Array.new(2) do
          Thread.new do
            described_class.with do |producer|
              mutex.synchronize { producer_ids << producer.id }
              sleep(0.1) # Hold the producer longer to force pool to create others
            end
          end
        end
        threads.each(&:join)

        assert_equal(2, producer_ids.uniq.size)
        assert_equal(%w[global-1 global-2], producer_ids.sort)
      end
    end

    describe ".with" do
      context "when global pool is configured" do
        before do
          described_class.setup(size: 3) do |config|
            config.deliver = false
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          end
        end

        after { described_class.shutdown }

        it "yields a producer from the global pool" do
          described_class.with do |producer|
            assert_kind_of(WaterDrop::Producer, producer)
            assert_predicate(producer.status, :configured?)
          end
        end

        it "returns the result of the block" do
          result = described_class.with { |producer| "global-#{producer.id}" }

          assert_match(/global-waterdrop-\w{12}/, result)
        end
      end

      context "when global pool is not configured" do
        it "raises an error" do
          error = assert_raises(RuntimeError) do
            described_class.with { |_producer| "test" }
          end
          assert_equal("No global connection pool configured. Call setup first.", error.message)
        end
      end
    end

    describe ".stats" do
      context "when global pool is configured" do
        before do
          described_class.setup(size: 7) do |config|
            config.deliver = false
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          end
        end

        after { described_class.shutdown }

        it "returns global pool statistics" do
          stats = described_class.stats

          assert_kind_of(Hash, stats)
          assert_equal(7, stats[:size])
          assert_equal(7, stats[:available])
        end

        it "reflects global pool usage" do
          described_class.with do |_producer|
            stats = described_class.stats

            assert_equal(7, stats[:size])
            assert_equal(6, stats[:available])
          end
        end
      end

      context "when global pool is not configured" do
        it "returns nil" do
          assert_nil(described_class.stats)
        end
      end

      context "when global pool becomes nil during operation" do
        it "handles nil pool gracefully" do
          described_class.setup do |config|
            config.deliver = false
          end

          described_class.shutdown

          # Should return nil after shutdown
          assert_nil(described_class.stats)
        end
      end
    end

    describe ".shutdown" do
      context "when global pool is configured" do
        before do
          described_class.setup do |config|
            config.deliver = false
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          end
        end

        it "shuts down the global pool" do
          described_class.shutdown

          assert_nil(described_class.default_pool)
        end

        it "allows shutdown to be called multiple times" do
          described_class.shutdown
          described_class.shutdown
        end
      end

      context "when global pool is not configured" do
        it "does not raise an error" do
          described_class.shutdown
        end
      end
    end

    describe ".close" do
      context "when global pool is configured" do
        before do
          described_class.setup do |config|
            config.deliver = false
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          end
        end

        it "is an alias for shutdown and closes the global pool" do
          described_class.close

          assert_nil(described_class.default_pool)
        end

        it "allows close to be called multiple times" do
          described_class.close
          described_class.close
        end
      end

      context "when global pool is not configured" do
        it "does not raise an error (same as shutdown)" do
          described_class.close
        end
      end
    end

    describe ".reload" do
      context "when global pool is configured" do
        before do
          described_class.setup do |config|
            config.deliver = false
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          end
        end

        after { described_class.shutdown }

        it "reloads the global pool" do
          described_class.reload
        end
      end

      context "when global pool is not configured" do
        it "does not raise an error" do
          described_class.reload
        end
      end

      context "when global pool is nil" do
        before do
          described_class.instance_variable_set(:@default_pool, nil)
        end

        it "handles nil pool gracefully with safe navigation" do
          described_class.reload
        end
      end
    end

    describe ".active?" do
      context "when global pool is not configured" do
        it "returns false" do
          refute_predicate(described_class, :active?)
        end
      end

      context "when global pool is configured" do
        before do
          described_class.setup do |config|
            config.deliver = false
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          end
        end

        after { described_class.shutdown }

        it "returns true" do
          assert_predicate(described_class, :active?)
        end
      end

      context "when after shutdown" do
        before do
          described_class.setup do |config|
            config.deliver = false
          end
          described_class.shutdown
        end

        it "returns false" do
          refute_predicate(described_class, :active?)
        end
      end
    end
  end

  describe "method delegation" do
    before do
      require "connection_pool"
      @pool = described_class.new(size: 3) do |config|
        config.deliver = false
      end
    end

    it "delegates size to underlying pool" do
      assert_equal(3, @pool.size)
    end

    it "delegates available to underlying pool" do
      assert_equal(3, @pool.available)
    end

    it "delegates with to underlying pool" do
      @pool.with do |producer|
        assert_kind_of(WaterDrop::Producer, producer)
      end
    end
  end

  describe "WaterDrop module convenience methods" do
    before { require "connection_pool" }

    describe "WaterDrop.with" do
      context "when global pool is configured" do
        before do
          described_class.setup do |config|
            config.deliver = false
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          end
        end

        after { described_class.shutdown }

        it "delegates to ConnectionPool.with" do
          WaterDrop.with do |producer|
            assert_kind_of(WaterDrop::Producer, producer)
            assert_predicate(producer.status, :configured?)
          end
        end

        it "returns the result of the block" do
          result = WaterDrop.with { |producer| "module-#{producer.id}" }

          assert_match(/module-waterdrop-\w{12}/, result)
        end
      end

      context "when global pool is not configured" do
        it "raises an error" do
          error = assert_raises(RuntimeError) do
            WaterDrop.with { |_producer| "test" }
          end
          assert_equal("No global connection pool configured. Call setup first.", error.message)
        end
      end
    end

    describe "WaterDrop.pool" do
      context "when global pool is configured" do
        before do
          described_class.setup do |config|
            config.deliver = false
          end
        end

        after { described_class.shutdown }

        it "returns the global connection pool" do
          assert_kind_of(described_class, WaterDrop.pool)
          assert_equal(described_class.default_pool, WaterDrop.pool)
        end
      end

      context "when global pool is not configured" do
        it "returns nil" do
          assert_nil(WaterDrop.pool)
        end
      end
    end
  end

  describe "concurrency" do
    before do
      require "connection_pool"
      @pool = described_class.new(size: 3) do |config|
        config.deliver = false
        config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      end
    end

    it "handles concurrent access correctly" do
      results = []
      threads = []

      5.times do |i|
        threads << Thread.new do
          @pool.with do |producer|
            sleep(0.01) # Simulate work
            results << "thread-#{i}-#{producer.id}"
          end
        end
      end

      threads.each(&:join)

      assert_equal(5, results.size)
      results.each { |r| assert_match(/thread-\d+-waterdrop-\w{12}/, r) }
    end

    it "respects pool size limits under concurrent load" do
      in_use_count = []
      threads = []
      mutex = Mutex.new

      # Start more threads than pool size
      8.times do
        threads << Thread.new do
          @pool.with do |_producer|
            mutex.synchronize { in_use_count << @pool.stats[:available] }
            sleep(0.05)
          end
        end
      end

      # Give threads time to start
      sleep(0.01)

      # Check that we never go below 0 available or above pool size
      threads.each(&:join)

      in_use_count.each { |c| assert_operator(c, :>=, 0) }
      in_use_count.each { |c| assert_operator(c, :<=, 3) }
    end
  end

  describe "error handling" do
    before do
      require "connection_pool"
      @pool = described_class.new(size: 2) do |config|
        config.deliver = false
        config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      end
    end

    it "handles producer configuration errors gracefully" do
      # Create a pool with invalid config - the error will occur when trying to use a producer
      broken_pool = described_class.new(size: 1) do |config|
        config.deliver = false
        config.max_payload_size = -1 # This will cause validation to fail during producer creation
      end

      assert_raises(WaterDrop::Errors::ConfigurationInvalidError) do
        broken_pool.with(&:id)
      end
    end

    it "maintains pool integrity when producer operations fail" do
      assert_equal(2, @pool.available)

      assert_raises(StandardError) do
        @pool.with do |_producer|
          # Simulate a producer operation failure
          raise StandardError, "Producer operation failed"
        end
      end

      # Pool should still be intact
      assert_equal(2, @pool.available)
    end

    context "when pool timeout is exceeded" do
      before do
        @small_pool = described_class.new(size: 1, timeout: 100) do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end
      end

      it "raises a timeout error when all connections are busy" do
        # Use Queue to synchronize - ensures thread1 has grabbed the connection
        # before main thread tries (eliminates race condition)
        grabbed = Queue.new

        thread1 = Thread.new do
          @small_pool.with do |_producer|
            grabbed << true
            sleep(0.3) # Hold connection longer than timeout
          end
        end

        grabbed.pop # Wait for thread1 to signal it has the connection

        assert_raises(ConnectionPool::TimeoutError) do
          @small_pool.with { |_producer| "should timeout" }
        end

        thread1.join
      end
    end
  end

  describe "fiber safety" do
    describe "fiber safety with async operations" do
      before do
        require "connection_pool"
        @fiber_pool = described_class.new(size: 3, timeout: 2000) do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end
      end

      it "handles fiber-based concurrent access correctly" do
        results = []
        fibers = []

        # Create multiple fibers that use the pool sequentially
        # Each fiber completes its work before yielding the connection
        5.times do |i|
          fibers << Fiber.new do
            @fiber_pool.with do |producer|
              results << "fiber-#{i}-#{producer.id}"
              producer.id
            end
          end
        end

        # Resume all fibers - they should complete successfully
        fibers.each(&:resume)

        assert_equal(5, results.size)
        results.each { |r| assert_match(/fiber-\d+-waterdrop-\w{12}/, r) }
      end

      it "maintains pool integrity with fiber yielding inside pool operations" do
        assert_equal(3, @fiber_pool.available)

        fiber = Fiber.new do
          @fiber_pool.with do |producer|
            assert_equal(2, @fiber_pool.available)

            # Yield control while holding a connection
            Fiber.yield(producer.id)

            # Still holding the same connection after yield
            assert_equal(2, @fiber_pool.available)
            producer.id
          end
        end

        producer_id1 = fiber.resume
        producer_id2 = fiber.resume

        # Same producer both times
        assert_equal(producer_id1, producer_id2)

        # Pool should be back to full capacity
        assert_equal(3, @fiber_pool.available)
      end

      it "handles exceptions in fibers correctly" do
        assert_equal(3, @fiber_pool.available)

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

        # Pool should still be intact after fiber exception
        assert_equal(3, @fiber_pool.available)
      end

      it "supports nested fiber operations with the pool" do
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

        assert_equal(3, results.size)
        assert_match(/outer-start-waterdrop-\w{12}/, results[0])
        assert_match(/inner-waterdrop-\w{12}/, results[1])
        assert_match(/outer-end-waterdrop-\w{12}/, results[2])

        # Different producers should be used
        refute_equal(outer_result, inner_result)
      end
    end

    describe "global pool fiber safety" do
      before do
        described_class.setup(size: 2, timeout: 1000) do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end
      end

      it "handles fiber-based access to global pool" do
        results = []
        fibers = []

        3.times do |i|
          fibers << Fiber.new do
            WaterDrop.with do |producer|
              results << "global-fiber-#{i}-#{producer.id}"
            end
          end
        end

        # Resume all fibers - they should complete successfully
        fibers.each(&:resume)

        assert_equal(3, results.size)
        results.each { |r| assert_match(/global-fiber-\d+-waterdrop-\w{12}/, r) }
      end

      it "maintains global pool stats consistency with fiber operations" do
        initial_stats = described_class.stats

        assert_equal(2, initial_stats[:size])
        assert_equal(2, initial_stats[:available])

        fiber = Fiber.new do
          WaterDrop.with do |producer|
            stats = described_class.stats

            assert_equal(2, stats[:size])
            assert_equal(1, stats[:available])

            Fiber.yield(producer.id)

            # Stats should still show one connection in use
            stats = described_class.stats

            assert_equal(2, stats[:size])
            assert_equal(1, stats[:available])
          end
        end

        fiber.resume # Start the fiber
        fiber.resume # Complete the fiber

        # After fiber completes, pool should be fully available
        final_stats = described_class.stats

        assert_equal(2, final_stats[:size])
        assert_equal(2, final_stats[:available])
      end
    end

    describe "mixed thread and fiber operations" do
      before do
        require "connection_pool"
        @mixed_pool = described_class.new(size: 4, timeout: 1000) do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end
      end

      it "handles mixed thread and fiber access patterns" do
        results = []
        mutex = Mutex.new

        # Start some threads
        threads = Array.new(2) do |i|
          Thread.new do
            @mixed_pool.with do |producer|
              sleep(0.01)
              mutex.synchronize { results << "thread-#{i}-#{producer.id}" }
            end
          end
        end

        # Start some fibers
        fibers = Array.new(2) do |i|
          Fiber.new do
            @mixed_pool.with do |producer|
              Fiber.yield
              mutex.synchronize { results << "fiber-#{i}-#{producer.id}" }
            end
          end
        end

        # Resume fibers
        fibers.each(&:resume)
        fibers.each(&:resume)

        # Wait for threads
        threads.each(&:join)

        assert_equal(4, results.size)
        thread_results = results.select { |r| r.start_with?("thread-") }
        fiber_results = results.select { |r| r.start_with?("fiber-") }

        assert_equal(2, thread_results.size)
        assert_equal(2, fiber_results.size)
      end

      it "maintains pool capacity limits with mixed concurrency" do
        # This test ensures that the pool respects size limits regardless of
        # whether access comes from threads or fibers

        available_counts = []
        mutex = Mutex.new

        operations = []

        # Create operations that will run concurrently
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

        # Start fibers
        operations.select { |op| op.is_a?(Fiber) }.each(&:resume)

        # Give operations time to start
        sleep(0.01)

        # Complete fibers
        operations.select { |op| op.is_a?(Fiber) }.each(&:resume)

        # Wait for threads
        operations.select { |op| op.is_a?(Thread) }.each(&:join)

        # All available counts should be valid (between 0 and pool size)
        available_counts.each do |c|
          assert_operator(c, :>=, 0)
          assert_operator(c, :<=, 4)
        end
      end
    end
  end

  describe "Connection Pool Events" do
    before do
      @events = []

      # Subscribe to all connection pool events
      @instrumentation.subscribe("connection_pool.created") { |event| @events << event }
      @instrumentation.subscribe("connection_pool.setup") { |event| @events << event }
      @instrumentation.subscribe("connection_pool.shutdown") { |event| @events << event }
      @instrumentation.subscribe("connection_pool.reload") { |event| @events << event }
      @instrumentation.subscribe("connection_pool.reloaded") { |event| @events << event }
    end

    describe "pool lifecycle events" do
      context "when creating a new pool" do
        it "emits connection_pool.created event" do
          pool = described_class.new(size: 2) do |config|
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
            config.deliver = false
          end

          created_event = @events.find { |e| e.id == "connection_pool.created" }

          refute_nil(created_event)
          assert_equal(pool, created_event[:pool])
          assert_equal(2, created_event[:size])
          assert_equal(5000, created_event[:timeout])
        end
      end

      context "when setting up global pool" do
        after do
          described_class.shutdown if described_class.active?
        end

        it "emits connection_pool.setup event" do
          pool = described_class.setup(size: 3, timeout: 10_000) do |config|
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
            config.deliver = false
          end

          setup_event = @events.find { |e| e.id == "connection_pool.setup" }

          refute_nil(setup_event)
          assert_equal(pool, setup_event[:pool])
          assert_equal(3, setup_event[:size])
          assert_equal(10_000, setup_event[:timeout])
        end
      end

      context "when shutting down global pool" do
        before do
          described_class.setup(size: 2) do |config|
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
            config.deliver = false
          end
        end

        it "emits connection_pool.shutdown event" do
          described_class.shutdown

          shutdown_event = @events.find { |e| e.id == "connection_pool.shutdown" }

          refute_nil(shutdown_event)
          refute_nil(shutdown_event[:pool])
        end
      end

      context "when reloading global pool" do
        before do
          described_class.setup(size: 2) do |config|
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
            config.deliver = false
          end
        end

        after do
          described_class.shutdown if described_class.active?
        end

        it "emits connection_pool.reload event" do
          described_class.reload

          reload_event = @events.find { |e| e.id == "connection_pool.reload" }

          refute_nil(reload_event)
          refute_nil(reload_event[:pool])
        end
      end

      context "when shutting down instance pool" do
        it "emits shutdown event" do
          pool = described_class.new(size: 2) do |config|
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
            config.deliver = false
          end

          pool.shutdown

          shutdown_event = @events.find { |e| e.id == "connection_pool.shutdown" }

          refute_nil(shutdown_event)
          assert_equal(pool, shutdown_event[:pool])
        end
      end

      context "when reloading instance pool" do
        it "emits reloaded event" do
          pool = described_class.new(size: 2) do |config|
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
            config.deliver = false
          end

          pool.reload

          reloaded_event = @events.find { |e| e.id == "connection_pool.reloaded" }

          refute_nil(reloaded_event)
          assert_equal(pool, reloaded_event[:pool])

          pool.shutdown
        end
      end
    end

    describe "event ordering" do
      it "emits events in the correct order for pool operations" do
        pool = described_class.new(size: 1) do |config|
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          config.deliver = false
        end

        pool.with { |producer| producer }
        pool.reload
        pool.shutdown

        event_ids = @events.map(&:id)

        # Pool creation comes first
        assert_equal("connection_pool.created", event_ids.first)

        # Reload event
        reload_index = event_ids.index("connection_pool.reloaded")

        assert_operator(reload_index, :>, 0)

        # Shutdown event comes last
        shutdown_index = event_ids.index("connection_pool.shutdown")

        assert_equal(event_ids.size - 1, shutdown_index)
      end
    end
  end

  describe "#transaction" do
    before do
      @pool = described_class.new(size: 2) do |config, index|
        config.kafka = {
          "bootstrap.servers": BOOTSTRAP_SERVERS,
          "transactional.id": "test-tx-#{index}"
        }
        config.deliver = false
      end
    end

    after { @pool.shutdown }

    context "when transaction succeeds" do
      it "executes the block within a transaction and returns result" do
        result = @pool.transaction do |producer|
          assert_kind_of(WaterDrop::Producer, producer)
          "success"
        end

        assert_equal("success", result)
      end
    end

    context "when transaction fails" do
      it "raises the exception from the block" do
        error_raised = StandardError.new("test error")

        assert_raises(StandardError) do
          @pool.transaction do |_producer|
            raise error_raised
          end
        end
      end
    end
  end

  describe ".transaction (class method)" do
    before do
      described_class.setup(size: 2) do |config, index|
        config.kafka = {
          "bootstrap.servers": BOOTSTRAP_SERVERS,
          "transactional.id": "test-global-tx-#{index}"
        }
        config.deliver = false
      end
    end

    after do
      described_class.shutdown if described_class.active?
    end

    context "when global pool is configured" do
      it "executes transaction using the global pool" do
        result = described_class.transaction do |producer|
          assert_kind_of(WaterDrop::Producer, producer)
          "global_success"
        end

        assert_equal("global_success", result)
      end
    end

    context "when global pool is not configured" do
      before { described_class.shutdown }

      it "raises an error" do
        error = assert_raises(RuntimeError) do
          described_class.transaction { |_producer| nil }
        end
        assert_equal("No global connection pool configured. Call setup first.", error.message)
      end
    end
  end

  describe "WaterDrop.transaction convenience method" do
    before do
      described_class.setup(size: 2) do |config, index|
        config.kafka = {
          "bootstrap.servers": BOOTSTRAP_SERVERS,
          "transactional.id": "test-convenience-tx-#{index}"
        }
        config.deliver = false
      end
    end

    after do
      described_class.shutdown if described_class.active?
    end

    it "delegates to ConnectionPool.transaction" do
      result = WaterDrop.transaction do |producer|
        assert_kind_of(WaterDrop::Producer, producer)
        "convenience_success"
      end

      assert_equal("convenience_success", result)
    end
  end
end
