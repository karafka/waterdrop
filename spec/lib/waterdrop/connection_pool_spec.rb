# frozen_string_literal: true

RSpec.describe_current do
  after { described_class.shutdown }

  let(:instrumentation) { WaterDrop.instrumentation }

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

        expect(pool.size).to eq(5)
        expect(pool.available).to eq(5)
      end

      it "creates a connection pool with custom settings" do
        pool = described_class.new(size: 10, timeout: 3000) do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end

        expect(pool.size).to eq(10)
        expect(pool.available).to eq(10)
      end

      it "configures producers correctly in the pool" do
        pool = described_class.new(size: 2) do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          config.id = SecureRandom.uuid
        end

        pool.with do |producer|
          expect(producer).to be_a(WaterDrop::Producer)
          expect(producer.status.configured?).to be(true)
          expect(producer.config.deliver).to be(false)
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

        expect(producer_ids.uniq.size).to eq(3)
        expect(producer_ids.sort).to eq(%w[producer-1 producer-2 producer-3])
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

        expect(transaction_ids.uniq.size).to eq(2)
        expect(transaction_ids.sort).to eq(%w[tx-1 tx-2])
      end

      it "maintains backward compatibility with single-parameter blocks" do
        pool = described_class.new(size: 2) do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end

        pool.with do |producer|
          expect(producer).to be_a(WaterDrop::Producer)
          expect(producer.status.configured?).to be(true)
          expect(producer.config.deliver).to be(false)
        end
      end
    end
  end

  describe "#with" do
    let(:pool) do
      described_class.new(size: 2) do |config|
        config.deliver = false
        config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      end
    end

    before { require "connection_pool" }

    it "yields a configured producer" do
      pool.with do |producer|
        expect(producer).to be_a(WaterDrop::Producer)
        expect(producer.status.configured?).to be(true)
      end
    end

    it "returns the result of the block" do
      result = pool.with { |producer| "producer-#{producer.id}" }
      expect(result).to match(/producer-waterdrop-\w{12}/)
    end

    it "handles exceptions properly" do
      expect do
        pool.with { |_producer| raise StandardError, "test error" }
      end.to raise_error(StandardError, "test error")
    end

    it "ensures producer is returned to pool after exception" do
      expect(pool.available).to eq(2)

      begin
        pool.with { |_producer| raise StandardError, "test error" }
      rescue
        # Expected
      end

      expect(pool.available).to eq(2)
    end
  end

  describe "#stats" do
    let(:pool) do
      described_class.new(size: 3) do |config|
        config.deliver = false
        config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      end
    end

    before { require "connection_pool" }

    it "returns pool statistics" do
      stats = pool.stats
      expect(stats).to be_a(Hash)
      expect(stats[:size]).to eq(3)
      expect(stats[:available]).to eq(3)
    end

    it "reflects pool usage" do
      pool.with do |_producer|
        stats = pool.stats
        expect(stats[:size]).to eq(3)
        expect(stats[:available]).to eq(2)
      end
    end
  end

  describe "#shutdown" do
    let(:pool) do
      described_class.new(size: 2) do |config|
        config.deliver = false
        config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      end
    end

    before { require "connection_pool" }

    it "shuts down all producers in the pool" do
      # Access a producer to ensure it's created
      pool.with(&:id)

      expect { pool.shutdown }.not_to raise_error
    end

    it "calls close! on active producers during shutdown" do
      producers = []

      # Capture producers by using them
      2.times do
        pool.with { |producer| producers << producer }
      end

      # Mock the close! method on the producers
      producers.each do |producer|
        allow(producer).to receive(:close!).and_call_original
        allow(producer.status).to receive(:active?).and_return(true)
      end

      pool.shutdown
    end

    it "handles inactive producers during shutdown" do
      # Create and capture a producer to mock specifically
      producer = nil
      pool.with { |p| producer = p }

      # Mock the specific producer's status to be inactive
      status = instance_double(WaterDrop::Producer::Status, active?: false)
      allow(producer).to receive(:status).and_return(status)

      expect { pool.shutdown }.not_to raise_error
    end

    it "handles nil status during shutdown" do
      # Create and capture a producer to mock specifically
      producer = nil
      pool.with { |p| producer = p }

      # Mock the specific producer's status to be nil - this covers the &. safe navigation
      allow(producer).to receive(:status).and_return(nil)

      expect { pool.shutdown }.not_to raise_error
    end
  end

  describe "#close" do
    let(:pool) do
      described_class.new(size: 2) do |config|
        config.deliver = false
        config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      end
    end

    before { require "connection_pool" }

    it "is an alias for shutdown and behaves identically" do
      # Access a producer to ensure it's created
      pool.with(&:id)

      expect { pool.close }.not_to raise_error
    end

    it "calls close! on active producers during close (same as shutdown)" do
      producers = []

      # Capture producers by using them
      2.times do
        pool.with { |producer| producers << producer }
      end

      # Mock the close! method on the producers
      producers.each do |producer|
        allow(producer).to receive(:close!).and_call_original
        allow(producer.status).to receive(:active?).and_return(true)
      end

      pool.close
    end

    it "handles inactive producers during close" do
      # Create and capture a producer to mock specifically
      producer = nil
      pool.with { |p| producer = p }

      # Mock the specific producer's status to be inactive
      status = instance_double(WaterDrop::Producer::Status, active?: false)
      allow(producer).to receive(:status).and_return(status)

      expect { pool.close }.not_to raise_error
    end

    it "handles nil status during close" do
      # Create and capture a producer to mock specifically
      producer = nil
      pool.with { |p| producer = p }

      # Mock the specific producer's status to be nil - this covers the &. safe navigation
      allow(producer).to receive(:status).and_return(nil)

      expect { pool.close }.not_to raise_error
    end
  end

  describe "#reload" do
    let(:pool) do
      described_class.new(size: 2) do |config|
        config.deliver = false
        config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      end
    end

    before { require "connection_pool" }

    it "reloads all connections in the pool" do
      expect { pool.reload }.not_to raise_error
    end

    it "handles inactive producers during reload" do
      # Create and capture a producer to mock specifically
      producer = nil
      pool.with { |p| producer = p }

      # Mock the specific producer's status to be inactive
      status = instance_double(WaterDrop::Producer::Status, active?: false)
      allow(producer).to receive(:status).and_return(status)

      expect { pool.reload }.not_to raise_error
    end

    it "handles nil status during reload" do
      # Create and capture a producer to mock specifically
      producer = nil
      pool.with { |p| producer = p }

      # Mock the specific producer's status to be nil - this covers the &. safe navigation
      allow(producer).to receive(:status).and_return(nil)

      expect { pool.reload }.not_to raise_error
    end
  end

  describe "#pool" do
    let(:pool) do
      described_class.new do |config|
        config.deliver = false
      end
    end

    before { require "connection_pool" }

    it "returns the underlying connection pool instance" do
      expect(pool.pool).to be_a(ConnectionPool)
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

        expect(described_class.default_pool).to be_a(described_class)
        expect(described_class.default_pool.size).to eq(5)
      end

      it "sets up a global connection pool with custom settings" do
        described_class.setup(size: 15, timeout: 3000) do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end

        expect(described_class.default_pool).to be_a(described_class)
        expect(described_class.default_pool.size).to eq(15)
      end

      it "requires connection_pool gem when called" do
        expect { described_class.setup { |config| config.deliver = false } }.not_to raise_error
      end

      it "raises LoadError when connection_pool gem is not available" do
        # Hide the ConnectionPool constant to simulate gem not being available
        hide_const("ConnectionPool")

        # Mock require to raise LoadError
        allow(described_class).to receive(:require).with("connection_pool").and_raise(LoadError)

        expect { described_class.setup { |config| config.deliver = false } }.to raise_error(
          LoadError,
          /WaterDrop::ConnectionPool requires the 'connection_pool' gem/
        )
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

        expect(producer_ids.uniq.size).to eq(2)
        expect(producer_ids.sort).to eq(%w[global-1 global-2])
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
            expect(producer).to be_a(WaterDrop::Producer)
            expect(producer.status.configured?).to be(true)
          end
        end

        it "returns the result of the block" do
          result = described_class.with { |producer| "global-#{producer.id}" }
          expect(result).to match(/global-waterdrop-\w{12}/)
        end
      end

      context "when global pool is not configured" do
        it "raises an error" do
          expect do
            described_class.with { |_producer| "test" }
          end.to raise_error(
            RuntimeError,
            "No global connection pool configured. Call setup first."
          )
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
          expect(stats).to be_a(Hash)
          expect(stats[:size]).to eq(7)
          expect(stats[:available]).to eq(7)
        end

        it "reflects global pool usage" do
          described_class.with do |_producer|
            stats = described_class.stats
            expect(stats[:size]).to eq(7)
            expect(stats[:available]).to eq(6)
          end
        end
      end

      context "when global pool is not configured" do
        it "returns nil" do
          expect(described_class.stats).to be_nil
        end
      end

      context "when global pool becomes nil during operation" do
        it "handles nil pool gracefully" do
          described_class.setup do |config|
            config.deliver = false
          end

          described_class.shutdown

          # Should return nil after shutdown
          expect(described_class.stats).to be_nil
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
          expect { described_class.shutdown }.not_to raise_error
          expect(described_class.default_pool).to be_nil
        end

        it "allows shutdown to be called multiple times" do
          described_class.shutdown
          expect { described_class.shutdown }.not_to raise_error
        end
      end

      context "when global pool is not configured" do
        it "does not raise an error" do
          expect { described_class.shutdown }.not_to raise_error
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
          expect { described_class.close }.not_to raise_error
          expect(described_class.default_pool).to be_nil
        end

        it "allows close to be called multiple times" do
          described_class.close
          expect { described_class.close }.not_to raise_error
        end
      end

      context "when global pool is not configured" do
        it "does not raise an error (same as shutdown)" do
          expect { described_class.close }.not_to raise_error
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
          expect { described_class.reload }.not_to raise_error
        end
      end

      context "when global pool is not configured" do
        it "does not raise an error" do
          expect { described_class.reload }.not_to raise_error
        end
      end

      context "when global pool is nil" do
        before do
          described_class.instance_variable_set(:@default_pool, nil)
        end

        it "handles nil pool gracefully with safe navigation" do
          expect { described_class.reload }.not_to raise_error
        end
      end
    end

    describe ".active?" do
      context "when global pool is not configured" do
        it "returns false" do
          expect(described_class.active?).to be(false)
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
          expect(described_class.active?).to be(true)
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
          expect(described_class.active?).to be(false)
        end
      end
    end
  end

  describe "method delegation" do
    let(:pool) do
      described_class.new(size: 3) do |config|
        config.deliver = false
      end
    end

    before { require "connection_pool" }

    it "delegates size to underlying pool" do
      expect(pool.size).to eq(3)
    end

    it "delegates available to underlying pool" do
      expect(pool.available).to eq(3)
    end

    it "delegates with to underlying pool" do
      expect { |b| pool.with(&b) }.to yield_with_args(WaterDrop::Producer)
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
            expect(producer).to be_a(WaterDrop::Producer)
            expect(producer.status.configured?).to be(true)
          end
        end

        it "returns the result of the block" do
          result = WaterDrop.with { |producer| "module-#{producer.id}" }
          expect(result).to match(/module-waterdrop-\w{12}/)
        end
      end

      context "when global pool is not configured" do
        it "raises an error" do
          expect do
            WaterDrop.with { |_producer| "test" }
          end.to raise_error(
            RuntimeError,
            "No global connection pool configured. Call setup first."
          )
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
          expect(WaterDrop.pool).to be_a(described_class)
          expect(WaterDrop.pool).to eq(described_class.default_pool)
        end
      end

      context "when global pool is not configured" do
        it "returns nil" do
          expect(WaterDrop.pool).to be_nil
        end
      end
    end
  end

  describe "concurrency" do
    let(:pool) do
      described_class.new(size: 3) do |config|
        config.deliver = false
        config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      end
    end

    before { require "connection_pool" }

    it "handles concurrent access correctly" do
      results = []
      threads = []

      5.times do |i|
        threads << Thread.new do
          pool.with do |producer|
            sleep(0.01) # Simulate work
            results << "thread-#{i}-#{producer.id}"
          end
        end
      end

      threads.each(&:join)

      expect(results.size).to eq(5)
      expect(results).to all(match(/thread-\d+-waterdrop-\w{12}/))
    end

    it "respects pool size limits under concurrent load" do
      in_use_count = []
      threads = []
      mutex = Mutex.new

      # Start more threads than pool size
      8.times do
        threads << Thread.new do
          pool.with do |_producer|
            mutex.synchronize { in_use_count << pool.stats[:available] }
            sleep(0.05)
          end
        end
      end

      # Give threads time to start
      sleep(0.01)

      # Check that we never go below 0 available or above pool size
      threads.each(&:join)

      expect(in_use_count).to all(be >= 0)
      expect(in_use_count).to all(be <= 3)
    end
  end

  describe "error handling" do
    let(:pool) do
      described_class.new(size: 2) do |config|
        config.deliver = false
        config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      end
    end

    before { require "connection_pool" }

    it "handles producer configuration errors gracefully" do
      # Create a pool with invalid config - the error will occur when trying to use a producer
      broken_pool = described_class.new(size: 1) do |config|
        config.deliver = false
        config.max_payload_size = -1 # This will cause validation to fail during producer creation
      end

      expect do
        broken_pool.with(&:id)
      end.to raise_error(WaterDrop::Errors::ConfigurationInvalidError)
    end

    it "maintains pool integrity when producer operations fail" do
      expect(pool.available).to eq(2)

      expect do
        pool.with do |_producer|
          # Simulate a producer operation failure
          raise StandardError, "Producer operation failed"
        end
      end.to raise_error(StandardError, "Producer operation failed")

      # Pool should still be intact
      expect(pool.available).to eq(2)
    end

    context "when pool timeout is exceeded" do
      let(:small_pool) do
        described_class.new(size: 1, timeout: 100) do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end
      end

      it "raises a timeout error when all connections are busy" do
        thread1 = Thread.new do
          small_pool.with do |_producer|
            sleep(0.2) # Hold connection longer than timeout
          end
        end

        sleep(0.01) # Give thread1 time to grab the connection

        expect do
          small_pool.with { |_producer| "should timeout" }
        end.to raise_error(ConnectionPool::TimeoutError)

        thread1.join
      end
    end
  end

  describe "fiber safety" do
    describe "fiber safety with async operations" do
      let(:fiber_pool) do
        described_class.new(size: 3, timeout: 2000) do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end
      end

      before { require "connection_pool" }

      it "handles fiber-based concurrent access correctly" do
        results = []
        fibers = []

        # Create multiple fibers that use the pool sequentially
        # Each fiber completes its work before yielding the connection
        5.times do |i|
          fibers << Fiber.new do
            fiber_pool.with do |producer|
              results << "fiber-#{i}-#{producer.id}"
              producer.id
            end
          end
        end

        # Resume all fibers - they should complete successfully
        fibers.each(&:resume)

        expect(results.size).to eq(5)
        expect(results).to all(match(/fiber-\d+-waterdrop-\w{12}/))
      end

      it "maintains pool integrity with fiber yielding inside pool operations" do
        expect(fiber_pool.available).to eq(3)

        fiber = Fiber.new do
          fiber_pool.with do |producer|
            expect(fiber_pool.available).to eq(2)

            # Yield control while holding a connection
            Fiber.yield(producer.id)

            # Still holding the same connection after yield
            expect(fiber_pool.available).to eq(2)
            producer.id
          end
        end

        producer_id1 = fiber.resume
        producer_id2 = fiber.resume

        # Same producer both times
        expect(producer_id1).to eq(producer_id2)

        # Pool should be back to full capacity
        expect(fiber_pool.available).to eq(3)
      end

      it "handles exceptions in fibers correctly" do
        expect(fiber_pool.available).to eq(3)

        fiber = Fiber.new do
          fiber_pool.with do |_producer|
            Fiber.yield
            raise StandardError, "Fiber error"
          end
        end

        fiber.resume

        expect do
          fiber.resume
        end.to raise_error(StandardError, "Fiber error")

        # Pool should still be intact after fiber exception
        expect(fiber_pool.available).to eq(3)
      end

      it "supports nested fiber operations with the pool" do
        results = []

        outer_fiber = Fiber.new do
          fiber_pool.with do |outer_producer|
            results << "outer-start-#{outer_producer.id}"

            inner_fiber = Fiber.new do
              fiber_pool.with do |inner_producer|
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

        expect(results.size).to eq(3)
        expect(results[0]).to match(/outer-start-waterdrop-\w{12}/)
        expect(results[1]).to match(/inner-waterdrop-\w{12}/)
        expect(results[2]).to match(/outer-end-waterdrop-\w{12}/)

        # Different producers should be used
        expect(outer_result).not_to eq(inner_result)
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

        expect(results.size).to eq(3)
        expect(results).to all(match(/global-fiber-\d+-waterdrop-\w{12}/))
      end

      it "maintains global pool stats consistency with fiber operations" do
        initial_stats = described_class.stats
        expect(initial_stats[:size]).to eq(2)
        expect(initial_stats[:available]).to eq(2)

        fiber = Fiber.new do
          WaterDrop.with do |producer|
            stats = described_class.stats
            expect(stats[:size]).to eq(2)
            expect(stats[:available]).to eq(1)

            Fiber.yield(producer.id)

            # Stats should still show one connection in use
            stats = described_class.stats
            expect(stats[:size]).to eq(2)
            expect(stats[:available]).to eq(1)
          end
        end

        fiber.resume # Start the fiber
        fiber.resume # Complete the fiber

        # After fiber completes, pool should be fully available
        final_stats = described_class.stats
        expect(final_stats[:size]).to eq(2)
        expect(final_stats[:available]).to eq(2)
      end
    end

    describe "mixed thread and fiber operations" do
      let(:mixed_pool) do
        described_class.new(size: 4, timeout: 1000) do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end
      end

      before { require "connection_pool" }

      it "handles mixed thread and fiber access patterns" do
        results = []
        mutex = Mutex.new

        # Start some threads
        threads = Array.new(2) do |i|
          Thread.new do
            mixed_pool.with do |producer|
              sleep(0.01)
              mutex.synchronize { results << "thread-#{i}-#{producer.id}" }
            end
          end
        end

        # Start some fibers
        fibers = Array.new(2) do |i|
          Fiber.new do
            mixed_pool.with do |producer|
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

        expect(results.size).to eq(4)
        thread_results = results.select { |r| r.start_with?("thread-") }
        fiber_results = results.select { |r| r.start_with?("fiber-") }

        expect(thread_results.size).to eq(2)
        expect(fiber_results.size).to eq(2)
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
            mixed_pool.with do |_producer|
              mutex.synchronize { available_counts << mixed_pool.available }
              sleep(0.05)
            end
          end
        end

        2.times do
          operations << Fiber.new do
            mixed_pool.with do |_producer|
              mutex.synchronize { available_counts << mixed_pool.available }
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
        expect(available_counts).to all(be_between(0, 4))
      end
    end
  end

  describe "Connection Pool Events" do
    let(:events) { [] }

    before do
      # Subscribe to all connection pool events
      instrumentation.subscribe("connection_pool.created") { |event| events << event }
      instrumentation.subscribe("connection_pool.setup") { |event| events << event }
      instrumentation.subscribe("connection_pool.shutdown") { |event| events << event }
      instrumentation.subscribe("connection_pool.reload") { |event| events << event }
      instrumentation.subscribe("connection_pool.reloaded") { |event| events << event }
    end

    describe "pool lifecycle events" do
      context "when creating a new pool" do
        let(:pool) do
          described_class.new(size: 2) do |config|
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
            config.deliver = false
          end
        end

        it "emits connection_pool.created event" do
          pool
          created_event = events.find { |e| e.id == "connection_pool.created" }

          expect(created_event).not_to be_nil
          expect(created_event[:pool]).to eq(pool)
          expect(created_event[:size]).to eq(2)
          expect(created_event[:timeout]).to eq(5000)
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

          setup_event = events.find { |e| e.id == "connection_pool.setup" }

          expect(setup_event).not_to be_nil
          expect(setup_event[:pool]).to eq(pool)
          expect(setup_event[:size]).to eq(3)
          expect(setup_event[:timeout]).to eq(10_000)
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

          shutdown_event = events.find { |e| e.id == "connection_pool.shutdown" }

          expect(shutdown_event).not_to be_nil
          expect(shutdown_event[:pool]).not_to be_nil
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

          reload_event = events.find { |e| e.id == "connection_pool.reload" }

          expect(reload_event).not_to be_nil
          expect(reload_event[:pool]).not_to be_nil
        end
      end

      context "when shutting down instance pool" do
        let(:pool) do
          described_class.new(size: 2) do |config|
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
            config.deliver = false
          end
        end

        it "emits shutdown event" do
          pool.shutdown

          shutdown_event = events.find { |e| e.id == "connection_pool.shutdown" }

          expect(shutdown_event).not_to be_nil
          expect(shutdown_event[:pool]).to eq(pool)
        end
      end

      context "when reloading instance pool" do
        let(:pool) do
          described_class.new(size: 2) do |config|
            config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
            config.deliver = false
          end
        end

        after { pool.shutdown }

        it "emits reloaded event" do
          pool.reload

          reloaded_event = events.find { |e| e.id == "connection_pool.reloaded" }

          expect(reloaded_event).not_to be_nil
          expect(reloaded_event[:pool]).to eq(pool)
        end
      end
    end

    describe "event ordering" do
      let(:pool) do
        described_class.new(size: 1) do |config|
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          config.deliver = false
        end
      end

      it "emits events in the correct order for pool operations" do
        pool.with { |producer| producer }
        pool.reload
        pool.shutdown

        event_ids = events.map(&:id)

        # Pool creation comes first
        expect(event_ids.first).to eq("connection_pool.created")

        # Reload event
        reload_index = event_ids.index("connection_pool.reloaded")
        expect(reload_index).to be > 0

        # Shutdown event comes last
        shutdown_index = event_ids.index("connection_pool.shutdown")
        expect(shutdown_index).to eq(event_ids.size - 1)
      end
    end
  end

  describe "#transaction" do
    let(:pool) do
      described_class.new(size: 2) do |config, index|
        config.kafka = {
          "bootstrap.servers": BOOTSTRAP_SERVERS,
          "transactional.id": "test-tx-#{index}"
        }
        config.deliver = false
      end
    end

    after { pool.shutdown }

    context "when transaction succeeds" do
      it "executes the block within a transaction and returns result" do
        result = pool.transaction do |producer|
          expect(producer).to be_a(WaterDrop::Producer)
          "success"
        end

        expect(result).to eq("success")
      end
    end

    context "when transaction fails" do
      it "raises the exception from the block" do
        error_raised = StandardError.new("test error")

        expect do
          pool.transaction do |_producer|
            raise error_raised
          end
        end.to raise_error(StandardError, "test error")
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
          expect(producer).to be_a(WaterDrop::Producer)
          "global_success"
        end

        expect(result).to eq("global_success")
      end
    end

    context "when global pool is not configured" do
      before { described_class.shutdown }

      it "raises an error" do
        expect do
          described_class.transaction { |_producer| nil }
        end.to raise_error(RuntimeError, "No global connection pool configured. Call setup first.")
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
        expect(producer).to be_a(WaterDrop::Producer)
        "convenience_success"
      end

      expect(result).to eq("convenience_success")
    end
  end
end
