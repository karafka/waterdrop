# frozen_string_literal: true

require "test_helper"

describe_current do
  before do
    @producer = described_class.new
    @_initial_producer = @producer
    @topic_name = "it-#{SecureRandom.uuid}"
  end

  after do
    @producer.close unless @producer.status.closed?
    # Close the initial producer if it was overridden by a nested before block
    if @_initial_producer != @producer
      @_initial_producer.close unless @_initial_producer.status.closed?
    end
  end

  describe "#initialize" do
    context "when we initialize without a setup" do
      it { @producer }

      it { refute_predicate(@producer.status, :active?) }
    end

    context "when initializing with setup" do
      before do
        @producer = described_class.new do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end
      end

      it { @producer }

      it { assert_predicate(@producer.status, :configured?) }
      it { assert_predicate(@producer.status, :active?) }
    end

    context "when initializing with oauth listener" do
      before do
        @listener = Class.new do
          def on_oauthbearer_token_refresh(_)
          end
        end

        @producer = described_class.new do |config|
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          config.oauth.token_provider_listener = @listener.new
        end
      end

      it { @producer }

      it { assert_predicate(@producer.status, :configured?) }
      it { assert_predicate(@producer.status, :active?) }
    end

    it "expect not to allow to disconnect" do
      refute(@producer.disconnect)
    end
  end

  describe "#setup" do
    context "when producer has already been configured" do
      before do
        @producer = build(:producer)
        @expected_error = WaterDrop::Errors::ProducerAlreadyConfiguredError
      end

      it { assert_raises(@expected_error) { @producer.setup { nil } } }
    end

    context "when producer was not yet configured" do
      before do
        @setup = lambda { |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        }
      end

      it { @producer.setup(&@setup) }
    end

    context "when idle_disconnect_timeout is zero (disabled)" do
      before do
        @producer.setup do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
          config.idle_disconnect_timeout = 0
        end
      end

      it "expect not to setup idle disconnector listener" do
        _ = @producer
      end
    end

    it "expect not to allow to disconnect" do
      refute(@producer.disconnect)
    end
  end

  describe "#client" do
    context "when producer is not configured" do
      before do
        @expected_error = WaterDrop::Errors::ProducerNotConfiguredError
      end

      it "expect not to allow to build client" do
        assert_raises(@expected_error) { @producer.client }
      end
    end

    context "when client is already connected" do
      before do
        @producer = build(:producer)
        @client = @producer.client
        @producer.client
      end

      after { @client.close }

      context "when called from a fork" do
        before do
          @expected_error = WaterDrop::Errors::ProducerUsedInParentProcess
        end

        # Simulates fork by changing the pid
        it do
          Process.stub(:pid, -1) do
            assert_raises(@expected_error) { @producer.client }
          end
        end
      end

      context "when called from the main process" do
        it { @client }
      end
    end

    context "when client is not initialized" do
      before do
        @producer = build(:producer)
      end

      context "when called from a fork" do
        it do
          Process.stub(:pid, -1) do
            @producer.client
          end
        end
      end

      context "when called from the main process" do
        it { @producer.client }
      end
    end
  end

  describe "#partition_count" do
    before do
      @producer = described_class.new do |config|
        config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      end
    end

    context "when topic does not exist" do
      before do
        @topic = "it-#{SecureRandom.uuid}"
      end

      it do
        @producer.partition_count(@topic)
        sleep(1)

        assert_equal(1, @producer.partition_count(@topic))
      rescue Rdkafka::RdkafkaError => e
        assert_kind_of(Rdkafka::RdkafkaError, e)
        assert_equal(:unknown_topic_or_part, e.code)
      end
    end

    context "when topic exists" do
      before do
        @topic = "it-#{SecureRandom.uuid}"
        @producer.produce_sync(topic: @topic, payload: "")
      end

      it { assert_equal(1, @producer.partition_count(@topic)) }
    end
  end

  describe "#queue_size" do
    context "when producer is not configured" do
      before do
        @producer = described_class.new
      end

      it { assert_equal(0, @producer.queue_size) }
      it { assert_equal(0, @producer.queue_length) }
    end

    context "when producer is configured but not connected" do
      before do
        @producer = build(:producer)
      end

      it { assert_equal(0, @producer.queue_size) }
    end

    context "when producer is connected with no pending messages" do
      before do
        @producer = build(:producer).tap(&:client)
      end

      it { assert_equal(0, @producer.queue_size) }
    end

    context "when producer is connected and message was produced synchronously" do
      before do
        @producer = build(:producer).tap(&:client)
        @producer.produce_sync(topic: @topic_name, payload: "test")
      end

      # After sync produce completes, queue should be empty
      it { assert_equal(0, @producer.queue_size) }
    end

    context "when producer is closed" do
      before do
        @producer = build(:producer).tap(&:client).tap(&:close)
      end

      it { assert_equal(0, @producer.queue_size) }
    end

    context "when called concurrently" do
      before do
        @producer = build(:producer).tap(&:client)
      end

      it "handles concurrent access safely" do
        threads = Array.new(10) do
          Thread.new { @producer.queue_size }
        end

        results = threads.map(&:value)

        results.each { |e| assert_kind_of(Integer, e) }
        results.each { |r| assert_operator(r, :>=, 0) }
      end
    end
  end

  describe "#idempotent?" do
    context "when producer is transactional" do
      before do
        @producer = build(:transactional_producer)
      end

      it { assert_predicate(@producer, :idempotent?) }
    end

    context "when it is a regular producer" do
      before do
        @producer = build(:producer)
      end

      it { refute_predicate(@producer, :idempotent?) }
    end

    context "when it is an idempotent producer" do
      before do
        @producer = build(:idempotent_producer)
      end

      it { assert_predicate(@producer, :idempotent?) }
    end

    context "when setting explicit enable.idempotence to false" do
      before do
        @producer = described_class.new do |config|
          config.kafka = { "enable.idempotence": false }
        end
      end

      it { refute_predicate(@producer, :idempotent?) }
    end
  end

  describe "#close" do
    context "when producer is already closed" do
      before do
        @producer = build(:producer).tap(&:client)
        @producer.close
      end

      it { @producer.close }

      it { assert_predicate(@producer.tap(&:close).status, :closed?) }
    end

    context "when producer was not yet closed" do
      before do
        @producer = build(:producer).tap(&:client)
      end

      it { @producer.close }

      it { assert_predicate(@producer.tap(&:close).status, :closed?) }
    end

    context "when there were messages in the buffer" do
      before do
        @producer = build(:producer).tap(&:client)
        @client = @producer.client
        @producer.buffer(build(:valid_message))
      end

      it do
        assert_equal(1, @producer.messages.size)
        @producer.close

        assert_equal(0, @producer.messages.size)
      end

      it "expect to close client since was open" do
        closed = false
        original_close = @client.method(:close)
        @client.stub(:close, -> {
          closed = true
          original_close.call
        }) do
          @producer.close
        end

        assert(closed)
      end
    end

    context "when producer was configured but not connected" do
      before do
        @producer = build(:producer)
      end

      it { assert_predicate(@producer.status, :configured?) }
      it { @producer.close }

      it { assert_predicate(@producer.tap(&:close).status, :closed?) }
    end

    context "when producer was configured and connected" do
      before do
        @producer = build(:producer).tap(&:client)
        @client = @producer.client
      end

      it { assert_predicate(@producer.status, :connected?) }
      it { @producer.close }

      it { assert_predicate(@producer.tap(&:close).status, :closed?) }
    end

    context "when flush timeouts" do
      before do
        @producer = build(:producer).tap(&:client)
        @message = build(:valid_message)
      end

      it "expect to close and not to raise any errors" do
        @producer.client.stub(:flush, ->(*) { raise Rdkafka::RdkafkaError.new(1) }) do
          @producer.close
        end
      end
    end

    context "when producer is already closed and we try to disconnect" do
      before { @producer.close }

      it "expect not to allow to disconnect" do
        refute(@producer.disconnect)
      end
    end
  end

  describe "#close!" do
    context "when producer cannot connect and dispatch messages" do
      before do
        @producer = build(
          :producer,
          kafka: { "bootstrap.servers": "localhost:9093" },
          max_wait_timeout: 1
        )
      end

      it "expect not to hang forever" do
        @producer.produce_async(topic: "na", payload: "data")
        @producer.close!
      end
    end
  end

  describe "#purge" do
    context "when there are some outgoing messages and we purge" do
      before do
        @producer = build(
          :producer,
          kafka: { "bootstrap.servers": "localhost:9093" },
          max_wait_timeout: 1
        )
      end

      after { @producer.close! }

      it "expect their deliveries to materialize with errors" do
        handler = @producer.produce_async(topic: "na", payload: "data")
        @producer.purge
        error = assert_raises(Rdkafka::RdkafkaError) { handler.wait }
        assert_match(/Purged in queue/, error.message)
      end

      context "when monitoring errors via instrumentation" do
        before do
          @detected = []

          @producer.monitor.subscribe("error.occurred") do |event|
            next unless event[:type] == "librdkafka.dispatch_error"

            @detected << event
          end
        end

        it "expect the error notifications to publish those errors" do
          handler = @producer.produce_async(topic: @topic_name, payload: "data", label: "test")
          @producer.purge

          handler.wait(raise_response_error: false)

          assert_equal(:purge_queue, @detected.first[:error].code)
          assert_equal("test", @detected.first[:label])
        end
      end
    end
  end

  describe "#ensure_usable!" do
    before do
      @producer = build(:producer)
    end

    context "when status is invalid" do
      before do
        @expected_error = WaterDrop::Errors::StatusInvalidError
      end

      it do
        @producer.status.stub(:configured?, false) do
          @producer.status.stub(:connected?, false) do
            @producer.status.stub(:initial?, false) do
              @producer.status.stub(:closing?, false) do
                @producer.status.stub(:closed?, false) do
                  assert_raises(@expected_error) { @producer.send(:ensure_active!) }
                end
              end
            end
          end
        end
      end
    end
  end

  describe "#tags" do
    before do
      @producer1 = build(:producer)
      @producer2 = build(:producer)
      @producer1.tags.add(:type, "transactional")
      @producer2.tags.add(:type, "regular")
    end

    after do
      @producer1.close
      @producer2.close
    end

    it { assert_equal(%w[transactional], @producer1.tags.to_a) }
    it { assert_equal(%w[regular], @producer2.tags.to_a) }
  end

  describe "#inspect" do
    context "when producer is not configured" do
      before do
        @producer = described_class.new
      end

      it { assert_includes(@producer.inspect, "WaterDrop::Producer") }
      it { assert_includes(@producer.inspect, "initial") }
      it { assert_includes(@producer.inspect, "buffer_size=0") }
      it { assert_includes(@producer.inspect, "id=nil") }
    end

    context "when producer is configured but not connected" do
      before do
        @producer = build(:producer)
      end

      it { assert_includes(@producer.inspect, "WaterDrop::Producer") }
      it { assert_includes(@producer.inspect, "configured") }
      it { assert_includes(@producer.inspect, "buffer_size=0") }
      it { assert_includes(@producer.inspect, "id=\"#{@producer.id}\"") }
    end

    context "when producer is connected" do
      before do
        @producer = build(:producer).tap(&:client)
      end

      it { assert_includes(@producer.inspect, "WaterDrop::Producer") }
      it { assert_includes(@producer.inspect, "connected") }
      it { assert_includes(@producer.inspect, "buffer_size=0") }
      it { assert_includes(@producer.inspect, "id=\"#{@producer.id}\"") }
    end

    context "when producer has messages in buffer" do
      before do
        @producer = build(:producer).tap(&:client)
        @message = build(:valid_message)
        @producer.buffer(@message)
      end

      it { assert_includes(@producer.inspect, "buffer_size=1") }

      context "when multiple messages are buffered" do
        before do
          @producer.buffer(@message)
          @producer.buffer(@message)
        end

        it { assert_includes(@producer.inspect, "buffer_size=3") }
      end
    end

    context "when buffer mutex is locked" do
      before do
        @producer = build(:producer).tap(&:client)
      end

      it "shows buffer as busy when mutex is locked" do
        @producer.instance_variable_get(:@buffer_mutex).lock

        assert_includes(@producer.inspect, "buffer_size=busy")

        @producer.instance_variable_get(:@buffer_mutex).unlock
      end
    end

    context "when producer is being closed" do
      before do
        @producer = build(:producer).tap(&:client)
      end

      it do
        @producer.status.stub(:to_s, "closing") do
          assert_includes(@producer.inspect, "closing")
        end
      end
    end

    context "when producer is closed" do
      before do
        @producer = build(:producer).tap(&:client).tap(&:close)
      end

      it { assert_includes(@producer.inspect, "closed") }
      it { assert_includes(@producer.inspect, "buffer_size=0") }
    end

    context "when called concurrently" do
      before do
        @producer = build(:producer).tap(&:client)
      end

      it "does not block or raise errors" do
        threads = Array.new(10) do
          Thread.new { @producer.inspect }
        end

        results = threads.map(&:value)

        results.each { |e| assert_kind_of(String, e) }
        results.each { |r| assert_includes(r, "WaterDrop::Producer") }
      end
    end

    context "when inspect is called during buffer operations" do
      before do
        @producer = build(:producer).tap(&:client)
        @message = build(:valid_message)
      end

      it "does not interfere with buffer operations" do
        # Start a thread that continuously inspects
        inspect_thread = Thread.new do
          100.times { @producer.inspect }
        end

        # Meanwhile, perform buffer operations
        10.times { @producer.buffer(@message) }
        @producer.flush_sync

        inspect_thread.join
      end
    end

    it "returns a string" do
      assert_kind_of(String, @producer.inspect)
    end

    it "does not call complex methods that could trigger side effects" do
      # Ensure inspect doesn't trigger client creation on a non-configured producer
      @producer.inspect
      # Producer should still be in initial state (no client created)
      refute_predicate(@producer.status, :connected?)
    end
  end

  describe "statistics callback hook" do
    before do
      @message = build(:valid_message)
    end

    context "when stats are emitted" do
      before do
        @producer = build(:producer)
        @events = []

        @producer.monitor.subscribe("statistics.emitted") do |event|
          @events << event
        end

        @producer.produce_sync(@message)

        sleep(0.001) while @events.size < 3
      end

      it { assert_equal("statistics.emitted", @events.last.id) }
      it { assert_equal(@producer.id, @events.last[:producer_id]) }
      it { assert_operator(@events.last[:statistics]["ts"], :>, 0) }
      # This is in microseconds. We needed a stable value for comparison, and the distance in
      # between statistics events should always be within 1ms (relaxed for slower CI like macOS)
      it do
        assert_operator(@events.last[:statistics]["ts_d"], :>=, 90_000)
        assert_operator(@events.last[:statistics]["ts_d"], :<=, 300_000)
      end
    end

    context "when we have a reconnected producer" do
      before do
        @producer = build(:producer)
      end

      it "expect to continue to receive statistics after reconnect" do
        switch = :before
        events = []

        @producer.monitor.subscribe("statistics.emitted") do |event|
          events << [event[:statistics].object_id, switch]
        end

        @producer.produce_sync(@message)

        sleep(2)

        @producer.disconnect
        switch = :disconnected

        sleep(2)

        switch = :reconnected
        @producer.produce_sync(@message)
        sleep(2)

        types = events.map(&:last)

        assert_includes(types, :before)
        assert_includes(types, :reconnected)
        refute_includes(types, :disconnected)
        # This ensures we do not get the same events twice after reconnecting
        assert_equal(events.map(&:first), events.map(&:first).uniq)
      end
    end

    context "when we have more producers" do
      before do
        @producer1 = build(:producer)
        @producer2 = build(:producer)
        @events1 = []
        @events2 = []

        @producer1.monitor.subscribe("statistics.emitted") do |event|
          @events1 << event
        end

        @producer2.monitor.subscribe("statistics.emitted") do |event|
          @events2 << event
        end

        @producer1.produce_sync(@message)
        @producer2.produce_sync(@message)

        # Wait for the error to occur
        sleep(0.001) while @events1.size < 2
        sleep(0.001) while @events2.size < 2
      end

      after do
        @producer1.close
        @producer2.close
      end

      it "expect not to have same statistics from both producers" do
        ids1 = @events1.map(&:payload).map { |payload| payload[:statistics] }.map(&:object_id)
        ids2 = @events2.map(&:payload).map { |payload| payload[:statistics] }.map(&:object_id)

        assert_empty(ids1 & ids2)
      end
    end
  end

  describe "error callback hook" do
    before do
      @message = build(:valid_message)
    end

    context "when error occurs" do
      before do
        @producer = build(:producer, kafka: { "bootstrap.servers": "localhost:9090" })
        @events = []

        @producer.monitor.subscribe("error.occurred") do |event|
          @events << event
        end

        # Forceful creation of a client will trigger a connection attempt
        @producer.client

        # Wait for the error to occur
        sleep(0.001) while @events.empty?
      end

      it "expect to emit proper stats" do
        assert_equal("error.occurred", @events.first.id)
        assert_equal(@producer.id, @events.first[:producer_id])
        assert_kind_of(Rdkafka::RdkafkaError, @events.first[:error])
      end
    end

    context "when we have more producers" do
      before do
        @producer1 = build(:producer, kafka: { "bootstrap.servers": "localhost:9090" })
        @producer2 = build(:producer, kafka: { "bootstrap.servers": "localhost:9090" })
        @events1 = []
        @events2 = []

        @producer1.monitor.subscribe("error.occurred") do |event|
          @events1 << event
        end

        @producer2.monitor.subscribe("error.occurred") do |event|
          @events2 << event
        end

        # Forceful creation of a client will trigger a connection attempt
        @producer1.client
        @producer2.client

        # Wait for the error to occur
        sleep(0.001) while @events1.empty?
        sleep(0.001) while @events2.empty?
      end

      after do
        @producer1.close
        @producer2.close
      end

      it "expect not to have same errors from both producers" do
        ids1 = @events1.map(&:payload).map { |payload| payload[:error] }.map(&:object_id)
        ids2 = @events2.map(&:payload).map { |payload| payload[:error] }.map(&:object_id)

        assert_empty(ids1 & ids2)
      end
    end
  end

  describe "producer with enable.idempotence true" do
    before do
      @producer = build(:idempotent_producer)
      @message = build(:valid_message)
    end

    it "expect to work as any producer without any exceptions" do
      @producer.produce_sync(@message)
    end
  end

  describe "fork integration spec" do
    before do
      @producer = build(:producer)
    end

    context "when producer not in use" do
      it "expect to work correctly" do
        fork do
          @producer.produce_sync(topic: @topic_name, payload: "1")
          @producer.close
          exit!(0)
        rescue
          exit!(1)
        end
        Process.wait

        assert_equal(0, $CHILD_STATUS.to_i)
      end
    end
  end

  # Since 30 seconds is minimum, they take a bit of time
  # We also test all producer cases at once to preserve time
  describe "disconnect integration specs" do
    before do
      @never_used = build(:producer, idle_disconnect_timeout: 30_000)
      @used_short = build(:producer, idle_disconnect_timeout: 30_000)
      @used = build(:producer, idle_disconnect_timeout: 30_000)
      @buffered = build(:producer, idle_disconnect_timeout: 30_000)
      @message = build(:valid_message)
    end

    after do
      @never_used.close
      @used_short.close
      @used.close
      @buffered.close
    end

    it "expect to correctly shutdown those that are in need of it" do
      _ = @never_used
      @buffered.buffer(@message)
      @used_short.produce_sync(@message)

      31.times do
        @used.produce_sync(@message)
        sleep(1)
      end

      # Give a bit of time for the instrumentation to kick in
      sleep(1)

      refute_predicate(@never_used.status, :disconnected?)
      refute_predicate(@used.status, :disconnected?)
      refute_predicate(@buffered.status, :disconnected?)
      assert_predicate(@used_short.status, :disconnected?)
    end
  end

  describe "#disconnectable?" do
    context "when producer is not configured" do
      it "expect not to be disconnectable" do
        refute_predicate(@producer, :disconnectable?)
      end
    end

    context "when producer is configured but not connected" do
      before do
        @producer.setup do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end
      end

      it "expect not to be disconnectable" do
        refute_predicate(@producer, :disconnectable?)
      end
    end

    context "when producer is connected" do
      before do
        @producer = build(:producer)
        # Initialize connection by accessing client
        @producer.client
      end

      it "expect to be disconnectable" do
        assert_predicate(@producer, :disconnectable?)
      end

      context "when producer has buffered messages" do
        before do
          @producer.buffer(build(:valid_message))
        end

        it "expect not to be disconnectable" do
          refute_predicate(@producer, :disconnectable?)
        end
      end

      context "when producer is closed" do
        before { @producer.close }

        it "expect not to be disconnectable" do
          refute_predicate(@producer, :disconnectable?)
        end
      end

      context "when producer is already disconnected" do
        before do
          # Disconnect the producer first
          @producer.disconnect
        end

        it "expect not to be disconnectable again" do
          refute_predicate(@producer, :disconnectable?)
        end
      end
    end

    context "when producer is in transaction" do
      before do
        @producer = build(:transactional_producer)
      end

      it "expect not to be disconnectable during transaction" do
        @producer.transaction do
          refute_predicate(@producer, :disconnectable?)
        end
      end
    end
  end
end
