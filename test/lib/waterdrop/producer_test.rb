# frozen_string_literal: true

# Tests for WaterDrop::Producer#initialize
class WaterDropProducerInitializeWithoutSetupTest < WaterDropTest::Base
  def setup
    @producer = WaterDrop::Producer.new
  end

  def teardown
    @producer.close
    super
  end

  def test_initialize_without_setup_does_not_raise
    WaterDrop::Producer.new.tap(&:close)
  end

  def test_status_is_not_active_without_setup
    assert_same false, @producer.status.active?
  end

  def test_not_allow_to_disconnect
    assert_same false, @producer.disconnect
  end
end

class WaterDropProducerInitializeWithSetupTest < WaterDropTest::Base
  def setup
    @producer = WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    @producer.close
    super
  end

  def test_initialize_with_setup_does_not_raise
    # Producer was created in setup without error
    assert_kind_of WaterDrop::Producer, @producer
  end

  def test_status_is_configured
    assert_same true, @producer.status.configured?
  end

  def test_status_is_active
    assert_same true, @producer.status.active?
  end
end

class WaterDropProducerInitializeWithOauthListenerTest < WaterDropTest::Base
  def setup
    listener_class = Class.new do
      def on_oauthbearer_token_refresh(_)
      end
    end

    @producer = WaterDrop::Producer.new do |config|
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.oauth.token_provider_listener = listener_class.new
    end
  end

  def teardown
    @producer.close
    super
  end

  def test_initialize_with_oauth_listener_does_not_raise
    assert_kind_of WaterDrop::Producer, @producer
  end

  def test_status_is_configured
    assert_same true, @producer.status.configured?
  end

  def test_status_is_active
    assert_same true, @producer.status.active?
  end
end

# Tests for WaterDrop::Producer#setup
class WaterDropProducerSetupAlreadyConfiguredTest < WaterDropTest::Base
  def setup
    @producer = build(:producer)
  end

  def teardown
    @producer.close
    super
  end

  def test_raises_already_configured_error
    assert_raises(WaterDrop::Errors::ProducerAlreadyConfiguredError) do
      @producer.setup { nil }
    end
  end
end

class WaterDropProducerSetupNotYetConfiguredTest < WaterDropTest::Base
  def setup
    @producer = WaterDrop::Producer.new
  end

  def teardown
    @producer.close
    super
  end

  def test_setup_does_not_raise
    @producer.setup do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def test_idle_disconnect_timeout_zero_does_not_raise
    @producer.setup do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      config.idle_disconnect_timeout = 0
    end
  end

  def test_not_allow_to_disconnect_before_setup
    assert_same false, @producer.disconnect
  end
end

# Tests for WaterDrop::Producer#client
class WaterDropProducerClientNotConfiguredTest < WaterDropTest::Base
  def setup
    @producer = WaterDrop::Producer.new
  end

  def teardown
    @producer.close
    super
  end

  def test_raises_not_configured_error
    assert_raises(WaterDrop::Errors::ProducerNotConfiguredError) do
      @producer.client
    end
  end
end

class WaterDropProducerClientAlreadyConnectedTest < WaterDropTest::Base
  def setup
    @producer = build(:producer)
    @client = @producer.client
    @producer.client
  end

  def teardown
    @producer.close
    super
  end

  def test_from_fork_raises_used_in_parent_process
    Process.stub(:pid, -1) do
      assert_raises(WaterDrop::Errors::ProducerUsedInParentProcess) do
        @producer.client
      end
    end
  end

  def test_from_main_process_does_not_raise
    @producer.client
  end
end

class WaterDropProducerClientNotInitializedTest < WaterDropTest::Base
  def setup
    @producer = build(:producer)
  end

  def teardown
    @producer.close
    super
  end

  def test_from_fork_does_not_raise
    Process.stub(:pid, -1) do
      @producer.client
    end
  end

  def test_from_main_process_does_not_raise
    @producer.client
  end
end

# Tests for WaterDrop::Producer#partition_count
class WaterDropProducerPartitionCountTest < WaterDropTest::Base
  def setup
    @producer = WaterDrop::Producer.new do |config|
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    @producer.close
    super
  end

  def test_topic_does_not_exist
    topic = "it-#{SecureRandom.uuid}"

    @producer.partition_count(topic)
    sleep(1)

    assert_equal 1, @producer.partition_count(topic)
  rescue Rdkafka::RdkafkaError => e
    assert_kind_of Rdkafka::RdkafkaError, e
    assert_equal :unknown_topic_or_part, e.code
  end

  def test_topic_exists
    topic = "it-#{SecureRandom.uuid}"
    @producer.produce_sync(topic: topic, payload: "")

    assert_equal 1, @producer.partition_count(topic)
  end
end

# Tests for WaterDrop::Producer#queue_size
class WaterDropProducerQueueSizeNotConfiguredTest < WaterDropTest::Base
  def setup
    @producer = WaterDrop::Producer.new
  end

  def teardown
    @producer.close
    super
  end

  def test_queue_size_is_zero
    assert_equal 0, @producer.queue_size
  end

  def test_queue_length_is_zero
    assert_equal 0, @producer.queue_length
  end
end

class WaterDropProducerQueueSizeConfiguredTest < WaterDropTest::Base
  def teardown
    @producer.close
    super
  end

  def test_configured_but_not_connected
    @producer = build(:producer)

    assert_equal 0, @producer.queue_size
  end

  def test_connected_with_no_pending_messages
    @producer = build(:producer).tap(&:client)

    assert_equal 0, @producer.queue_size
  end

  def test_after_sync_produce_queue_is_empty
    @producer = build(:producer).tap(&:client)
    topic_name = "it-#{SecureRandom.uuid}"
    @producer.produce_sync(topic: topic_name, payload: "test")

    assert_equal 0, @producer.queue_size
  end

  def test_closed_producer_queue_size_is_zero
    @producer = build(:producer).tap(&:client).tap(&:close)

    assert_equal 0, @producer.queue_size
  end

  def test_concurrent_access_safely
    @producer = build(:producer).tap(&:client)
    threads = Array.new(10) do
      Thread.new { @producer.queue_size }
    end

    results = threads.map(&:value)

    results.each { |r| assert_kind_of Integer, r }
    results.each { |r| assert_operator r, :>=, 0 }
  end
end

# Tests for WaterDrop::Producer#idempotent?
class WaterDropProducerIdempotentTest < WaterDropTest::Base
  def teardown
    @producer.close
    super
  end

  def test_transactional_producer_is_idempotent
    @producer = build(:transactional_producer)

    assert_same true, @producer.idempotent?
  end

  def test_regular_producer_is_not_idempotent
    @producer = build(:producer)

    assert_same false, @producer.idempotent?
  end

  def test_idempotent_producer_is_idempotent
    @producer = build(:idempotent_producer)

    assert_same true, @producer.idempotent?
  end

  def test_explicit_idempotence_false
    @producer = WaterDrop::Producer.new do |config|
      config.kafka = { "enable.idempotence": false }
    end

    assert_same false, @producer.idempotent?
  end
end

# Tests for WaterDrop::Producer#close
class WaterDropProducerCloseAlreadyClosedTest < WaterDropTest::Base
  def setup
    @producer = build(:producer).tap(&:client)
    @producer.close
  end

  def test_close_again_does_not_raise
    @producer.close
  end

  def test_status_is_closed_after_double_close
    @producer.close

    assert_same true, @producer.status.closed?
  end
end

class WaterDropProducerCloseNotYetClosedTest < WaterDropTest::Base
  def setup
    @producer = build(:producer).tap(&:client)
  end

  def test_close_does_not_raise
    @producer.close
  end

  def test_status_is_closed_after_close
    @producer.close

    assert_same true, @producer.status.closed?
  end
end

class WaterDropProducerCloseWithBufferedMessagesTest < WaterDropTest::Base
  def setup
    @producer = build(:producer).tap(&:client)
    @producer.buffer(build(:valid_message))
  end

  def test_close_clears_buffer
    assert_equal 1, @producer.messages.size
    @producer.close

    assert_equal 0, @producer.messages.size
  end

  def test_close_closes_client
    # Verify the producer is connected before close
    assert_same true, @producer.status.connected?
    @producer.close

    assert_same true, @producer.status.closed?
  end
end

class WaterDropProducerCloseConfiguredButNotConnectedTest < WaterDropTest::Base
  def setup
    @producer = build(:producer)
  end

  def test_status_is_configured
    assert_same true, @producer.status.configured?
  end

  def test_close_does_not_raise
    @producer.close
  end

  def test_status_is_closed_after_close
    @producer.close

    assert_same true, @producer.status.closed?
  end

  def test_close_does_not_create_client
    # The producer should go from configured to closed without connecting
    assert_same true, @producer.status.configured?
    @producer.close

    assert_same true, @producer.status.closed?
  end
end

class WaterDropProducerCloseConfiguredAndConnectedTest < WaterDropTest::Base
  def setup
    @producer = build(:producer).tap(&:client)
  end

  def test_status_is_connected
    assert_same true, @producer.status.connected?
  end

  def test_close_does_not_raise
    @producer.close
  end

  def test_status_is_closed_after_close
    @producer.close

    assert_same true, @producer.status.closed?
  end
end

class WaterDropProducerCloseFlushTimeoutsTest < WaterDropTest::Base
  def setup
    @producer = build(:producer).tap(&:client)
  end

  def teardown
    # Ensure producer is closed even if test fails
    @producer.close rescue nil # rubocop:disable Style/RescueModifier
    super
  end

  def test_close_does_not_raise_on_flush_timeout
    @producer.client.stub(:flush, ->(*) { raise Rdkafka::RdkafkaError.new(1) }) do
      @producer.close
    end
  end
end

class WaterDropProducerCloseAlreadyClosedDisconnectTest < WaterDropTest::Base
  def setup
    @producer = WaterDrop::Producer.new
    @producer.close
  end

  def test_not_allow_to_disconnect
    assert_same false, @producer.disconnect
  end
end

# Tests for WaterDrop::Producer#close!
class WaterDropProducerCloseBangTest < WaterDropTest::Base
  def test_does_not_hang_forever
    producer = build(
      :producer,
      kafka: { "bootstrap.servers": "localhost:9093" },
      max_wait_timeout: 1
    )
    producer.produce_async(topic: "na", payload: "data")
    producer.close!
  end
end

# Tests for WaterDrop::Producer#purge
class WaterDropProducerPurgeTest < WaterDropTest::Base
  def setup
    @producer = build(
      :producer,
      kafka: { "bootstrap.servers": "localhost:9093" },
      max_wait_timeout: 1
    )
  end

  def teardown
    @producer.close!
    super
  end

  def test_purged_deliveries_have_errors
    handler = @producer.produce_async(topic: "na", payload: "data")
    @producer.purge
    assert_raises(Rdkafka::RdkafkaError) { handler.wait }
  end

  def test_purge_error_notifications_publish
    detected = []
    topic_name = "it-#{SecureRandom.uuid}"

    @producer.monitor.subscribe("error.occurred") do |event|
      next unless event[:type] == "librdkafka.dispatch_error"

      detected << event
    end

    handler = @producer.produce_async(topic: topic_name, payload: "data", label: "test")
    @producer.purge

    handler.wait(raise_response_error: false)

    assert_equal :purge_queue, detected.first[:error].code
    assert_equal "test", detected.first[:label]
  end
end

# Tests for WaterDrop::Producer#ensure_usable!
class WaterDropProducerEnsureUsableTest < WaterDropTest::Base
  def setup
    @producer = build(:producer)
  end

  def teardown
    @producer.close
    super
  end

  def test_raises_status_invalid_error_when_status_is_invalid
    @producer.status.stub(:configured?, false) do
      @producer.status.stub(:connected?, false) do
        @producer.status.stub(:initial?, false) do
          @producer.status.stub(:closing?, false) do
            @producer.status.stub(:closed?, false) do
              assert_raises(WaterDrop::Errors::StatusInvalidError) do
                @producer.send(:ensure_active!)
              end
            end
          end
        end
      end
    end
  end
end

# Tests for WaterDrop::Producer#tags
class WaterDropProducerTagsTest < WaterDropTest::Base
  def setup
    @producer1 = build(:producer)
    @producer2 = build(:producer)
    @producer1.tags.add(:type, "transactional")
    @producer2.tags.add(:type, "regular")
  end

  def teardown
    @producer1.close
    @producer2.close
    super
  end

  def test_producer1_tags
    assert_equal %w[transactional], @producer1.tags.to_a
  end

  def test_producer2_tags
    assert_equal %w[regular], @producer2.tags.to_a
  end
end

# Tests for WaterDrop::Producer#inspect
class WaterDropProducerInspectNotConfiguredTest < WaterDropTest::Base
  def setup
    @producer = WaterDrop::Producer.new
  end

  def teardown
    @producer.close
    super
  end

  def test_includes_class_name
    assert_includes @producer.inspect, "WaterDrop::Producer"
  end

  def test_includes_initial_status
    assert_includes @producer.inspect, "initial"
  end

  def test_includes_buffer_size_zero
    assert_includes @producer.inspect, "buffer_size=0"
  end

  def test_includes_id_nil
    assert_includes @producer.inspect, "id=nil"
  end

  def test_returns_a_string
    assert_kind_of String, @producer.inspect
  end
end

class WaterDropProducerInspectConfiguredNotConnectedTest < WaterDropTest::Base
  def setup
    @producer = build(:producer)
  end

  def teardown
    @producer.close
    super
  end

  def test_includes_class_name
    assert_includes @producer.inspect, "WaterDrop::Producer"
  end

  def test_includes_configured_status
    assert_includes @producer.inspect, "configured"
  end

  def test_includes_buffer_size_zero
    assert_includes @producer.inspect, "buffer_size=0"
  end

  def test_includes_id
    assert_includes @producer.inspect, "id=\"#{@producer.id}\""
  end
end

class WaterDropProducerInspectConnectedTest < WaterDropTest::Base
  def setup
    @producer = build(:producer).tap(&:client)
  end

  def teardown
    @producer.close
    super
  end

  def test_includes_class_name
    assert_includes @producer.inspect, "WaterDrop::Producer"
  end

  def test_includes_connected_status
    assert_includes @producer.inspect, "connected"
  end

  def test_includes_buffer_size_zero
    assert_includes @producer.inspect, "buffer_size=0"
  end

  def test_includes_id
    assert_includes @producer.inspect, "id=\"#{@producer.id}\""
  end
end

class WaterDropProducerInspectWithBufferedMessagesTest < WaterDropTest::Base
  def setup
    @producer = build(:producer).tap(&:client)
    @message = build(:valid_message)
    @producer.buffer(@message)
  end

  def teardown
    @producer.close
    super
  end

  def test_includes_buffer_size_one
    assert_includes @producer.inspect, "buffer_size=1"
  end

  def test_includes_buffer_size_three_with_multiple_messages
    @producer.buffer(@message)
    @producer.buffer(@message)

    assert_includes @producer.inspect, "buffer_size=3"
  end
end

class WaterDropProducerInspectBufferMutexLockedTest < WaterDropTest::Base
  def setup
    @producer = build(:producer).tap(&:client)
  end

  def teardown
    @producer.close
    super
  end

  def test_shows_buffer_busy_when_mutex_locked
    @producer.instance_variable_get(:@buffer_mutex).lock

    assert_includes @producer.inspect, "buffer_size=busy"
    @producer.instance_variable_get(:@buffer_mutex).unlock
  end
end

class WaterDropProducerInspectClosingTest < WaterDropTest::Base
  def setup
    @producer = build(:producer).tap(&:client)
  end

  def teardown
    @producer.close
    super
  end

  def test_includes_closing_status
    @producer.status.stub(:to_s, "closing") do
      assert_includes @producer.inspect, "closing"
    end
  end
end

class WaterDropProducerInspectClosedTest < WaterDropTest::Base
  def setup
    @producer = build(:producer).tap(&:client).tap(&:close)
  end

  def test_includes_closed_status
    assert_includes @producer.inspect, "closed"
  end

  def test_includes_buffer_size_zero
    assert_includes @producer.inspect, "buffer_size=0"
  end
end

class WaterDropProducerInspectConcurrentTest < WaterDropTest::Base
  def setup
    @producer = build(:producer).tap(&:client)
  end

  def teardown
    @producer.close
    super
  end

  def test_concurrent_inspect_does_not_block_or_raise
    threads = Array.new(10) do
      Thread.new { @producer.inspect }
    end

    results = threads.map(&:value)

    results.each { |r| assert_kind_of String, r }
    results.each { |r| assert_includes r, "WaterDrop::Producer" }
  end
end

class WaterDropProducerInspectDuringBufferOperationsTest < WaterDropTest::Base
  def setup
    @producer = build(:producer).tap(&:client)
    @message = build(:valid_message)
  end

  def teardown
    @producer.close
    super
  end

  def test_does_not_interfere_with_buffer_operations
    inspect_thread = Thread.new do
      100.times { @producer.inspect }
    end

    10.times { @producer.buffer(@message) }
    @producer.flush_sync

    inspect_thread.join
  end
end

class WaterDropProducerInspectNoSideEffectsTest < WaterDropTest::Base
  def setup
    @producer = WaterDrop::Producer.new
  end

  def teardown
    @producer.close
    super
  end

  def test_does_not_trigger_client_creation
    # Inspect a non-configured producer - it should not trigger client creation
    result = @producer.inspect

    assert_kind_of String, result
    # Producer should still be in initial state
    assert_same false, @producer.status.active?
  end
end

# Tests for statistics callback hook
class WaterDropProducerStatisticsCallbackTest < WaterDropTest::Base
  def setup
    @producer = build(:producer)
    @events = []

    @producer.monitor.subscribe("statistics.emitted") do |event|
      @events << event
    end

    @producer.produce_sync(build(:valid_message))
    sleep(0.001) while @events.size < 3
  end

  def teardown
    @producer.close
    super
  end

  def test_event_id_is_statistics_emitted
    assert_equal "statistics.emitted", @events.last.id
  end

  def test_event_producer_id_matches
    assert_equal @producer.id, @events.last[:producer_id]
  end

  def test_statistics_ts_is_positive
    assert_operator @events.last[:statistics]["ts"], :>, 0
  end

  def test_statistics_ts_d_is_in_expected_range
    assert_includes 90_000..300_000, @events.last[:statistics]["ts_d"]
  end
end

class WaterDropProducerStatisticsReconnectedTest < WaterDropTest::Base
  def setup
    @producer = build(:producer)
    @message = build(:valid_message)
  end

  def teardown
    @producer.close
    super
  end

  def test_continues_to_receive_statistics_after_reconnect
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

    assert_includes types, :before
    assert_includes types, :reconnected
    refute_includes types, :disconnected
    assert_equal events.map(&:first), events.map(&:first).uniq
  end
end

class WaterDropProducerStatisticsMultipleProducersTest < WaterDropTest::Base
  def setup
    @message = build(:valid_message)
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

    sleep(0.001) while @events1.size < 2
    sleep(0.001) while @events2.size < 2
  end

  def teardown
    @producer1.close
    @producer2.close
    super
  end

  def test_different_statistics_from_different_producers
    ids1 = @events1.map(&:payload).map { |p| p[:statistics] }.map(&:object_id)
    ids2 = @events2.map(&:payload).map { |p| p[:statistics] }.map(&:object_id)

    assert_empty(ids1 & ids2)
  end
end

# Tests for error callback hook
class WaterDropProducerErrorCallbackTest < WaterDropTest::Base
  def setup
    @producer = build(:producer, kafka: { "bootstrap.servers": "localhost:9090" })
    @events = []

    @producer.monitor.subscribe("error.occurred") do |event|
      @events << event
    end

    @producer.client
    sleep(0.001) while @events.empty?
  end

  def teardown
    @producer.close
    super
  end

  def test_emits_error_occurred_event
    assert_equal "error.occurred", @events.first.id
  end

  def test_event_producer_id_matches
    assert_equal @producer.id, @events.first[:producer_id]
  end

  def test_event_error_is_rdkafka_error
    assert_kind_of Rdkafka::RdkafkaError, @events.first[:error]
  end
end

class WaterDropProducerErrorCallbackMultipleProducersTest < WaterDropTest::Base
  def setup
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

    @producer1.client
    @producer2.client

    sleep(0.001) while @events1.empty?
    sleep(0.001) while @events2.empty?
  end

  def teardown
    @producer1.close
    @producer2.close
    super
  end

  def test_different_errors_from_different_producers
    ids1 = @events1.map(&:payload).map { |p| p[:error] }.map(&:object_id)
    ids2 = @events2.map(&:payload).map { |p| p[:error] }.map(&:object_id)

    assert_empty(ids1 & ids2)
  end
end

# Tests for producer with enable.idempotence true
class WaterDropProducerIdempotentProducerTest < WaterDropTest::Base
  def setup
    @producer = build(:idempotent_producer)
    @message = build(:valid_message)
  end

  def teardown
    @producer.close
    super
  end

  def test_works_without_exceptions
    @producer.produce_sync(@message)
  end
end

# Tests for fork integration
class WaterDropProducerForkTest < WaterDropTest::Base
  def setup
    @producer = build(:producer)
    @topic_name = "it-#{SecureRandom.uuid}"
  end

  def teardown
    @producer.close
    super
  end

  def test_works_correctly_in_child_process
    child_process = fork do
      @producer.produce_sync(topic: @topic_name, payload: "1")
      @producer.close
      # Use exit! to skip at_exit handlers (Minitest autorun would re-run tests in child)
      exit!(0)
    end

    Process.wait(child_process)

    assert_equal 0, $CHILD_STATUS.to_i
  end
end

# Tests for disconnect integration
class WaterDropProducerDisconnectIntegrationTest < WaterDropTest::Base
  def setup
    @never_used = build(:producer, idle_disconnect_timeout: 30_000)
    @used_short = build(:producer, idle_disconnect_timeout: 30_000)
    @used = build(:producer, idle_disconnect_timeout: 30_000)
    @buffered = build(:producer, idle_disconnect_timeout: 30_000)
    @message = build(:valid_message)
  end

  def teardown
    @never_used.close
    @used_short.close
    @used.close
    @buffered.close
    super
  end

  def test_correctly_shutdown_those_in_need
    # @never_used is already created in setup, just needs to exist
    @buffered.buffer(@message)
    @used_short.produce_sync(@message)

    31.times do
      @used.produce_sync(@message)
      sleep(1)
    end

    sleep(1)

    assert_same false, @never_used.status.disconnected?
    assert_same false, @used.status.disconnected?
    assert_same false, @buffered.status.disconnected?
    assert_same true, @used_short.status.disconnected?
  end
end

# Tests for WaterDrop::Producer#disconnectable?
class WaterDropProducerDisconnectableNotConfiguredTest < WaterDropTest::Base
  def setup
    @producer = WaterDrop::Producer.new
  end

  def teardown
    @producer.close
    super
  end

  def test_not_disconnectable
    assert_same false, @producer.disconnectable?
  end
end

class WaterDropProducerDisconnectableConfiguredNotConnectedTest < WaterDropTest::Base
  def setup
    @producer = WaterDrop::Producer.new
    @producer.setup do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  def teardown
    @producer.close
    super
  end

  def test_not_disconnectable
    assert_same false, @producer.disconnectable?
  end
end

class WaterDropProducerDisconnectableConnectedTest < WaterDropTest::Base
  def setup
    @producer = build(:producer)
    @producer.client
  end

  def teardown
    @producer.close
    super
  end

  def test_is_disconnectable
    assert_same true, @producer.disconnectable?
  end

  def test_not_disconnectable_with_buffered_messages
    @producer.buffer(build(:valid_message))

    assert_same false, @producer.disconnectable?
  end

  def test_not_disconnectable_when_closed
    @producer.close

    assert_same false, @producer.disconnectable?
  end

  def test_not_disconnectable_when_already_disconnected
    @producer.disconnect

    assert_same false, @producer.disconnectable?
  end
end

class WaterDropProducerDisconnectableInTransactionTest < WaterDropTest::Base
  def setup
    @producer = build(:transactional_producer)
  end

  def teardown
    @producer.close
    super
  end

  def test_not_disconnectable_during_transaction
    @producer.transaction do
      assert_same false, @producer.disconnectable?
    end
  end
end
