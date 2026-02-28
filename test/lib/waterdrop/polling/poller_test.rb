# frozen_string_literal: true

class WaterDropPollingPollerTest < WaterDropTest::Base
  # A stub client that responds to all methods the poller thread needs
  class StubClient
    def enable_queue_io_events(_fd)
    end

    def queue_size
      0
    end

    def events_poll_nb_each
    end
  end

  # A stub monitor that records instrument calls
  class StubMonitor
    attr_reader :events

    def initialize
      @events = []
    end

    def instrument(event, **kwargs)
      @events << { event: event, **kwargs }
    end
  end

  def setup
    @poller = WaterDrop::Polling::Poller.instance
    @producer_id = "test-producer-#{SecureRandom.hex(6)}"

    @producer_config = WaterDrop::Config.new.tap do |cfg|
      cfg.configure do |c|
        c.kafka = { "bootstrap.servers": "localhost:9092" }
        c.polling.mode = :fd
        c.polling.fd.max_time = 100
        c.polling.fd.periodic_poll_interval = 1000
      end
    end

    @monitor = StubMonitor.new

    @producer = OpenStruct.new(
      id: @producer_id,
      config: @producer_config.config,
      monitor: @monitor
    )

    @client = StubClient.new

    @poller.shutdown!
  end

  def teardown
    @poller.shutdown!
    super
  end

  def test_register_adds_producer_to_registry
    @poller.register(@producer, @client)

    assert_equal 1, @poller.count
  end

  def test_register_starts_polling_thread
    @poller.register(@producer, @client)

    assert_same true, @poller.alive?
  end

  def test_register_instruments_producer_registration
    @poller.register(@producer, @client)

    event = @monitor.events.find { |e| e[:event] == "poller.producer_registered" }

    refute_nil event
    assert_equal @producer_id, event[:producer_id]
  end

  def test_unregister_instruments_producer_unregistration
    @poller.register(@producer, @client)
    @poller.unregister(@producer)

    event = @monitor.events.find { |e| e[:event] == "poller.producer_unregistered" }

    refute_nil event
    assert_equal @producer_id, event[:producer_id]
  end

  def test_unregister_stops_thread_when_last_producer_removed
    @poller.register(@producer, @client)

    assert_same true, @poller.alive?

    @poller.unregister(@producer)

    assert_same false, @poller.alive?
  end

  def test_alive_returns_false_with_no_producers
    assert_same false, @poller.alive?
  end

  def test_count_returns_zero_with_no_producers
    assert_equal 0, @poller.count
  end

  def test_count_returns_correct_number_with_multiple_producers
    monitor2 = StubMonitor.new
    producer2 = OpenStruct.new(
      id: "test-producer-2",
      config: @producer_config.config,
      monitor: monitor2
    )

    @poller.register(@producer, @client)
    @poller.register(producer2, StubClient.new)

    assert_equal 2, @poller.count
  end

  def test_shutdown_stops_polling_thread
    @poller.register(@producer, @client)

    assert_same true, @poller.alive?
    @poller.shutdown!

    assert_same false, @poller.alive?
  end

  def test_shutdown_clears_all_producers
    @poller.register(@producer, @client)

    assert_equal 1, @poller.count
    @poller.shutdown!

    assert_equal 0, @poller.count
  end

  def test_shutdown_can_be_called_multiple_times
    @poller.register(@producer, @client)
    3.times { @poller.shutdown! }
  end

  def test_thread_priority_from_config
    original_priority = WaterDrop::Polling::Config.config.thread_priority

    WaterDrop::Polling::Config.setup do |config|
      config.thread_priority = -1
    end

    @poller.shutdown!
    @poller.register(@producer, @client)

    thread = @poller.instance_variable_get(:@thread)

    assert_equal(-1, thread.priority)
  ensure
    WaterDrop::Polling::Config.setup do |config|
      config.thread_priority = original_priority
    end
  end

  def test_in_poller_thread_returns_false_from_main_thread
    assert_same false, @poller.in_poller_thread?
  end

  def test_id_returns_integer
    assert_kind_of Integer, @poller.id
  end

  def test_new_creates_separate_instance
    custom_poller = WaterDrop::Polling::Poller.new

    refute_equal custom_poller, @poller
  end

  def test_new_assigns_incremental_ids
    poller1 = WaterDrop::Polling::Poller.new
    poller2 = WaterDrop::Polling::Poller.new

    assert_operator poller2.id, :>, poller1.id
  end

  def test_new_includes_id_in_thread_name
    custom_poller = WaterDrop::Polling::Poller.new
    monitor = StubMonitor.new

    producer = OpenStruct.new(
      id: @producer_id,
      config: @producer_config.config,
      monitor: monitor
    )

    custom_poller.register(producer, StubClient.new)

    thread = custom_poller.instance_variable_get(:@thread)

    assert_equal "waterdrop.poller##{custom_poller.id}", thread.name

    custom_poller.shutdown!
  end
end
