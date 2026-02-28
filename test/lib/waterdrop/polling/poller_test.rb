# frozen_string_literal: true

class WaterDropPollingPollerTest < WaterDropTest::Base
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

    @monitor = Minitest::Mock.new

    @producer = OpenStruct.new(
      id: @producer_id,
      config: @producer_config.config,
      monitor: @monitor
    )

    @client = Minitest::Mock.new
    @client.expect(:enable_queue_io_events, nil, [Integer])
    @client.expect(:queue_size, 0)

    @poller.shutdown!
  end

  def teardown
    @poller.shutdown!
    super
  end

  def test_register_adds_producer_to_registry
    @monitor.expect(:instrument, nil, ["poller.producer_registered"], producer_id: @producer_id)
    @poller.register(@producer, @client)

    assert_equal 1, @poller.count
  end

  def test_register_starts_polling_thread
    @monitor.expect(:instrument, nil, ["poller.producer_registered"], producer_id: @producer_id)
    @poller.register(@producer, @client)

    assert_same true, @poller.alive?
  end

  def test_alive_returns_false_with_no_producers
    assert_same false, @poller.alive?
  end

  def test_count_returns_zero_with_no_producers
    assert_equal 0, @poller.count
  end

  def test_shutdown_stops_polling_thread
    @monitor.expect(:instrument, nil, ["poller.producer_registered"], producer_id: @producer_id)
    @poller.register(@producer, @client)

    assert_same true, @poller.alive?
    @poller.shutdown!

    assert_same false, @poller.alive?
  end

  def test_shutdown_clears_all_producers
    @monitor.expect(:instrument, nil, ["poller.producer_registered"], producer_id: @producer_id)
    @poller.register(@producer, @client)

    assert_equal 1, @poller.count
    @poller.shutdown!

    assert_equal 0, @poller.count
  end

  def test_shutdown_can_be_called_multiple_times
    @monitor.expect(:instrument, nil, ["poller.producer_registered"], producer_id: @producer_id)
    @poller.register(@producer, @client)
    3.times { @poller.shutdown! }
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

    monitor = Minitest::Mock.new
    monitor.expect(:instrument, nil, ["poller.producer_registered"], producer_id: @producer_id)

    producer = OpenStruct.new(
      id: @producer_id,
      config: @producer_config.config,
      monitor: monitor
    )

    client = Minitest::Mock.new
    client.expect(:enable_queue_io_events, nil, [Integer])
    client.expect(:queue_size, 0)

    custom_poller.register(producer, client)

    thread = custom_poller.instance_variable_get(:@thread)

    assert_equal "waterdrop.poller##{custom_poller.id}", thread.name

    custom_poller.shutdown!
  end
end
