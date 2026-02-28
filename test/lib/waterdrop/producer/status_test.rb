# frozen_string_literal: true

class ProducerStatusTest < WaterDropTest::Base
  def setup
    @status = WaterDrop::Producer::Status.new
  end

  def test_default_state_is_initial
    assert_same true, @status.initial?
  end
end

class ProducerStatusInitialTest < WaterDropTest::Base
  def setup
    @status = WaterDrop::Producer::Status.new
    @status.initial!
  end

  def test_initial
    assert_same true, @status.initial?
  end

  def test_not_configured
    assert_same false, @status.configured?
  end

  def test_not_active
    assert_same false, @status.active?
  end

  def test_not_connected
    assert_same false, @status.connected?
  end

  def test_not_disconnecting
    assert_same false, @status.disconnecting?
  end

  def test_not_disconnected
    assert_same false, @status.disconnected?
  end

  def test_not_closing
    assert_same false, @status.closing?
  end

  def test_not_closed
    assert_same false, @status.closed?
  end

  def test_to_s
    assert_equal "initial", @status.to_s
  end
end

class ProducerStatusConfiguredTest < WaterDropTest::Base
  def setup
    @status = WaterDrop::Producer::Status.new
    @status.configured!
  end

  def test_not_initial
    assert_same false, @status.initial?
  end

  def test_configured
    assert_same true, @status.configured?
  end

  def test_active
    assert_same true, @status.active?
  end

  def test_not_connected
    assert_same false, @status.connected?
  end

  def test_not_disconnecting
    assert_same false, @status.disconnecting?
  end

  def test_not_disconnected
    assert_same false, @status.disconnected?
  end

  def test_not_closing
    assert_same false, @status.closing?
  end

  def test_not_closed
    assert_same false, @status.closed?
  end

  def test_to_s
    assert_equal "configured", @status.to_s
  end
end

class ProducerStatusConnectedTest < WaterDropTest::Base
  def setup
    @status = WaterDrop::Producer::Status.new
    @status.connected!
  end

  def test_not_initial
    assert_same false, @status.initial?
  end

  def test_not_configured
    assert_same false, @status.configured?
  end

  def test_active
    assert_same true, @status.active?
  end

  def test_connected
    assert_same true, @status.connected?
  end

  def test_not_disconnecting
    assert_same false, @status.disconnecting?
  end

  def test_not_disconnected
    assert_same false, @status.disconnected?
  end

  def test_not_closing
    assert_same false, @status.closing?
  end

  def test_not_closed
    assert_same false, @status.closed?
  end

  def test_to_s
    assert_equal "connected", @status.to_s
  end
end

class ProducerStatusDisconnectingTest < WaterDropTest::Base
  def setup
    @status = WaterDrop::Producer::Status.new
    @status.disconnecting!
  end

  def test_not_initial
    assert_same false, @status.initial?
  end

  def test_not_configured
    assert_same false, @status.configured?
  end

  def test_active
    assert_same true, @status.active?
  end

  def test_not_connected
    assert_same false, @status.connected?
  end

  def test_disconnecting
    assert_same true, @status.disconnecting?
  end

  def test_not_disconnected
    assert_same false, @status.disconnected?
  end

  def test_not_closing
    assert_same false, @status.closing?
  end

  def test_not_closed
    assert_same false, @status.closed?
  end

  def test_to_s
    assert_equal "disconnecting", @status.to_s
  end
end

class ProducerStatusDisconnectedTest < WaterDropTest::Base
  def setup
    @status = WaterDrop::Producer::Status.new
    @status.disconnected!
  end

  def test_not_initial
    assert_same false, @status.initial?
  end

  def test_not_configured
    assert_same false, @status.configured?
  end

  def test_active
    assert_same true, @status.active?
  end

  def test_not_connected
    assert_same false, @status.connected?
  end

  def test_not_disconnecting
    assert_same false, @status.disconnecting?
  end

  def test_disconnected
    assert_same true, @status.disconnected?
  end

  def test_not_closing
    assert_same false, @status.closing?
  end

  def test_not_closed
    assert_same false, @status.closed?
  end

  def test_to_s
    assert_equal "disconnected", @status.to_s
  end
end

class ProducerStatusClosingTest < WaterDropTest::Base
  def setup
    @status = WaterDrop::Producer::Status.new
    @status.closing!
  end

  def test_not_initial
    assert_same false, @status.initial?
  end

  def test_not_configured
    assert_same false, @status.configured?
  end

  def test_not_active
    assert_same false, @status.active?
  end

  def test_not_connected
    assert_same false, @status.connected?
  end

  def test_not_disconnecting
    assert_same false, @status.disconnecting?
  end

  def test_not_disconnected
    assert_same false, @status.disconnected?
  end

  def test_closing
    assert_same true, @status.closing?
  end

  def test_not_closed
    assert_same false, @status.closed?
  end

  def test_to_s
    assert_equal "closing", @status.to_s
  end
end

class ProducerStatusClosedTest < WaterDropTest::Base
  def setup
    @status = WaterDrop::Producer::Status.new
    @status.closed!
  end

  def test_not_initial
    assert_same false, @status.initial?
  end

  def test_not_configured
    assert_same false, @status.configured?
  end

  def test_not_active
    assert_same false, @status.active?
  end

  def test_not_connected
    assert_same false, @status.connected?
  end

  def test_not_disconnecting
    assert_same false, @status.disconnecting?
  end

  def test_not_disconnected
    assert_same false, @status.disconnected?
  end

  def test_not_closing
    assert_same false, @status.closing?
  end

  def test_closed
    assert_same true, @status.closed?
  end

  def test_to_s
    assert_equal "closed", @status.to_s
  end
end
