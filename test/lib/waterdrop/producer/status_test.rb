# frozen_string_literal: true

class ProducerStatusTest < WaterDropTest::Base
  def setup
    @status = WaterDrop::Producer::Status.new
  end

  def test_default_state_is_initial
    assert @status.initial?
  end
end

class ProducerStatusInitialTest < WaterDropTest::Base
  def setup
    @status = WaterDrop::Producer::Status.new
    @status.initial!
  end

  def test_initial
    assert @status.initial?
  end

  def test_not_configured
    refute @status.configured?
  end

  def test_not_active
    refute @status.active?
  end

  def test_not_connected
    refute @status.connected?
  end

  def test_not_disconnecting
    refute @status.disconnecting?
  end

  def test_not_disconnected
    refute @status.disconnected?
  end

  def test_not_closing
    refute @status.closing?
  end

  def test_not_closed
    refute @status.closed?
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
    refute @status.initial?
  end

  def test_configured
    assert @status.configured?
  end

  def test_active
    assert @status.active?
  end

  def test_not_connected
    refute @status.connected?
  end

  def test_not_disconnecting
    refute @status.disconnecting?
  end

  def test_not_disconnected
    refute @status.disconnected?
  end

  def test_not_closing
    refute @status.closing?
  end

  def test_not_closed
    refute @status.closed?
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
    refute @status.initial?
  end

  def test_not_configured
    refute @status.configured?
  end

  def test_active
    assert @status.active?
  end

  def test_connected
    assert @status.connected?
  end

  def test_not_disconnecting
    refute @status.disconnecting?
  end

  def test_not_disconnected
    refute @status.disconnected?
  end

  def test_not_closing
    refute @status.closing?
  end

  def test_not_closed
    refute @status.closed?
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
    refute @status.initial?
  end

  def test_not_configured
    refute @status.configured?
  end

  def test_active
    assert @status.active?
  end

  def test_not_connected
    refute @status.connected?
  end

  def test_disconnecting
    assert @status.disconnecting?
  end

  def test_not_disconnected
    refute @status.disconnected?
  end

  def test_not_closing
    refute @status.closing?
  end

  def test_not_closed
    refute @status.closed?
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
    refute @status.initial?
  end

  def test_not_configured
    refute @status.configured?
  end

  def test_active
    assert @status.active?
  end

  def test_not_connected
    refute @status.connected?
  end

  def test_not_disconnecting
    refute @status.disconnecting?
  end

  def test_disconnected
    assert @status.disconnected?
  end

  def test_not_closing
    refute @status.closing?
  end

  def test_not_closed
    refute @status.closed?
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
    refute @status.initial?
  end

  def test_not_configured
    refute @status.configured?
  end

  def test_not_active
    refute @status.active?
  end

  def test_not_connected
    refute @status.connected?
  end

  def test_not_disconnecting
    refute @status.disconnecting?
  end

  def test_not_disconnected
    refute @status.disconnected?
  end

  def test_closing
    assert @status.closing?
  end

  def test_not_closed
    refute @status.closed?
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
    refute @status.initial?
  end

  def test_not_configured
    refute @status.configured?
  end

  def test_not_active
    refute @status.active?
  end

  def test_not_connected
    refute @status.connected?
  end

  def test_not_disconnecting
    refute @status.disconnecting?
  end

  def test_not_disconnected
    refute @status.disconnected?
  end

  def test_not_closing
    refute @status.closing?
  end

  def test_closed
    assert @status.closed?
  end

  def test_to_s
    assert_equal "closed", @status.to_s
  end
end
