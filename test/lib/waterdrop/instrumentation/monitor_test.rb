# frozen_string_literal: true

class WaterDropInstrumentationMonitorTest < WaterDropTest::Base
  def test_inherits_from_karafka_core_monitoring_monitor
    assert_operator WaterDrop::Instrumentation::Monitor, :<,
      Karafka::Core::Monitoring::Monitor
  end
end
