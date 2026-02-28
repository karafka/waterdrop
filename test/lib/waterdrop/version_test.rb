# frozen_string_literal: true

class WaterDropVersionTest < WaterDropTest::Base
  def test_version_constant_is_accessible
    WaterDrop::VERSION
  end
end
