# frozen_string_literal: true

module WaterDrop
  module Contractable
    # Representation of a single validation rule
    Rule = Struct.new(:path, :type, :validator)
  end
end
