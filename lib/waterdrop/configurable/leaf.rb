# frozen_string_literal: true

module WaterDrop
  module Configurable
    # Single end config value representation
    class Leaf < Struct.new(:name, :default, :constructor)
    end
  end
end
