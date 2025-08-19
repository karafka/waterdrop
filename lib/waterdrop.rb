# frozen_string_literal: true

# External components
require 'forwardable'
require 'json'
require 'zeitwerk'
require 'securerandom'
require 'karafka-core'
require 'pathname'

# WaterDrop library
module WaterDrop
  class << self
    # @return [String] root path of this gem
    def gem_root
      Pathname.new(File.expand_path('..', __dir__))
    end
  end
end

loader = Zeitwerk::Loader.for_gem
loader.inflector.inflect('waterdrop' => 'WaterDrop')
# Do not load vendors instrumentation components. Those need to be required manually if needed
loader.ignore("#{__dir__}/waterdrop/instrumentation/vendors/**/*.rb")
loader.setup
loader.eager_load
