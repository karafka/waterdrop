# frozen_string_literal: true

# External components
# delegate should be removed because we don't need it, we just add it because of ruby-kafka
%w[
  delegate
  forwardable
  json
  zeitwerk
  securerandom
  karafka-core
  pathname
].each { |lib| require lib }

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
