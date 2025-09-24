# frozen_string_literal: true

# External components
require 'delegate'
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

    # @return [WaterDrop::Instrumentation::ClassMonitor] global monitor for
    #   class-level event subscriptions. This allows external libraries to subscribe to WaterDrop
    #   lifecycle events without needing producer instance references.
    #
    # @note Only supports class-level events (producer.created, producer.configured), not
    #   instance events
    #
    # @example Subscribe to producer creation events
    #   WaterDrop.monitor.subscribe('producer.created') do |event|
    #     producer = event[:producer]
    #     # Configure producer or add middleware
    #   end
    def monitor
      @instrumentation ||= Instrumentation::ClassMonitor.new
    end

    # @deprecated Use #monitor instead. This method is provided for backward compatibility only.
    alias instrumentation monitor
  end
end

loader = Zeitwerk::Loader.for_gem
loader.inflector.inflect('waterdrop' => 'WaterDrop')
# Do not load vendors instrumentation components. Those need to be required manually if needed
loader.ignore("#{__dir__}/waterdrop/instrumentation/vendors/**/*.rb")
loader.setup
loader.eager_load
