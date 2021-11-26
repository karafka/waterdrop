# frozen_string_literal: true

# External components
# delegate should be removed because we don't need it, we just add it because of ruby-kafka
%w[
  concurrent/array
  dry-configurable
  dry/monitor/notifications
  dry-validation
  rdkafka
  json
  zeitwerk
  securerandom
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

Zeitwerk::Loader
  .for_gem
  .tap { |loader| loader.ignore("#{__dir__}/waterdrop.rb") }
  .tap(&:setup)
  .tap(&:eager_load)

# Rdkafka uses a single global callback for things. We bypass that by injecting a manager for
# each callback type. Callback manager allows us to register more than one callback
# @note Those managers are also used by Karafka for consumer related statistics
Rdkafka::Config.statistics_callback = WaterDrop::Instrumentation.statistics_callbacks
Rdkafka::Config.error_callback = WaterDrop::Instrumentation.error_callbacks
