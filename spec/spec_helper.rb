# frozen_string_literal: true

coverage = !ENV.key?('GITHUB_WORKFLOW')
coverage = true if ENV['GITHUB_COVERAGE'] == 'true'

if coverage
  require 'simplecov'

  # Don't include unnecessary stuff into rcov
  SimpleCov.start do
    add_filter '/spec/'
    add_filter '/vendor/'
    add_filter '/gems/'
    add_filter '/.bundle/'
    add_filter '/doc/'
    add_filter '/config/'

    merge_timeout 600
    minimum_coverage 100
    enable_coverage :branch
  end
end

RSpec.configure do |config|
  config.disable_monkey_patching!
  config.order = :random

  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.before(:all) { DeliveryBoy.test_mode! }
  config.before { DeliveryBoy.testing.clear }
end

require 'water_drop'

# Configure for test setup
WaterDrop.setup do |config|
  config.deliver = true
  config.kafka.seed_brokers = %w[kafka://localhost:9092]
  config.logger = Logger.new(File.join(WaterDrop.gem_root, 'log', 'test.log'))
end

WaterDrop.monitor.subscribe(WaterDrop::Instrumentation::StdoutListener.new)
