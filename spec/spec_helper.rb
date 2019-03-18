# frozen_string_literal: true

$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))

%w[
  rubygems
  simplecov
].each(&method(:require))

# Don't include unnecessary stuff into coverage
SimpleCov.start do
  %w[
    .bundle
    config
    doc
    gems
    spec
    vendor
  ].each { |dir| add_filter "/#{dir}/" }

  merge_timeout 600
end

RSpec.configure do |config|
  config.disable_monkey_patching!
  config.order = :random

  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end
end

require 'water_drop'

# Configure for test setup
WaterDrop.setup do |config|
  config.deliver = true
  config.kafka.seed_brokers = %w[kafka://localhost:9092]
  config.logger = Logger.new(File.join(WaterDrop.gem_root, 'log', 'test.log'))
end

WaterDrop.monitor.subscribe(WaterDrop::Instrumentation::StdoutListener)
