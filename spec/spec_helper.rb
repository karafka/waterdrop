# frozen_string_literal: true

require 'ostruct'
require 'securerandom'
require 'logger'

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
    add_filter '/lib/waterdrop/patches/'

    merge_timeout 600
    minimum_coverage 100
    enable_coverage :branch
  end
end

Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].sort.each { |f| require f }

require_relative 'support/factories'
require_relative 'support/factories/message'
require_relative 'support/factories/producer'

RSpec.configure do |config|
  config.disable_monkey_patching!
  config.order = :random

  config.include Factories
  config.include Factories::Message
  config.include Factories::Producer

  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end
end

require 'support/rspec_locator'
RSpec.extend RSpecLocator.new(__FILE__, 'Waterdrop' => 'WaterDrop')

require 'waterdrop'
require 'waterdrop/instrumentation/vendors/datadog/metrics_listener'
