# frozen_string_literal: true

require 'factory_bot'

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

RSpec.configure do |config|
  config.include FactoryBot::Syntax::Methods
  config.disable_monkey_patching!
  config.order = :random

  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end
end

require 'karafka/core/helpers/rspec_locator'
RSpec.extend Karafka::Core::Helpers::RSpecLocator.new(__FILE__, 'Waterdrop' => 'WaterDrop')

require 'waterdrop'
require 'waterdrop/instrumentation/vendors/datadog/listener'
