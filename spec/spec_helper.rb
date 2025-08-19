# frozen_string_literal: true

Warning[:performance] = true if RUBY_VERSION >= '3.3'
Warning[:deprecated] = true
$VERBOSE = true

require 'warning'

Warning.process do |warning|
  next unless warning.include?(Dir.pwd)
  next if warning.include?('vendor/bundle')
  # Allow OpenStruct usage only in specs
  next if warning.include?('OpenStruct use') && warning.include?('_spec')
  next if warning.include?('$CHILD_STATUS')

  raise "Warning in your code: #{warning}"
end

require 'ostruct'
require 'securerandom'
require 'logger'

coverage = !ENV.key?('GITHUB_WORKFLOW')
coverage = true if ENV['GITHUB_COVERAGE'] == 'true'

if ENV.key?('KAFKA_IP')
  KAFKA_IP = ENV.fetch('KAFKA_IP')
  KAFKA_PORT = ENV.fetch('KAFKA_PORT')
else
  ip = `docker inspect \
        -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
        $(docker compose ps -q kafka 2>/dev/null) \
        2>/dev/null`.strip
  KAFKA_IP = ip.empty? ? 'localhost' : ip
  KAFKA_PORT = ENV.fetch('KAFKA_PORT', '9092')
end

KAFKA_HOST = "#{KAFKA_IP}:#{KAFKA_PORT}"

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

require 'karafka/core/helpers/rspec_locator'
RSpec.extend Karafka::Core::Helpers::RSpecLocator.new(__FILE__, 'Waterdrop' => 'WaterDrop')

require 'waterdrop'
require 'waterdrop/instrumentation/vendors/datadog/metrics_listener'
