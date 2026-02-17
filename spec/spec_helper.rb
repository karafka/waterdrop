# frozen_string_literal: true

Warning[:performance] = true if RUBY_VERSION >= "3.3"
Warning[:deprecated] = true
$VERBOSE = true

require "warning"

Warning.process do |warning|
  next unless warning.include?(Dir.pwd)
  next if warning.include?("vendor/bundle")
  # Allow OpenStruct usage only in specs
  next if warning.include?("OpenStruct use") && warning.include?("_spec")
  next if warning.include?("$CHILD_STATUS")

  raise "Warning in your code: #{warning}"
end

require "ostruct"
require "securerandom"
require "logger"

coverage = !ENV.key?("GITHUB_WORKFLOW")
coverage = true if ENV["GITHUB_COVERAGE"] == "true"

# Used to point out to Kafka location
# Useful when we run specs on non-local clusters for extended testing
BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

if coverage
  require "simplecov"

  # Don't include unnecessary stuff into rcov
  SimpleCov.start do
    add_filter "/spec/"
    add_filter "/vendor/"
    add_filter "/gems/"
    add_filter "/.bundle/"
    add_filter "/doc/"
    add_filter "/config/"
    add_filter "/lib/waterdrop/patches/"

    merge_timeout 600
    # Coverage is below 100% because thread mode and FD polling mode
    # exercise different code paths, and each CI run only tests one mode
    minimum_coverage 95
    enable_coverage :branch
  end
end

Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].each { |f| require f }

require_relative "support/factories"
require_relative "support/factories/message"
require_relative "support/factories/producer"

RSpec.configure do |config|
  config.disable_monkey_patching!
  config.order = :random

  config.include Factories
  config.include Factories::Message
  config.include Factories::Producer

  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  # Clean up the Poller singleton after each test to prevent mock leakage
  # This is needed because the Poller is a singleton that may hold references
  # to test doubles from previous tests
  config.after do
    next unless ENV["FD_POLLING"]

    poller = WaterDrop::Polling::Poller.instance

    # Signal shutdown and wait for thread to stop
    poller.instance_variable_set(:@shutdown, true)
    thread = poller.instance_variable_get(:@thread)
    if thread&.alive?
      begin
        poller.instance_variable_get(:@wakeup_write)&.write_nonblock("W")
      rescue IOError, Errno::EPIPE, Errno::EAGAIN
        nil
      end
      thread.join(0.5)
      thread.kill if thread.alive?
    end

    # Clear all state
    poller.instance_variable_set(:@thread, nil)
    poller.instance_variable_set(:@producers, {})
    poller.instance_variable_set(:@shutdown, false)
    poller.instance_variable_set(:@ios_dirty, true)
    poller.instance_variable_set(:@cached_ios, [])
    poller.instance_variable_set(:@cached_io_to_state, {})
  end
end

require "karafka/core/helpers/rspec_locator"
RSpec.extend Karafka::Core::Helpers::RSpecLocator.new(__FILE__, "Waterdrop" => "WaterDrop")

require "waterdrop"
require "waterdrop/producer/testing"
require "waterdrop/instrumentation/vendors/datadog/metrics_listener"
