# frozen_string_literal: true

Warning[:performance] = true if RUBY_VERSION >= "3.3"
Warning[:deprecated] = true
$VERBOSE = true

require "warning"

Warning.process do |warning|
  next unless warning.include?(Dir.pwd)
  next if warning.include?("vendor/bundle")
  # Allow OpenStruct usage only in tests
  next if warning.include?("OpenStruct use") && warning.include?("_test")
  next if warning.include?("$CHILD_STATUS")

  raise "Warning in your code: #{warning}"
end

require "ostruct"
require "securerandom"
require "logger"

coverage = !ENV.key?("GITHUB_WORKFLOW")
coverage = true if ENV["GITHUB_COVERAGE"] == "true"

# Used to point out to Kafka location
# Useful when we run tests on non-local clusters for extended testing
BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

if coverage
  require "simplecov"

  # Don't include unnecessary stuff into rcov
  SimpleCov.start do
    add_filter "/test/"
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

require "minitest/autorun"
require "minitest/mock"

require "waterdrop"
require "waterdrop/producer/testing"
require "waterdrop/instrumentation/vendors/datadog/metrics_listener"

require_relative "support/factories"
require_relative "support/factories/message"
require_relative "support/factories/producer"

module WaterDropTest
  # Base test class for all WaterDrop tests
  class Base < Minitest::Test
    include Factories
    include Factories::Message
    include Factories::Producer

    def teardown
      return unless ENV["FD_POLLING"] == "true"

      WaterDrop::Polling::Poller.instance.shutdown!
    end
  end
end
