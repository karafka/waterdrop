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
  # Allow method redefinition in tests (e.g. define_singleton_method on OpenStruct mocks)
  next if warning.include?("method redefined") && warning.include?("_test")
  next if warning.include?("previous definition") && warning.include?("_test")

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
    command_name "Minitest-#{ENV["FD_POLLING"] == "true" ? "fiber" : "thread"}"

    add_filter "/test/"
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
    minimum_coverage 0
    enable_coverage :branch
  end
end

require "minitest/autorun"
require "minitest/spec"
require "minitest/mock"

# Add `context` as an alias for `describe` in minitest/spec
class Minitest::Spec
  class << self
    alias_method :context, :describe
  end

  # Provide RSpec-compatible described_class by walking up the describe hierarchy
  # to find the first Class or Module passed to describe/describe_current
  def described_class
    klass = self.class

    while klass.respond_to?(:desc)
      return klass.desc if klass.desc.is_a?(Module)

      klass = klass.superclass
    end

    nil
  end
end

require_relative "support/factories"
require_relative "support/factories/message"
require_relative "support/factories/producer"

class Minitest::Spec
  include Factories
  include Factories::Message
  include Factories::Producer

  # Track all producers created via build() so we can close them in teardown.
  # This prevents leaking producers when nested before blocks override @producer.
  # @param factory_name [Symbol, String] factory to build
  # @param attributes [Object] factory attribute overrides
  def build(factory_name, **attributes)
    instance = super

    if instance.is_a?(WaterDrop::Producer)
      @_created_producers ||= []
      @_created_producers << instance
    end

    instance
  end

  # Clean up the Poller singleton after each test to prevent mock leakage
  # Reset the Poller singleton between tests to prevent state leakage
  def teardown
    super

    # Close any producers created via build() that are still open
    @_created_producers&.each do |producer|
      producer.close unless producer.status.closed?
    end

    if ENV["FD_POLLING"] == "true"
      WaterDrop::Polling::Poller.instance.shutdown!
    end
  end
end

require "karafka/core/helpers/minitest_locator"
extend Karafka::Core::Helpers::MinitestLocator.new(__FILE__, "Waterdrop" => "WaterDrop")

require "waterdrop"
require "waterdrop/producer/testing"
require "waterdrop/instrumentation/vendors/datadog/metrics_listener"
