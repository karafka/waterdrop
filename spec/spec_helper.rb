# frozen_string_literal: true

$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))

%w[
  byebug
  factory_bot
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

Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].sort.each { |f| require f }

SimpleCov.minimum_coverage 100

RSpec.configure do |config|
  config.include FactoryBot::Syntax::Methods
  config.disable_monkey_patching!
  config.order = :random

  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end
end

require 'water_drop'
