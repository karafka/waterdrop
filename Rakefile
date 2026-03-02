# frozen_string_literal: true

require "bundler/setup"
require "bundler/gem_tasks"
require "minitest/test_task"

Minitest::TestTask.create(:test) do |t|
  t.libs << "test"
  t.libs << "lib"
  t.test_globs = ["test/**/*_test.rb"]
  # Load test_helper before minitest/autorun so SimpleCov's at_exit hook is
  # registered first and thus runs last (LIFO), collecting coverage AFTER tests
  t.test_prelude = 'require "test_helper"; require "minitest/autorun"'
end

task default: :test
