# frozen_string_literal: true

require "digest"
require "securerandom"

PROJECT_ROOT = File.expand_path(File.join(__dir__, "..", ".."))

# Computes a short hash of a file path relative to the project root.
# This hash is embedded in auto-generated topic names so that Kafka warnings
# (e.g., TOPIC_ALREADY_EXISTS) can be traced back to the originating test file.
#
# The hash is based on the path relative to the project root, so it remains
# consistent across environments (local dev vs CI runners with different absolute paths).
#
# For integration tests run via bin/integrations, the runner sets SPEC_FILE_PATH with
# the relative path (since specs are copied to a temp directory).
# For unit tests run via Minitest::TestTask ($PROGRAM_NAME = "-e"), caller_locations
# is used to find the actual _test.rb file.
SPEC_HASH_CACHE = {}

# Generates a unique topic name with an 8-char hash of the originating test file
# for traceability.
#
# @param label [String, nil] optional descriptive label (e.g., "fd-stats", "tx-concurrent")
# @return [String] topic name like "it-a1b2c3d4-abcdef012345" or "it-a1b2c3d4-fd-stats-abcd0123"
def generate_topic(label = nil)
  spec_hash = if ENV.key?("SPEC_FILE_PATH")
    SPEC_HASH_CACHE["SPEC_FILE_PATH"] ||= Digest::MD5.hexdigest(ENV["SPEC_FILE_PATH"])[0, 8]
  else
    test_file = caller_locations.find { |loc| loc.path.end_with?("_test.rb", "_spec.rb") }&.path
    relative = test_file ? test_file.sub("#{PROJECT_ROOT}/", "") : $PROGRAM_NAME

    SPEC_HASH_CACHE[relative] ||= Digest::MD5.hexdigest(relative)[0, 8]
  end

  if label
    "it-#{spec_hash}-#{label}-#{SecureRandom.hex(4)}"
  else
    "it-#{spec_hash}-#{SecureRandom.hex(6)}"
  end
end

# Pre-creates a Kafka topic via the admin API to avoid TOPIC_ALREADY_EXISTS race conditions
# that occur when concurrent produce requests trigger auto-topic creation simultaneously.
#
# @param topic_name [String] the topic name to create
# @param partitions [Integer] number of partitions (default: 1)
# @param replication_factor [Integer] replication factor (default: 1)
# @param topic_config [Hash] optional topic-level configuration (e.g., "max.message.bytes": 128)
def create_topic(topic_name, partitions: 1, replication_factor: 1, **topic_config)
  admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP_SERVERS).admin
  admin.create_topic(topic_name, partitions, replication_factor, topic_config).wait
  admin.close
end
