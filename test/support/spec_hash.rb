# frozen_string_literal: true

require "digest"

# Computes a short hash of the current test file's relative path.
# This hash is embedded in auto-generated topic names so that Kafka warnings
# (e.g., TOPIC_ALREADY_EXISTS) can be traced back to the originating test file.
#
# The hash is based on the path relative to the project root, so it remains
# consistent across environments (local dev vs CI runners with different absolute paths).
#
# For integration tests run via bin/integrations, the runner sets SPEC_FILE_PATH with
# the relative path (since specs are copied to a temp directory).
# For unit tests, the path is derived from $PROGRAM_NAME.
SPEC_HASH = begin
  relative_path = ENV.fetch("SPEC_FILE_PATH") do
    project_root = File.expand_path(File.join(__dir__, "..", ".."))
    absolute_program = File.expand_path($PROGRAM_NAME)
    absolute_program.sub("#{project_root}/", "")
  end

  Digest::MD5.hexdigest(relative_path)[0, 8]
end

# Generates a unique topic name that includes SPEC_HASH for traceability.
#
# @param label [String, nil] optional descriptive label (e.g., "fd-stats", "tx-concurrent")
# @return [String] topic name like "it-a1b2c3d4-abcdef012345" or "it-a1b2c3d4-fd-stats-abcd0123"
def generate_topic(label = nil)
  if label
    "it-#{SPEC_HASH}-#{label}-#{SecureRandom.hex(4)}"
  else
    "it-#{SPEC_HASH}-#{SecureRandom.hex(6)}"
  end
end
