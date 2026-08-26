# frozen_string_literal: true

# Integration test for https://github.com/karafka/waterdrop/issues/941
#
# When a producer is configured with a frozen string id (the common case: a string literal in a file
# with `# frozen_string_literal: true`, e.g. `config.id = "rspec"`), #close raises
# `FrozenError: can't modify frozen String`.

require "waterdrop"

FROZEN_ID = "frozen-producer-id-941"
raise "test setup broken: id must be frozen to reproduce the bug" unless FROZEN_ID.frozen?

producer = WaterDrop::Producer.new do |config|
  config.deliver = false
  config.id = FROZEN_ID
  config.kafka = { "bootstrap.servers": "localhost:9092" }
end

producer.close
