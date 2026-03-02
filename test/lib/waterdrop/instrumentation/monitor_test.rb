# frozen_string_literal: true

require "test_helper"

describe_current do
  it { assert_operator(described_class, :<, Karafka::Core::Monitoring::Monitor) }
end
