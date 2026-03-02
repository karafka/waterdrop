# frozen_string_literal: true

describe_current do
  it { assert_operator(described_class, :<, Karafka::Core::Monitoring::Monitor) }
end
