# frozen_string_literal: true

RSpec.describe WaterDrop::Instrumentation::StdoutListener do
  subject(:listener) { described_class.new(Logger.new) }

  pending
end
