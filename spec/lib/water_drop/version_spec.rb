# frozen_string_literal: true

RSpec.describe WaterDrop do
  it { expect { WaterDrop::VERSION }.not_to raise_error }
end
