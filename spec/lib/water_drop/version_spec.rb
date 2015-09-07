require 'spec_helper'

RSpec.describe WaterDrop do
  it { expect { WaterDrop::VERSION }.not_to raise_error }
end
