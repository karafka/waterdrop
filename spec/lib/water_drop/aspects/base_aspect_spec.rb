require 'spec_helper'

RSpec.describe WaterDrop::Aspects::BaseAspect do
  subject { described_class.new(nil, {}) }
  specify { expect(described_class).to be < Aspector::Base }
end
