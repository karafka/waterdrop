# frozen_string_literal: true

RSpec.describe WaterDrop::Config do
  subject(:config) { described_class.config }

  pending

  describe '.setup' do
    it { expect { |block| described_class.setup(&block) }.to yield_with_args }
  end
end
