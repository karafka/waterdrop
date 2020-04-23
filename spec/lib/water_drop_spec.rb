# frozen_string_literal: true

RSpec.describe WaterDrop do
  subject(:water_drop) { described_class }

  describe '#gem_root' do
    context 'when we want to get gem root path' do
      let(:path) { Dir.pwd }

      it { expect(water_drop.gem_root.to_path).to eq path }
    end
  end
end
