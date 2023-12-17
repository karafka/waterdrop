# frozen_string_literal: true

RSpec.describe_current do
  subject(:waterdrop) { described_class }

  describe '#gem_root' do
    context 'when we want to get gem root path' do
      let(:path) { Dir.pwd }

      it { expect(waterdrop.gem_root.to_path).to eq path }
    end
  end
end
