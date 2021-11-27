# frozen_string_literal: true

RSpec.describe_current do
  subject(:manager) { described_class.new }

  let(:id) { SecureRandom.uuid }

  describe '#call' do
    context 'when there are no callbacks added' do
      it { expect { manager.call }.not_to raise_error }
    end

    context 'when there are callbacks added' do
      let(:changed) { [] }
      let(:start) { [rand, rand, rand] }

      before do
        manager.add('1', ->(val1, _, _) { changed << val1 + 1 })
        manager.add('2', ->(_, val2, _) { changed << val2 + 2 })
        manager.add('3', ->(_, _, val3) { changed << val3 + 3 })
      end

      it 'expect to run each of them and pass the args' do
        manager.call(*start)
        expect(changed).to eq([start[0] + 1, start[1] + 2, start[2] + 3])
      end
    end
  end

  describe '#add' do
    let(:changed) { [] }

    it 'expect after adding to be used' do
      manager.add(id, -> { changed << true })
      manager.call
      expect(changed).to eq([true])
    end
  end

  describe '#delete' do
    let(:changed) { [] }

    before { manager.add(id, -> { changed << true }) }

    it 'expect after removal not to be used' do
      manager.delete(id)
      manager.call
      expect(changed).to be_empty
    end
  end
end
