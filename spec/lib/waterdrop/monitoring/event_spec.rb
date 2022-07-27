# frozen_string_literal: true

RSpec.describe_current do
  subject(:event) { described_class.new(id, payload) }

  let(:id) { rand.to_s }
  let(:payload) { { rand => rand } }

  it { expect(event.id).to eq(id) }
  it { expect(event.payload).to eq(payload) }

  describe '#[]' do
    context 'when key is present' do
      let(:payload) { { test: 1 } }

      it 'expect to return it' do
        expect(event[:test]).to eq(1)
      end
    end

    context 'when key is missing' do
      it { expect { event[:test] }.to raise_error(KeyError) }
    end
  end
end
