# frozen_string_literal: true

RSpec.describe_current do
  subject(:middleware) { described_class.new }

  let(:message) { build(:valid_message) }

  context 'when no middlewares' do
    it { expect { middleware.run(message) }.not_to change { message } }
  end

  context 'when morphing middleware' do
    before do
      middleware.prepend lambda do |msg|
        msg[:test] = 1
        msg
      end
    end

    it { expect { middleware.run(message) }.to(change { message[:test] }.from(nil).to(1)) }
  end

  context 'when morphing middlewares' do
    before do
      middleware.prepend lambda do |msg|
        msg[:test] = 1
        msg
      end

      middleware.prepend lambda do |msg|
        msg[:test2] = 2
        msg
      end
    end

    it { expect { middleware.run(message) }.to(change { message[:test] }.from(nil).to(1)) }
    it { expect { middleware.run(message) }.to(change { message[:test2] }.from(nil).to(2)) }
  end

  context 'when non-morphing middleware' do
    before do
      middleware.prepend lambda do |msg|
        msg = msg.dup
        msg[:test] = 1
        msg
      end
    end

    it { expect { middleware.run(message) }.not_to change { message } }
    it { expect(middleware.run(message)[:test]).to eq(1) }
  end

  context 'when non-morphing middlewares' do
    before do
      middleware.prepend lambda do |msg|
        msg = msg.dup

        msg[:test] = 1
        msg
      end

      middleware.prepend lambda do |msg|
        msg = msg.dup

        msg[:test2] = 2
        msg
      end
    end

    it { expect { middleware.run(message) }.not_to change { message } }
    it { expect(middleware.run(message)[:test]).to eq(1) }
    it { expect(middleware.run(message)[:test2]).to eq(2) }
  end

  context 'when morphing middleware on many' do
    before do
      middleware.append lambda do |msg|
        msg[:test] = 1
        msg
      end
    end

    it { expect { middleware.run_many([message]) }.to(change { message[:test] }.from(nil).to(1)) }
  end

  context 'when morphing middlewares on many' do
    before do
      middleware.append lambda do |msg|
        msg[:test] = 1
        msg
      end

      middleware.append lambda do |msg|
        msg[:test2] = 2
        msg
      end
    end

    it { expect { middleware.run_many([message]) }.to(change { message[:test] }.from(nil).to(1)) }
    it { expect { middleware.run_many([message]) }.to(change { message[:test2] }.from(nil).to(2)) }
  end

  context 'when non-morphing middleware on many' do
    before do
      middleware.append lambda do |msg|
        msg = msg.dup
        msg[:test] = 1
        msg
      end
    end

    it { expect { middleware.run_many([message]) }.not_to change { message } }
    it { expect(middleware.run_many([message]).first[:test]).to eq(1) }
  end
end
