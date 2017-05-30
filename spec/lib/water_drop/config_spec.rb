# frozen_string_literal: true

RSpec.describe WaterDrop::Config do
  subject(:config) { described_class.config }

  %i[
    connection_pool_timeout
    send_messages
    raise_on_failure
    connection_pool_size
  ].each do |attribute|
    describe "#{attribute}=" do
      let(:value) { rand }

      before { config.public_send(:"#{attribute}=", value) }

      it 'assigns a given value' do
        expect(config.public_send(attribute)).to eq value
      end
    end
  end

  %i[
    ca_cert
    client_cert
    client_cert_key
  ].each do |attribute|
    describe "#{attribute}=" do
      let(:value) { rand }

      before { config.kafka.ssl[attribute] = value }

      it 'assigns a given value' do
        expect(config.kafka.ssl[attribute]).to eq value
      end
    end
  end

  describe 'kafka.hosts=' do
    let(:value) { rand }

    before { config.kafka.hosts = value }

    it 'assigns a given value' do
      expect(config.kafka.hosts).to eq value
    end
  end

  describe '.setup' do
    it { expect { |block| described_class.setup(&block) }.to yield_with_args }
  end
end
