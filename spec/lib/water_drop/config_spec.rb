# frozen_string_literal: true

RSpec.describe WaterDrop::Config do
  subject(:config) { described_class.config }

  %i[
    client_id
    logger
    send_messages
    raise_on_failure
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
    size
    timeout
  ].each do |attribute|
    describe "#{attribute}=" do
      let(:value) { rand }

      before { config.connection_pool[attribute] = value }

      it 'assigns a given value' do
        expect(config.connection_pool[attribute]).to eq value
      end
    end
  end

  %i[
    ssl_ca_cert
    ssl_ca_cert_file_path
    ssl_client_cert
    ssl_client_cert_key
    sasl_gssapi_principal
    sasl_gssapi_keytab
    sasl_plain_authzid
    sasl_plain_username
    sasl_plain_password
  ].each do |attribute|
    describe "#{attribute}=" do
      let(:value) { rand }

      before { config.kafka[attribute] = value }

      it 'assigns a given value' do
        expect(config.kafka[attribute]).to eq value
      end
    end
  end

  describe 'kafka.seed_brokers=' do
    let(:value) { rand }

    before { config.kafka.seed_brokers = value }

    it 'assigns a given value' do
      expect(config.kafka.seed_brokers).to eq value
    end
  end

  describe '.setup' do
    it { expect { |block| described_class.setup(&block) }.to yield_with_args }
  end
end
