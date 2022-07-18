# frozen_string_literal: true

RSpec.describe_current do
  subject(:validator_class) do
    Class.new(described_class) do
      configure do |config|
        config.error_messages = YAML.safe_load(
          File.read(
            File.join(WaterDrop.gem_root, 'config', 'errors.yml')
          )
        ).fetch('en').fetch('validations').fetch('config')
      end

      required(:id) { |id| id.is_a?(String) }
    end
  end

  describe '#validate!' do
    subject(:validation) { validator_class.new.validate!(data, ArgumentError) }

    context 'when data is valid' do
      let(:data) { { id: '1' } }

      it { expect { validation }.not_to raise_error }
    end

    context 'when data is not valid' do
      let(:data) { { id: 1 } }

      it { expect { validation }.to raise_error(ArgumentError) }
    end

  end

  context 'when there are nested values in a contract' do
    let(:validator_class) do
      Class.new(described_class) do
        configure do |config|
          config.error_messages = YAML.safe_load(
            File.read(
              File.join(WaterDrop.gem_root, 'config', 'errors.yml')
            )
          ).fetch('en').fetch('validations').fetch('test')
        end

        nested(:nested) do
          required(:id) { |id| id.is_a?(String) }
          optional(:id2) { |id| id.is_a?(String) }
        end
      end
    end

    describe '#validate!' do
      subject(:validation) { validator_class.new.validate!(data, ArgumentError) }

      context 'when data is valid without optional' do
        let(:data) { { nested: { id: '1' } } }

        it { expect { validation }.not_to raise_error }
      end

      context 'when data is valid with optional' do
        let(:data) { { nested: { id: '1', id2: '2' } } }

        it { expect { validation }.not_to raise_error }
      end

      context 'when data is not valid with invalid optional' do
        let(:data) { { nested: { id: '1', id2: 2 } } }

        it { expect { validation }.to raise_error(ArgumentError) }
      end

      context 'when data is not valid' do
        let(:data) { { id: 1 } }

        it { expect { validation }.to raise_error(ArgumentError) }
      end
    end
  end

  context 'when contract has its own error reported' do
    let(:validator_class) do
      Class.new(described_class) do
        virtual do
          [[%i[id], 'String error']]
        end
      end
    end

    subject(:validation) { validator_class.new.validate!(data, ArgumentError) }

    context 'when data is valid without optional' do
      let(:data) { { nested: { id: '1' } } }

      it { expect { validation }.to raise_error(ArgumentError) }
    end
  end
end
