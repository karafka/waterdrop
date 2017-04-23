RSpec.describe WaterDrop::Message do
  let(:topic) { double }
  let(:message) { [] }
  let(:producer) { double }

  subject { described_class.new(topic, message) }

  describe '#send!' do
    let(:message_producer) { double }
    let(:messages) { double }

    context 'when everything is ok' do
      before do
        allow(WaterDrop)
          .to receive_message_chain(:config, :send_messages)
          .and_return(true)

        allow(WaterDrop)
          .to receive_message_chain(:config, :kafka, :topic_prefix)
          .and_return(nil)

        expect(WaterDrop::Pool).to receive(:with).and_yield(producer)
        expect(producer).to receive(:send_message)
          .with(any_args)
      end
    end

    [StandardError].each do |error|
      let(:config) do
        WaterDrop::Config.config.tap do |config|
          config.raise_on_failure = raise_on_failure
          config.send_messages = true
        end
      end

      context "when #{error} happens" do
        before do
          expect(::WaterDrop)
            .to receive(:config)
            .and_return(config)
            .exactly(2).times

          expect(::WaterDrop::Pool)
            .to receive(:with)
            .and_raise(error)
        end

        context 'and raise_on_failure is set to false' do
          let(:raise_on_failure) { false }

          it { expect { subject.send! }.not_to raise_error }
        end

        context 'and raise_on_failure is set to true' do
          let(:raise_on_failure) { true }

          it { expect { subject.send! }.to raise_error(error) }
        end
      end
    end
  end

  describe '#topic' do
    let(:prefix) { nil }

    before do
      allow(WaterDrop)
        .to receive_message_chain(:config, :kafka, :topic_prefix)
        .and_return(prefix)
    end

    context 'with a topic prefix' do
      let(:prefix) { 'cat' }
      
      it 'adds the prefix to the topic' do
        expect(subject.topic).to eql("#{prefix}#{topic}")
      end
    end

    context 'without a topic prefix' do
      it 'does not change the topic' do
        expect(subject.topic).to eql(topic.to_s)
      end
    end
  end
end
