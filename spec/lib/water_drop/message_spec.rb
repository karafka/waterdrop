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
          .to receive_message_chain(:config, :kafka, :topic_mapper)
          .and_return(nil)

        expect(WaterDrop::Pool).to receive(:with).and_yield(producer)
        expect(producer).to receive(:send_message)
          .with(any_args)
      end

      it 'sends message with topic and message' do
        subject.send!
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
    let(:mapper) { nil }

    before do
      allow(WaterDrop)
        .to receive_message_chain(:config, :kafka, :topic_mapper)
        .and_return(mapper)
    end

    context 'with a topic mapper' do
      let(:mapper) { ->(topic_in) { "#{topic_in}cat" } }

      it 'adds the mapper to the topic' do
        expect(mapper).to receive(:call).with(topic.to_s).and_call_original
        expect(subject.topic).to include('cat')
      end
    end

    context 'without a topic mapper' do
      it 'does not change the topic' do
        expect(subject.topic).to eql(topic.to_s)
      end
    end

    context 'with a different type' do
      let(:mapper) { 'foobar' }

      it 'does not change the topic' do
        expect(subject.topic).to eql(topic.to_s)
      end
    end
  end
end
