# frozen_string_literal: true

# rubocop:disable RSpec/VerifiedDoubles
RSpec.describe_current do
  subject(:pipe) { described_class.new(client) }

  let(:client) do
    double(:rdkafka_producer, enable_queue_io_events: nil)
  end

  after do
    pipe.close
  rescue StandardError
    # Ignore if pipe was never created
  end

  describe "#initialize" do
    it "creates a readable IO" do
      expect(pipe.reader).to be_a(IO)
    end

    it "calls enable_queue_io_events on the client" do
      expect(client).to receive(:enable_queue_io_events).with(kind_of(Integer))
      described_class.new(client)
    end

    it "is not closed by default" do
      expect(pipe.closed?).to be(false)
    end

    context "when enable_queue_io_events raises" do
      let(:client) do
        double(:rdkafka_producer).tap do |c|
          allow(c).to receive(:enable_queue_io_events).and_raise(StandardError, "test error")
        end
      end

      it "propagates the error" do
        expect { described_class.new(client) }.to raise_error(StandardError, "test error")
      end
    end
  end

  describe "#drain" do
    it "does not raise when pipe is empty" do
      expect { pipe.drain }.not_to raise_error
    end
  end

  describe "#close" do
    it "marks the pipe as closed" do
      pipe.close
      expect(pipe.closed?).to be(true)
    end

    it "closes the reader IO" do
      reader = pipe.reader
      pipe.close
      expect(reader.closed?).to be(true)
    end

    it "can be called multiple times safely" do
      expect do
        3.times { pipe.close }
      end.not_to raise_error
    end
  end

  describe "#reader" do
    it "can be used with IO.select" do
      # We can't easily trigger librdkafka to write to the pipe in a unit test,
      # but we can verify the reader is a valid IO for select
      expect(pipe.reader).to respond_to(:read_nonblock)
    end
  end
end
# rubocop:enable RSpec/VerifiedDoubles
