# WaterDrop

[![Build Status](https://travis-ci.org/karafka/waterdrop.png)](https://travis-ci.org/karafka/waterdrop)
[![Join the chat at https://gitter.im/karafka/karafka](https://badges.gitter.im/karafka/karafka.svg)](https://gitter.im/karafka/karafka?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Gem used to send messages to Kafka in an easy way with an extra validation layer. It is a part of the [Karafka](https://github.com/karafka/karafka) ecosystem.

WaterDrop is based on Zendesks [delivery_boy](https://github.com/zendesk/delivery_boy) gem.

It is:

 - Thread safe
 - Supports sync and async producers
 - Working with 0.10.1+ Kafka

## Installation

```ruby
gem install waterdrop
```

or add this to your Gemfile:

```ruby
gem 'waterdrop'
```

and run

```
bundle install
```

## Setup

WaterDrop is a complex tool, that contains multiple configuration options. To keep everything organized, all the configuration options were divided into two groups:

- WaterDrop options - options directly related to Karafka framework and it's components
- Ruby-Kafka driver options - options related to Ruby-Kafka/Delivery boy

To apply all those configuration options, you need to use the ```#setup``` method:

```ruby
WaterDrop.setup do |config|
  config.deliver = true
  config.kafka.seed_brokers = %w[kafka://localhost:9092]
end
```

### WaterDrop configuration options

| Option                      | Description                                                      |
|-----------------------------|------------------------------------------------------------------|
| client_id                   | This is how the client will identify itself to the Kafka brokers |
| logger                      | Logger that we want to use                                       |
| deliver                     | Should we send messages to Kafka                                 |

### Ruby-Kafka driver and Delivery boy configuration options

**Note:** We've listed here only **the most important** configuration options. If you're interested in all the options, please go to the [config.rb](https://github.com/karafka/waterdrop/blob/master/lib/water_drop/config.rb) file for more details.

**Note:** All the options are subject to validations. In order to check what is and what is not acceptable, please go to the [config.rb validation schema](https://github.com/karafka/waterdrop/blob/master/lib/water_drop/schemas/config.rb) file.

| Option                   | Description                                                                                                                                           |
|--------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| raise_on_buffer_overflow | Should we raise an exception, when messages can't be sent in an async way due to the message buffer overflow or should we just drop them              |
| delivery_interval        | The number of seconds between background message deliveries. Disable timer-based background deliveries by setting this to 0.                          |
| delivery_threshold       | The number of buffered messages that will trigger a background message delivery. Disable buffer size based background deliveries by setting this to 0.|
| required_acks            | The number of Kafka replicas that must acknowledge messages before they're considered as successfully written.                                        |
| ack_timeout              | A timeout executed by a broker when the client is sending messages to it.                                                                             |
| max_retries              | The number of retries when attempting to deliver messages.                                                                                            |
| retry_backoff            | The number of seconds to wait after a failed attempt to send messages to a Kafka broker before retrying.                                              |
| max_buffer_bytesize      | The maximum number of bytes allowed in the buffer before new messages are rejected.                                                                   |
| max_buffer_size          | The maximum number of messages allowed in the buffer before new messages are rejected.                                                                |
| max_queue_size           | The maximum number of messages allowed in the queue before new messages are rejected.                                                                 |
| sasl_plain_username      | The username used to authenticate.                                                                                                                    |
| sasl_plain_password      | The password used to authenticate.                                                                                                                    |

This configuration can be also placed in *config/initializers* and can vary based on the environment:

```ruby
WaterDrop.setup do |config|
  config.deliver = Rails.env.production?
  config.kafka.seed_brokers = [Rails.env.production? ? 'kafka://prod-host:9091' : 'kafka://localhost:9092']
end
```

## Usage

To send Kafka messages, just use one of the producers:

```ruby
WaterDrop::SyncProducer.call('message', topic: 'my-topic')
# or for async
WaterDrop::AsyncProducer.call('message', topic: 'my-topic')
```

Both ```SyncProducer``` and ```AsyncProducer``` accept following options:

| Option              | Required | Value type | Description                                                         |
|-------------------- |----------|------------|---------------------------------------------------------------------|
| ```topic```         | true     | String     | The Kafka topic that should be written to                           |
| ```key```           | false    | String     | The key that should be set in the Kafka message                     |
| ```partition```     | false    | Integer    | A specific partition number that should be written to               |
| ```partition_key``` | false    | String     | A string that can be used to deterministically select the partition |
| ```create_time```   | false    | Time       | The timestamp that should be set on the message                     |

Keep in mind, that message you want to send should be either binary or stringified (to_s, to_json, etc).

## References

* [Karafka framework](https://github.com/karafka/karafka)
* [WaterDrop Travis CI](https://travis-ci.org/karafka/waterdrop)
* [WaterDrop Coditsu](https://app.coditsu.io/karafka/repositories/waterdrop)

## Note on contributions

First, thank you for considering contributing to WaterDrop! It's people like you that make the open source community such a great community!

Each pull request must pass all the RSpec specs and meet our quality requirements.

To check if everything is as it should be, we use [Coditsu](https://coditsu.io) that combines multiple linters and code analyzers for both code and documentation. Once you're done with your changes, submit a pull request.

Coditsu will automatically check your work against our quality standards. You can find your commit check results on the [builds page](https://app.coditsu.io/karafka/repositories/waterdrop/builds/commit_builds) of WaterDrop repository.

[![coditsu](https://coditsu.io/assets/quality_bar.svg)](https://app.coditsu.io/karafka/repositories/waterdrop/builds/commit_builds)
