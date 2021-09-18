# WaterDrop

**Note**: Documentation presented here refers to WaterDrop `2.0.0`.

WaterDrop `2.0` does **not** work with Karafka `1.*` and aims to either work as a standalone producer outside of Karafka `1.*` ecosystem or as a part of not yet released Karafka `2.0.*`.

Please refer to [this](https://github.com/karafka/waterdrop/tree/1.4) branch and its documentation for details about WaterDrop `1.*` usage.

[![Build Status](https://github.com/karafka/waterdrop/workflows/ci/badge.svg)](https://github.com/karafka/waterdrop/actions?query=workflow%3Aci)
[![Gem Version](https://badge.fury.io/rb/waterdrop.svg)](http://badge.fury.io/rb/waterdrop)
[![Join the chat at https://slack.karafka.io](https://raw.githubusercontent.com/karafka/misc/master/slack.svg)](https://slack.karafka.io)

Gem used to send messages to Kafka in an easy way with an extra validation layer. It is a part of the [Karafka](https://github.com/karafka/karafka) ecosystem.

It:

 - Is thread safe
 - Supports sync producing
 - Supports async producing
 - Supports buffering
 - Supports producing messages to multiple clusters
 - Supports multiple delivery policies
 - Works with Kafka 1.0+ and Ruby 2.6+

## Table of contents

- [WaterDrop](#waterdrop)
  * [Table of contents](#table-of-contents)
  * [Installation](#installation)
  * [Setup](#setup)
    + [WaterDrop configuration options](#waterdrop-configuration-options)
    + [Kafka configuration options](#kafka-configuration-options)
  * [Usage](#usage)
    + [Basic usage](#basic-usage)
    + [Buffering](#buffering)
      - [Using WaterDrop to buffer messages based on the application logic](#using-waterdrop-to-buffer-messages-based-on-the-application-logic)
      - [Using WaterDrop with rdkafka buffers to achieve periodic auto-flushing](#using-waterdrop-with-rdkafka-buffers-to-achieve-periodic-auto-flushing)
  * [Instrumentation](#instrumentation)
    + [Usage statistics](#usage-statistics)
    + [Forking and potential memory problems](#forking-and-potential-memory-problems)
  * [References](#references)
  * [Note on contributions](#note-on-contributions)

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

- WaterDrop options - options directly related to WaterDrop and its components
- Kafka driver options - options related to `rdkafka`

To apply all those configuration options, you need to create a producer instance and use the ```#setup``` method:

```ruby
producer = WaterDrop::Producer.new

producer.setup do |config|
  config.deliver = true
  config.kafka = {
    'bootstrap.servers': 'localhost:9092',
    'request.required.acks': 1
  }
end
```

or you can do the same while initializing the producer:

```ruby
producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = {
    'bootstrap.servers': 'localhost:9092',
    'request.required.acks': 1
  }
end
```

### WaterDrop configuration options

| Option             | Description                                                     |
|--------------------|-----------------------------------------------------------------|
| `id`               | id of the producer for instrumentation and logging              |
| `logger`           | Logger that we want to use                                      |
| `deliver`          | Should we send messages to Kafka or just fake the delivery      |
| `max_wait_timeout` | Waits that long for the delivery report or raises an error      |
| `wait_timeout`     | Waits that long before re-check of delivery report availability |

### Kafka configuration options

You can create producers with different `kafka` settings. Documentation of the available configuration options is available on https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.

## Usage

Please refer to the [documentation](https://www.rubydoc.info/gems/waterdrop) in case you're interested in the more advanced API.

### Basic usage

To send Kafka messages, just create a producer and use it:

```ruby
producer = WaterDrop::Producer.new

producer.setup do |config|
  config.kafka = { 'bootstrap.servers': 'localhost:9092' }
end

producer.produce_sync(topic: 'my-topic', payload: 'my message')

# or for async
producer.produce_async(topic: 'my-topic', payload: 'my message')

# or in batches
producer.produce_many_sync(
  [
    { topic: 'my-topic', payload: 'my message'},
    { topic: 'my-topic', payload: 'my message'}
  ]
)

# both sync and async
producer.produce_many_async(
  [
    { topic: 'my-topic', payload: 'my message'},
    { topic: 'my-topic', payload: 'my message'}
  ]
)

# Don't forget to close the producer once you're done to flush the internal buffers, etc
producer.close
```

Each message that you want to publish, will have its value checked.

Here are all the things you can provide in the message hash:

| Option          | Required | Value type    | Description                                              |
|-----------------|----------|---------------|----------------------------------------------------------|
| `topic`         | true     | String        | The Kafka topic that should be written to                |
| `payload`       | true     | String        | Data you want to send to Kafka                           |
| `key`           | false    | String        | The key that should be set in the Kafka message          |
| `partition`     | false    | Integer       | A specific partition number that should be written to    |
| `partition_key` | false    | String        | Key to indicate the destination partition of the message |
| `timestamp`     | false    | Time, Integer | The timestamp that should be set on the message          |
| `headers`       | false    | Hash          | Headers for the message                                  |

Keep in mind, that message you want to send should be either binary or stringified (to_s, to_json, etc).

### Buffering

WaterDrop producers support buffering messages in their internal buffers and on the `rdkafka` level via `queue.buffering.*` set of settings.

This means that depending on your use case, you can achieve both granular buffering and flushing control when needed with context awareness and periodic and size-based flushing functionalities.

#### Using WaterDrop to buffer messages based on the application logic

```ruby
producer = WaterDrop::Producer.new

producer.setup do |config|
  config.kafka = { 'bootstrap.servers': 'localhost:9092' }
end

# Simulating some events states of a transaction - notice, that the messages will be flushed to
# kafka only upon arrival of the `finished` state.
%w[
  started
  processed
  finished
].each do |state|
  producer.buffer(topic: 'events', payload: state)

  puts "The messages buffer size #{producer.messages.size}"
  producer.flush_sync if state == 'finished'
  puts "The messages buffer size #{producer.messages.size}"
end

producer.close
```

#### Using WaterDrop with rdkafka buffers to achieve periodic auto-flushing

```ruby
producer = WaterDrop::Producer.new

producer.setup do |config|
  config.kafka = {
    'bootstrap.servers': 'localhost:9092',
    # Accumulate messages for at most 10 seconds
    'queue.buffering.max.ms' => 10_000
  }
end

# WaterDrop will flush messages minimum once every 10 seconds
30.times do |i|
  producer.produce_async(topic: 'events', payload: i.to_s)
  sleep(1)
end

producer.close
```

## Instrumentation

Each of the producers after the `#setup` is done, has a custom monitor to which you can subscribe.

```ruby
producer = WaterDrop::Producer.new

producer.setup do |config|
  config.kafka = { 'bootstrap.servers': 'localhost:9092' }
end

producer.monitor.subscribe('message.produced_async') do |event|
  puts "A message was produced to '#{event[:message][:topic]}' topic!"
end

producer.produce_async(topic: 'events', payload: 'data')

producer.close
```

See the `WaterDrop::Instrumentation::Monitor::EVENTS` for the list of all the supported events.

### Usage statistics

WaterDrop may be configured to emit internal metrics at a fixed interval by setting the `kafka` `statistics.interval.ms` configuration property to a value > `0`. Once that is done, emitted statistics are available after subscribing to the `statistics.emitted` publisher event.

The statistics include all of the metrics from `librdkafka` (full list [here](https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md)) as well as the diff of those against the previously emitted values.

For several attributes like `txmsgs`, `librdkafka` publishes only the totals. In order to make it easier to track the progress (for example number of messages sent between statistics emitted events), WaterDrop diffs all the numeric values against previously available numbers. All of those metrics are available under the same key as the metric but with additional `_d` postfix:


```ruby
producer = WaterDrop::Producer.new do |config|
  config.kafka = {
    'bootstrap.servers': 'localhost:9092',
    'statistics.interval.ms': 2_000 # emit statistics every 2 seconds
  }
end

producer.monitor.subscribe('statistics.emitted') do |event|
  sum = event[:statistics]['txmsgs']
  diff = event[:statistics]['txmsgs_d']

  p "Sent messages: #{sum}"
  p "Messages sent from last statistics report: #{diff}"
end

sleep(2)

# Sent messages: 0
# Messages sent from last statistics report: 0

20.times { producer.produce_async(topic: 'events', payload: 'data') }

# Sent messages: 20
# Messages sent from last statistics report: 20

sleep(2)

20.times { producer.produce_async(topic: 'events', payload: 'data') }

# Sent messages: 40
# Messages sent from last statistics report: 20

sleep(2)

# Sent messages: 40
# Messages sent from last statistics report: 0

producer.close
```

Note: The metrics returned may not be completely consistent between brokers, toppars and totals, due to the internal asynchronous nature of librdkafka. E.g., the top level tx total may be less than the sum of the broker tx values which it represents.

### Forking and potential memory problems

If you work with forked processes, make sure you **don't** use the producer before the fork. You can easily configure the producer and then fork and use it.

To tackle this [obstacle](https://github.com/appsignal/rdkafka-ruby/issues/15) related to rdkafka, WaterDrop adds finalizer to each of the producers to close the rdkafka client before the Ruby process is shutdown. Due to the [nature of the finalizers](https://www.mikeperham.com/2010/02/24/the-trouble-with-ruby-finalizers/), this implementation prevents producers from being GCed (except upon VM shutdown) and can cause memory leaks if you don't use persistent/long-lived producers in a long-running process or if you don't use the `#close` method of a producer when it is no longer needed. Creating a producer instance for each message is anyhow a rather bad idea, so we recommend not to.

## References

* [WaterDrop code documentation](https://www.rubydoc.info/github/karafka/waterdrop)
* [Karafka framework](https://github.com/karafka/karafka)
* [WaterDrop Actions CI](https://github.com/karafka/waterdrop/actions?query=workflow%3Ac)
* [WaterDrop Coditsu](https://app.coditsu.io/karafka/repositories/waterdrop)

## Note on contributions

First, thank you for considering contributing to the Karafka ecosystem! It's people like you that make the open source community such a great community!

Each pull request must pass all the RSpec specs, integration tests and meet our quality requirements.

Fork it, update and wait for the Github Actions results.
