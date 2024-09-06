# WaterDrop

[![Build Status](https://github.com/karafka/waterdrop/workflows/ci/badge.svg)](https://github.com/karafka/waterdrop/actions?query=workflow%3Aci)
[![Gem Version](https://badge.fury.io/rb/waterdrop.svg)](http://badge.fury.io/rb/waterdrop)
[![Join the chat at https://slack.karafka.io](https://raw.githubusercontent.com/karafka/misc/master/slack.svg)](https://slack.karafka.io)

WaterDrop is a standalone gem that sends messages to Kafka easily with an extra validation layer. It is a part of the [Karafka](https://github.com/karafka/karafka) ecosystem.

It:

 - Is thread-safe
 - Supports sync and async producing
 - Supports transactions
 - Supports buffering
 - Supports producing to multiple clusters
 - Supports multiple delivery policies
 - Supports per-topic configuration alterations (variants)
 - Works with Kafka `1.0+` and Ruby `3.1+`
 - Works with and without Karafka

## Documentation

Karafka ecosystem components documentation, including WaterDrop, can be found [here](https://karafka.io/docs/#waterdrop).

## Getting Started

If you want to both produce and consume messages, please use [Karafka](https://github.com/karafka/karafka/). It integrates WaterDrop automatically.

To get started with WaterDrop:

1. Add it to your Gemfile:

```bash
bundle add waterdrop
```

2. Create and configure a producer:

```ruby
producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = {
    'bootstrap.servers': 'localhost:9092',
    'request.required.acks': 1
  }
end
```

3. Use it as follows:


```ruby
# sync producing
producer.produce_sync(topic: 'my-topic', payload: 'my message')

# or for async
producer.produce_async(topic: 'my-topic', payload: 'my message')

# or in sync batches
producer.produce_many_sync(
  [
    { topic: 'my-topic', payload: 'my message'},
    { topic: 'my-topic', payload: 'my message'}
  ]
)

# and async batches
producer.produce_many_async(
  [
    { topic: 'my-topic', payload: 'my message'},
    { topic: 'my-topic', payload: 'my message'}
  ]
)

# transactions
producer.transaction do
  producer.produce_async(topic: 'my-topic', payload: 'my message')
  producer.produce_async(topic: 'my-topic', payload: 'my message')
end
```
