# WaterDrop

[![Build Status](https://travis-ci.org/karafka/waterdrop.png)](https://travis-ci.org/karafka/waterdrop)
[![Code Climate](https://codeclimate.com/github/karafka/waterdrop/badges/gpa.svg)](https://codeclimate.com/github/karafka/waterdrop)
[![Gem Version](https://badge.fury.io/rb/waterdrop.svg)](http://badge.fury.io/rb/waterdrop)
[![Join the chat at https://gitter.im/karafka/karafka](https://badges.gitter.im/karafka/karafka.svg)](https://gitter.im/karafka/karafka?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Gem used to send messages to Kafka in an easy way.

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

WaterDrop has following configuration options:

| Option                    | Required   | Value type    | Description                      |
|---------------------------|------------|---------------|----------------------------------|
| send_messages             | true       | Boolean       | Should we send messages to Kafka |
| kafka.hosts               | true       | Array<String> | Kafka servers hosts with ports   |
| connection_pool_size      | true       | Integer       | Kafka connection pool size       |
| connection_pool_timeout   | true       | Integer       | Kafka connection pool timeout    |
| raise_on_failure          | true       | Boolean       | Should we raise an exception when we cannot send message to Kafka - if false will silently ignore failures (will just ignore them) |
| kafka.ssl.ca_cert         | false      | String        | SSL CA certificate               |
| kafka.ssl.client_cert     | false      | String        | SSL client certificate           |
| kafka.ssl.client_cert_key | false      | String        | SSL client certificate password  |
| kafka.topic_prefix        | false      | String        | Prefix to concatenate to topic   |

To apply this configuration, you need to use a *setup* method:

```ruby
WaterDrop.setup do |config|
  config.send_messages = true
  config.connection_pool_size = 20
  config.connection_pool_timeout = 1
  config.kafka.hosts = ['localhost:9092']
  config.raise_on_failure = true
end
```

This configuration can be placed in *config/initializers* and can vary based on the environment:

```ruby
WaterDrop.setup do |config|
  config.send_messages = Rails.env.production?
  config.connection_pool_size = 20
  config.connection_pool_timeout = 1
  config.kafka.hosts = [Rails.env.production? ? 'prod-host:9091' : 'localhost:9092']
  config.raise_on_failure = Rails.env.production?
end
```

## Usage

### Creating and sending standard messages

To send Kafka messages, just create and send messages directly:

```ruby
message = WaterDrop::Message.new('topic', 'message')
message.send!

message = WaterDrop::Message.new('topic', { user_id: 1 }.to_json)
message.send!
```

message that you want to send should be either binary or stringified (to_s, to_json, etc).

### Using aspects to handle messages

WaterDrop no longer depends on [Aspector](https://github.com/gcao/aspector). Please refer to [Aspector](https://github.com/gcao/aspector) documentation if you want to handle messaging in an aspect way.

## References

* [Karafka framework](https://github.com/karafka/karafka)
* [Waterdrop](https://github.com/karafka/waterdrop)
* [Worker Glass](https://github.com/karafka/worker-glass)
* [Envlogic](https://github.com/karafka/envlogic)
* [Null Logger](https://github.com/karafka/null-logger)
* [Aspector](https://github.com/gcao/aspector)
* [WaterDrop Travis CI](https://travis-ci.org/karafka/waterdrop)
* [WaterDrop Code Climate](https://codeclimate.com/github/karafka/waterdrop)

## Note on Patches/Pull Requests

Fork the project.
Make your feature addition or bug fix.
Add tests for it. This is important so I don't break it in a future version unintentionally.
Commit, do not mess with Rakefile, version, or history. (if you want to have your own version, that is fine but bump version in a commit by itself I can ignore when I pull). Send me a pull request. Bonus points for topic branches.

Each pull request must pass our quality requirements. To check if everything is as it should be, we use [PolishGeeks Dev Tools](https://github.com/polishgeeks/polishgeeks-dev-tools) that combine multiple linters and code analyzers. Please run:

```bash
bundle exec rake
```

to check if everything is in order. After that you can submit a pull request.
