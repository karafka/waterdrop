# WaterDrop

[![Build Status](https://travis-ci.org/karafka/waterdrop.png)](https://travis-ci.org/karafka/waterdrop)
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

| Option                      | Required   | Value type      | Description                                                                        |
|-----------------------------|------------|-----------------|------------------------------------------------------------------------------------|
| send_messages               | true       | Boolean         | Should we send messages to Kafka                                                   |
| connect_timeout             | false      | Integer         | Number of seconds to wait while connecting to a broker for the first time          |
| required_acks               | false      | Symbol, Integer | [:all, 0, 1] acknowledgement level for Kafka                                       |
| socket_timeout              | false      | Integer         | Number of seconds to wait when reading from or writing to a socket                 |
| connection_pool.size        | true       | Integer         | Kafka connection pool size                                                         |
| connection_pool.timeout     | true       | Integer         | Kafka connection pool timeout                                                      |
| kafka.seed_brokers          | true       | Array<String>   | Kafka servers hosts with ports                                                     |
| raise_on_failure            | true       | Boolean         | Should we raise an exception when we cannot send message to Kafka - if false will silently ignore failures |
| kafka.ssl_ca_cert           | false      | String          | SSL CA certificate                                                                 |
| kafka.ssl_ca_cert_file_path | false      | String          | SSL CA certificate file path                                                       |
| kafka.ssl_client_cert       | false      | String          | SSL client certificate                                                             |
| kafka.ssl_client_cert_key   | false      | String          | SSL client certificate password                                                    |
| kafka.sasl_gssapi_principal | false      | String          | SASL principal                                                                     |
| kafka.sasl_gssapi_keytab    | false      | String          | SASL keytab                                                                        |
| kafka.sasl_plain_authzid    | false      | String          | The authorization identity to use                                                  |
| kafka.sasl_plain_username   | false      | String          | The username used to authenticate                                                  |
| kafka.sasl_plain_password   | false      | String          | The password used to authenticate                                                  |

To apply this configuration, you need to use a *setup* method:

```ruby
WaterDrop.setup do |config|
  config.send_messages = true
  config.connection_pool.size = 20
  config.connection_pool.timeout = 1
  config.kafka.seed_brokers = ['localhost:9092']
  config.raise_on_failure = true
end
```

This configuration can be placed in *config/initializers* and can vary based on the environment:

```ruby
WaterDrop.setup do |config|
  config.send_messages = Rails.env.production?
  config.connection_pool.size = 20
  config.connection_pool.timeout = 1
  config.kafka.seed_brokers = [Rails.env.production? ? 'prod-host:9091' : 'localhost:9092']
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

## References

* [Karafka framework](https://github.com/karafka/karafka)
* [WaterDrop Travis CI](https://travis-ci.org/karafka/waterdrop)
* [WaterDrop Coditsu](https://app.coditsu.io/karafka/repositories/waterdrop)

## Note on Patches/Pull Requests

Fork the project.
Make your feature addition or bug fix.
Add tests for it. This is important so we don't break it in a future versions unintentionally.
Commit, do not mess with Rakefile, version, or history. (if you want to have your own version, that is fine but bump version in a commit by itself I can ignore when I pull). Send me a pull request. Bonus points for topic branches.

[![coditsu](https://coditsu.io/assets/quality_bar.svg)](https://app.coditsu.io/karafka/repositories/waterdrop)

Each pull request must pass our quality requirements. To check if everything is as it should be, we use [Coditsu](https://coditsu.io) that combinse multiple linters and code analyzers for both code and documentation.

Unfortunately, it does not yet support independent forks, however you should be fine by looking at what we require.

Please run:

```bash
bundle exec rake
```

to check if everything is in order. After that you can submit a pull request.
