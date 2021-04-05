# WaterDrop changelog

## Unreleased
- Remove Ruby 2.5 support and update minimum Ruby requirement to 2.6

## 1.4.2 (2021-03-30)
- Additional 3.0 fixes (ojab)

## 1.4.1 (2021-03-23)
- Support for Ruby 3.0

## 1.4.0 (2020-08-25)
- Release to match Karafka 1.4 versioning.

## 1.3.4 (2020-02-17)
- Support for new `dry-configurable`

## 1.3.3 (2019-01-06)
- #119 - Support exactly once delivery and transactional messaging (kylekthompson)
- #119 - Support delivery_boy 1.0 (kylekthompson)

## 1.3.2 (2019-26-12)
- Ruby 2.7.0 support
- Fix missing `delegate` dependency on `ruby-kafka`

## 1.3.1 (2019-10-21)
- Ruby 2.6.5 support
- Expose setting to optionally verify hostname on ssl certs #109 (tabdollahi)

## 1.3.0 (2019-09-09)
- Drop Ruby 2.4 support

## 1.3.0.rc1 (2019-07-31)
- Drop Ruby 2.3 support
- Drop support for Kafka 0.10 in favor of native support for Kafka 0.11.
- Ruby 2.6.3 support
- Support message headers
- `sasl_over_ssl` support
- Unlock Ruby Kafka + provide support for 0.7 only
- #60 - Rename listener to StdoutListener
- Drop support for Kafka 0.10 in favor of native support for Kafka 0.11.
- Support ruby-kafka 0.7
- Support message headers
- `sasl_over_ssl` support
- `ssl_client_cert_key_password` support
- #87 - Make stdout listener as instance
- Use Zeitwerk for gem code loading
- #93 - zstd compression support
- #99 - schemas are renamed to contracts
- Bump delivery_boy (0.2.7 => 0.2.8)

## 1.2.5
- Bump dependencies to match Karafka
- drop jruby support
- drop ruby 2.2 support

## 1.2.4
- Due to multiple requests, unlock of 0.7 with an additional post-install message

## 1.2.3
- Lock ruby-kafka to 0.6 (0.7 support targeted for WaterDrop 1.3)

## 1.2.2
- #55 - Codec settings unification and config applier

## 1.2.1
- #54 - compression_codec api sync with king-konf requirements

## 1.2.0
- #45 - Allow specifying a create time for messages
- #47 - Support SCRAM once released
- #49 - Add lz4 support once merged and released
- #50 - Potential message loss in async mode
- Ruby 2.5.0 support
- Gem bump to match Karafka framework versioning
- #48 - ssl_ca_certs_from_system
- #52 - Use instrumentation compatible with Karafka 1.2

## 1.0.1
- Added high level retry on connection problems

## 1.0.0

- #37 - ack level for producer
- Gem bump
- Ruby 2.4.2 support
- Raw ruby-kafka driver is now replaced with delivery_boy
- Sync and async producers
- Complete update of the API
- Much better validations for config details
- Complete API remodel - please read the new README
- Renamed send_messages to deliver

## 0.4
- Bump to match Karafka
- Renamed ```hosts``` to ```seed_brokers```
- Removed the ```ssl``` scoping for ```kafka``` config namespace to better match Karafka conventions
- Added ```client_id``` option on a root config level
- Added ```logger``` option on a root config level
- Auto Propagation of config down to ruby-kafka

## 0.3.2
- Removed support for Ruby 2.1.*
- ~~Ruby 2.3.3 as default~~
- Ruby 2.4.0 as default
- Gem dump x2
- Dry configurable config (#20)
- added .rspec for default spec helper require
- Added SSL capabilities
- Coditsu instead of PG dev tools for quality control

## 0.3.1
- Dev tools update
- Gem update
- Specs updates
- File naming convention fix from waterdrop to water_drop + compatibility file
- Additional params (partition, etc) that can be passed into producer

## 0.3.0
- Driver change from Poseidon (not maintained) to Ruby-Kafka

## 0.2.0
- Version dump - this WaterDrop version no longer relies on Aspector to work
- #17 - Logger for Aspector - WaterDrop no longer depends on Aspector
- #8 - add send date as a default value added to a message - wont-fix. Should be implemented on a message level since WaterDrop just transports messages without adding additional stuff.
- #11 - same as above

## 0.1.13
- Resolved bug #15. When you use waterdrop in aspect way, message will be automatically parse to JSON.

## 0.1.12
- Removed default to_json casting because of binary/other data types incompatibility. This is an incompatibility. If you use WaterDrop, please add a proper casting method to places where you use it.
- Gem dump

## 0.1.11
- Poseidon options extractions and tweaks

## 0.1.10
- Switched raise_on_failure to ignore all StandardError failures (or not to), not just specific once
- Reloading inside connection pool connection that seems to be broken (one that failed) - this should prevent from multiple issues (but not from single one) that are related to the connection

## 0.1.9
- Required acks and set to -1 (most secure but slower)
- Added a proxy layer to to producer so we could replace Kafka with other messaging systems
- Gem dump

## 0.1.8
- proper poseidon clients names (not duplicated)

## 0.1.7
- kafka_host, kafka_hosts and kafka_ports settings consistency fix

## 0.1.6
- Added null-logger gem

## 0.1.5
- raise_on_failure flag to ignore (if false) that message was not sent

## 0.1.4
- Renamed WaterDrop::Event to WaterDrop::Message to follow Apache Kafka naming convention

## 0.1.3
- Gems cleanup
- Requirements fix

## 0.1.2
- Initial gem release
