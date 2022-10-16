# WaterDrop changelog

## Unreleased
- Support for librdkafka 0.13

## 2.4.2 (2022-09-29)
- Allow sending tombstone messages (#267)

## 2.4.1 (2022-08-01)
- Replace local statistics decorator with the one extracted to `karafka-core`.

## 2.4.0 (2022-07-28)
- Small refactor of the DataDog/Statsd listener to align for future extraction to `karafka-common`.
- Replace `dry-monitor` with home-brew notification layer (API compatible) and allow for usage with `ActiveSupport::Notifications`.
- Remove all the common code into `karafka-core` and add it as a dependency.

## 2.3.3 (2022-07-18)
- Replace `dry-validation` with home-brew validation layer and drop direct dependency on `dry-validation`.
- Remove indirect dependency on dry-configurable from DataDog listener (no changes required).

## 2.3.2 (2022-07-17)
- Replace `dry-configurable` with home-brew config and drop direct dependency on `dry-configurable`.

## 2.3.1 (2022-06-17)
- Update rdkafka patches to align with `0.12.0` and `0.11.1` support.

## 2.3.0 (2022-04-03)
-  Rename StdoutListener to LoggerListener (#240)

## 2.2.0 (2022-02-18)
- Add Datadog listener for metrics + errors publishing
- Add Datadog example dashboard template
- Update Readme to show Dd instrumentation usage
- Align the directory namespace convention with gem name (waterdrop => WaterDrop)
- Introduce a common base for validation contracts
- Drop CI support for ruby 2.6
- Require all `kafka` settings to have symbol keys (compatibility with Karafka 2.0 and rdkafka)

## 2.1.0 (2022-01-03)
- Ruby 3.1 support
- Change the error notification key from `error.emitted` to `error.occurred`.
- Normalize error tracking and make all the places publish errors into the same notification endpoint (`error.occurred`).
- Start semantic versioning WaterDrop.

## 2.0.7 (2021-12-03)
- Source code metadata url added to the gemspec
- Replace `:producer` with `:producer_id` in events and update `StdoutListener` accordingly. This change aligns all the events in terms of not publishing the whole producer object in the events.
- Add `error.emitted` into the `StdoutListener`.
- Enable `StdoutLogger` in specs for additional integration coverage.

## 2.0.6 (2021-12-01)
- #218 - Fixes a case, where dispatch of callbacks the same moment a new producer was created could cause a concurrency issue in the manager.
- Fix some unstable specs.

## 2.0.5 (2021-11-28)

### Bug fixes

- Fixes an issue where multiple producers would emit stats of other producers causing the same stats to be published several times (as many times as a number of producers). This could cause invalid reporting for multi-kafka setups.
- Fixes a bug where emitted statistics would contain their first value as the first delta value for first stats emitted.
- Fixes a bug where decorated statistics would include a delta for a root field with non-numeric values.

### Changes and features
- Introduces support for error callbacks instrumentation notifications with `error.emitted` monitor emitted key for tracking background errors that would occur on the producer (disconnects, etc).
- Removes the `:producer` key from `statistics.emitted` and replaces it with `:producer_id` not to inject whole producer into the payload
- Removes the `:producer` key from `message.acknowledged` and replaces it with `:producer_id` not to inject whole producer into the payload
- Cleanup and refactor of callbacks support to simplify the API and make it work with Rdkafka way of things.
- Introduces a callbacks manager concept that will also be within in Karafka `2.0` for both statistics and errors tracking per client.
- Sets default Kafka `client.id` to `waterdrop` when not set.
- Updates specs to always emit statistics for better test coverage.
- Adds statistics and errors integration specs running against Kafka.
- Replaces direct `RSpec.describe` reference with auto-discovery
- Patches `rdkafka` to provide functionalities that are needed for granular callback support.

## 2.0.4 (2021-09-19)
- Update `dry-*` to the recent versions and update settings syntax to match it
- Update Zeitwerk requirement

## 2.0.3 (2021-09-05)
- Remove rdkafka patch in favour of spec topic pre-creation
- Do not close client that was never used upon closing producer

## 2.0.2 (2021-08-13)
- Add support for `partition_key`
- Switch license from `LGPL-3.0` to  `MIT`
- Switch flushing on close to sync

## 2.0.1 (2021-06-05)
- Remove Ruby 2.5 support and update minimum Ruby requirement to 2.6
- Fix the `finalizer references object to be finalized` warning issued with 3.0

## 2.0.0 (2020-12-13)
- Redesign of the whole API (see `README.md` for the use-cases and the current API)
- Replace `ruby-kafka` with `rdkafka`
- Switch license from `MIT` to `LGPL-3.0`
- #113 - Add some basic validations of the kafka scope of the config (Azdaroth)
- Global state removed
- Redesigned metrics that use `rdkafka` internal data + custom diffing
- Restore JRuby support

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
