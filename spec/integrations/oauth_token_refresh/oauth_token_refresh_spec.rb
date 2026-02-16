# frozen_string_literal: true

# Integration test for WaterDrop OAuth token refresh functionality
# Tests that the oauthbearer.token_refresh event is properly triggered
# and that tokens can be set via the bearer callback.
#
# This test requires:
# - Keycloak running on port 8080 with the kafka realm configured
# - Kafka with SASL_PLAINTEXT/OAUTHBEARER on port 9094
#
# Run with: docker compose -f docker-compose.oauth.yml up -d
# Then: ./bin/integrations oauth_token_refresh

require "waterdrop"
require "net/http"
require "json"

KEYCLOAK_URL = "http://127.0.0.1:8080"
KAFKA_BOOTSTRAP = "127.0.0.1:9094"
CLIENT_ID = "waterdrop-test"
CLIENT_SECRET = "test-secret"

# Track OAuth callback invocations
oauth_callbacks = []
token_set_success = false
token_errors = []

# Token fetcher for Keycloak
def fetch_token
  puts "Fetching token from Keycloak..."
  uri = URI("#{KEYCLOAK_URL}/realms/kafka/protocol/openid-connect/token")
  response = Net::HTTP.post_form(uri, {
    "grant_type" => "client_credentials",
    "client_id" => CLIENT_ID,
    "client_secret" => CLIENT_SECRET
  })

  puts "Token response status: #{response.code}"

  data = JSON.parse(response.body)

  if data["error"]
    puts "Token error: #{data["error"]} - #{data["error_description"]}"
    raise "Failed to get token: #{data["error"]}"
  end

  puts "Got token, expires_in: #{data["expires_in"]}s"
  data
end

producer = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "OAUTHBEARER"
  }
  # Longer timeout for OAuth flow
  config.max_wait_timeout = 30_000
end

# Subscribe to error events for debugging
producer.monitor.subscribe("error.occurred") do |event|
  puts "ERROR: #{event[:type]} - #{event[:error].class}: #{event[:error].message}"
  token_errors << event[:error]
end

# Subscribe to OAuth token refresh callback
producer.monitor.subscribe("oauthbearer.token_refresh") do |event|
  puts "OAuth token refresh callback triggered!"
  oauth_callbacks << Time.now

  begin
    token_data = fetch_token

    puts "Setting token on bearer..."
    event[:bearer].oauthbearer_set_token(
      token: token_data["access_token"],
      lifetime_ms: token_data["expires_in"] * 1000,
      principal_name: CLIENT_ID
    )
    puts "Token set successfully!"

    token_set_success = true
  rescue => e
    puts "Error in OAuth callback: #{e.class}: #{e.message}"
    puts e.backtrace.first(5).join("\n")

    # Set token failure so rdkafka knows the token refresh failed
    begin
      event[:bearer].oauthbearer_set_token_failure(e.message)
    rescue => set_err
      puts "Failed to set token failure: #{set_err.message}"
    end
  end
end

puts "Starting OAuth integration test..."
puts "Kafka bootstrap: #{KAFKA_BOOTSTRAP}"
puts "Keycloak URL: #{KEYCLOAK_URL}"

# Attempt to produce - this triggers OAuth flow
begin
  puts "Attempting to produce message..."
  producer.produce_sync(topic: "oauth-test", payload: "test message")
  puts "Message produced successfully!"
rescue => e
  puts "Production failed: #{e.class}: #{e.message}"
  puts "OAuth callbacks triggered: #{oauth_callbacks.size}"
  puts "Token errors: #{token_errors.map(&:message).join(", ")}"
  producer.close
  exit 1
end

producer.close

# Verify OAuth callback was invoked
if oauth_callbacks.empty?
  puts "FAIL: OAuth callback was never invoked"
  exit 1
end

if !token_set_success
  puts "FAIL: Token was not set successfully"
  exit 1
end

puts "SUCCESS: OAuth callback invoked #{oauth_callbacks.size} time(s)"
exit 0
