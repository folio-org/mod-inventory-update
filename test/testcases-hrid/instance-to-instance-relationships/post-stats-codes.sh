tenant=diku
username=diku_admin
password=admin
protocol=http
host=localhost:9130


token=$(curl -s -X POST -D - -H "Content-type: application/json" -H "X-Okapi-Tenant: $tenant"  -d "{ \"username\": \"$username\", \"password\": \"$password\"}" "$protocol://$host/authn/login" | grep x-okapi-token | tr -d '\r' | cut -d " " -f2)

curl -X POST -D - \
  -H "Content-type: application/json" \
  -H "X-Okapi-Tenant: $tenant" \
  -H "X-Okapi-Token: $token" \
  -d @statistical-code-type-interim.json $protocol://$host/statistical-code-types

curl -X POST -D - \
  -H "Content-type: application/json" \
  -H "X-Okapi-Tenant: $tenant" \
  -H "X-Okapi-Token: $token" \
  -d @statistical-code-interim.json $protocol://$host/statistical-codes

echo
date
echo
