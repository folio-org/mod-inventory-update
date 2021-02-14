tenant=diku
username=diku_admin
password=admin
protocol=http
host=localhost:9130


token=$(curl -s -X POST -D - -H "Content-type: application/json" -H "X-Okapi-Tenant: $tenant"  -d "{ \"username\": \"$username\", \"password\": \"$password\"}" "$protocol://$host/authn/login" | grep x-okapi-token | tr -d '\r' | cut -d " " -f2)

curl -X GET -D - \
  -H "Content-type: application/json" \
  -H "X-Okapi-Tenant: $tenant" \
  -H "X-Okapi-Token: $token" \
  $protocol://$host/instance-storage/instance-relationships?query=\(subInstanceId=$1+or+superInstanceId=$1\)

echo 
echo `date`
echo

