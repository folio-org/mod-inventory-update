mvn clean install

curl -X DELETE -D - -w '\n' http://localhost:9130/_/proxy/tenants/diku/modules/mod-inventory-update-0.0.3-SNAPSHOT

sleep 1
curl -X DELETE -D - -w '\n' http://localhost:9130/_/discovery/modules/mod-inventory-update-0.0.3-SNAPSHOT

sleep 1
curl -w '\n' -X POST -D - -H "Content-type: application/json" -d @$FOLIO/install-folio-backend/other-modules/inventory/DeploymentDescriptor-mod-inventory-update.json http://localhost:9130/_/discovery/modules

sleep 1
curl -w '\n' -X POST -D - -H "Content-type: application/json" -d @/home/ne/folio/mod-inventory-update/target/TenantModuleDescriptor.json http://localhost:9130/_/proxy/tenants/diku/modules

