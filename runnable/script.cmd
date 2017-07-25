start "kvServer: 50000" java -jar .\cdb-server.jar 50000
start "kvServer: 50001" java -jar .\cdb-server.jar 50001
start "kvServer: 50002" java -jar .\cdb-server.jar 50002
start "ECSServer: 40000" java -jar .\cdb-ecs.jar
start "kvClient" java -jar .\cdb-client.jar