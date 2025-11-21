@echo off
echo.
echo ====================================
echo CONFIGURACION DE KAFKA
echo ====================================
echo.
echo Tu IP actual es:
ipconfig | findstr "IPv4"
echo.
set /p IP="Introduce tu IP (ejemplo 192.168.1.101): "

echo.
echo [1/4] Limpiando contenedor anterior...
docker stop kafka-network 2>nul
docker rm kafka-network 2>nul

echo.
echo [2/4] Iniciando Kafka con IP: %IP%
docker run -d --name kafka-network -p 9092:9092 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://%IP%:9092 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 apache/kafka:4.1.0

echo.
echo [3/4] Esperando a que Kafka este listo (20 segundos)...
timeout /t 20 /nobreak >nul

echo.
echo [4/4] Creando topics...

echo   - Creando topic 'driver_requests'...
docker exec kafka-network /opt/kafka/bin/kafka-topics.sh --create --topic driver_requests --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>nul
if %ERRORLEVEL% EQU 0 (
    echo     [OK] Topic 'driver_requests' creado
) else (
    echo     [INFO] Topic 'driver_requests' ya existe o error al crear
)

echo   - Creando topic 'cp_status'...
docker exec kafka-network /opt/kafka/bin/kafka-topics.sh --create --topic cp_status --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>nul
if %ERRORLEVEL% EQU 0 (
    echo     [OK] Topic 'cp_status' creado
) else (
    echo     [INFO] Topic 'cp_status' ya existe o error al crear
)

echo   - Creando topic 'ev_events'...
docker exec kafka-network /opt/kafka/bin/kafka-topics.sh --create --topic ev_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>nul
if %ERRORLEVEL% EQU 0 (
    echo     [OK] Topic 'ev_events' creado
) else (
    echo     [INFO] Topic 'ev_events' ya existe o error al crear
)

echo.
echo ====================================
echo VERIFICANDO TOPICS CREADOS
echo ====================================
docker exec kafka-network /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

echo.
echo ====================================
echo KAFKA CONFIGURADO CORRECTAMENTE
echo ====================================
echo   IP: %IP%:9092
echo   Topics: driver_requests, cp_status, ev_events
echo ====================================
echo.
echo Siguiente paso:
echo   python EV_Central.py
echo.
pause