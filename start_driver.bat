@echo off
setlocal enabledelayedexpansion

title EV Drivers Launcher

echo ========================================
echo       LEVANTADOR DE COCHES EV
echo ========================================

REM Configuración por defecto - MODIFICA ESTOS VALORES
set DEFAULT_KAFKA_HOST=localhost
set DEFAULT_KAFKA_PORT=9092
set DEFAULT_NUM_DRIVERS=3
set DRIVER_PREFIX=DRV

echo.
echo Configuracion actual:
echo   Kafka Host: %DEFAULT_KAFKA_HOST%
echo   Kafka Port: %DEFAULT_KAFKA_PORT%
echo   Numero de coches: %DEFAULT_NUM_DRIVERS%
echo   Prefijo drivers: %DRIVER_PREFIX%
echo.

set /p KAFKA_HOST="Introduce el host de Kafka [%DEFAULT_KAFKA_HOST%]: "
if "!KAFKA_HOST!"=="" set KAFKA_HOST=%DEFAULT_KAFKA_HOST%

set /p KAFKA_PORT="Introduce el puerto de Kafka [%DEFAULT_KAFKA_PORT%]: "
if "!KAFKA_PORT!"=="" set KAFKA_PORT=%DEFAULT_KAFKA_PORT%

set /p NUM_DRIVERS="Cuantas instancias de coches quieres levantar? [%DEFAULT_NUM_DRIVERS%]: "
if "!NUM_DRIVERS!"=="" set NUM_DRIVERS=%DEFAULT_NUM_DRIVERS%

echo.
echo ========================================
echo Iniciando !NUM_DRIVERS! coches...
echo Kafka: !KAFKA_HOST!:!KAFKA_PORT!
echo ========================================
echo.

REM Verificar que el script Python existe
if not exist "EV_Driver.py" (
    echo ERROR: No se encuentra el archivo EV_Driver.py
    echo Asegurate de que este en la misma carpeta que este .bat
    pause
    exit /b 1
)

REM Iniciar los drivers
for /l %%i in (1,1,!NUM_DRIVERS!) do (
    set DRIVER_ID=!DRIVER_PREFIX!%%i
    echo Iniciando driver: !DRIVER_ID!
    start "EV Driver !DRIVER_ID!" python EV_Driver.py !DRIVER_ID! !KAFKA_HOST! !KAFKA_PORT!
    timeout /t 1 /nobreak >nul
)

echo.
echo ========================================
echo Se han iniciado !NUM_DRIVERS! coches
echo ========================================
echo.
echo Los coches estan ejecutandose en ventanas separadas.
echo Para detenerlos, simplemente cierra las ventanas.
echo.

pause