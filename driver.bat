@echo off
setlocal enabledelayedexpansion

title EV Drivers Launcher

echo ========================================
echo        LEVANTADOR DE COCHES EV
echo ========================================

REM Configuracion por defecto - MODIFICA ESTOS VALORES
set DEFAULT_KAFKA_HOST=localhost
set DEFAULT_KAFKA_PORT=9092
set DEFAULT_NUM_DRIVERS=3
set DRIVER_PREFIX=DRV

REM Array de 40 CPs disponibles
set CP_ARRAY[0]=CP001
set CP_ARRAY[1]=CP002
set CP_ARRAY[2]=CP003
set CP_ARRAY[3]=CP004
set CP_ARRAY[4]=CP005
set CP_ARRAY[5]=CP006
set CP_ARRAY[6]=CP007
set CP_ARRAY[7]=CP008
set CP_ARRAY[8]=CP009
set CP_ARRAY[9]=CP010
set CP_ARRAY[10]=CP011
set CP_ARRAY[11]=CP012
set CP_ARRAY[12]=CP013
set CP_ARRAY[13]=CP014
set CP_ARRAY[14]=CP015
set CP_ARRAY[15]=CP016
set CP_ARRAY[16]=CP017
set CP_ARRAY[17]=CP018
set CP_ARRAY[18]=CP019
set CP_ARRAY[19]=CP020
set CP_ARRAY[20]=CP021
set CP_ARRAY[21]=CP022
set CP_ARRAY[22]=CP023
set CP_ARRAY[23]=CP024
set CP_ARRAY[24]=CP025
set CP_ARRAY[25]=CP026
set CP_ARRAY[26]=CP027
set CP_ARRAY[27]=CP028
set CP_ARRAY[28]=CP029
set CP_ARRAY[29]=CP030
set CP_ARRAY[30]=CP031
set CP_ARRAY[31]=CP032
set CP_ARRAY[32]=CP033
set CP_ARRAY[33]=CP034
set CP_ARRAY[34]=CP035
set CP_ARRAY[35]=CP036
set CP_ARRAY[36]=CP037
set CP_ARRAY[37]=CP038
set CP_ARRAY[38]=CP039
set CP_ARRAY[39]=CP040

set CP_COUNT=40

echo.
echo Configuracion actual:
echo   Kafka Host: %DEFAULT_KAFKA_HOST%
echo   Kafka Port: %DEFAULT_KAFKA_PORT%
echo   Numero de coches: %DEFAULT_NUM_DRIVERS%
echo   Servicios por coche: 10 (aleatorios)
echo   CPs disponibles: 40 (CP001 a CP040)
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
echo Creando ficheros de servicios...
echo ========================================
echo.
echo Seleccionando 10 CPs aleatorios de 40 disponibles para cada driver
echo.

REM Crear carpeta para ficheros si no existe
if not exist "services" mkdir services

REM Contador de servicios totales
set /a TOTAL_SERVICES=0

REM Crear fichero para cada driver
for /l %%i in (1,1,!NUM_DRIVERS!) do (
    set DRIVER_ID=!DRIVER_PREFIX!%%i
    set SERVICE_FILE=services\!DRIVER_ID!_services.txt
    
    echo Creando !SERVICE_FILE!...
    
    REM Borrar fichero si existe
    if exist "!SERVICE_FILE!" del "!SERVICE_FILE!"
    
    REM Variable para mostrar los servicios creados
    set SERVICES_CREATED=
    
    REM Crear 10 servicios aleatorios
    for /l %%j in (1,1,10) do (
        REM Generar indice aleatorio entre 0 y 39
        set /a "RAND_INDEX=!RANDOM! %% !CP_COUNT!"
        
        REM Obtener el CP del array
        call set CP_ID=%%CP_ARRAY[!RAND_INDEX!]%%
        
        REM Escribir al fichero
        echo !CP_ID!>> "!SERVICE_FILE!"
        
        REM Acumular para mostrar (solo primeros 5)
        if %%j LEQ 5 (
            if "!SERVICES_CREATED!"=="" (
                set SERVICES_CREATED=!CP_ID!
            ) else (
                set SERVICES_CREATED=!SERVICES_CREATED!, !CP_ID!
            )
        )
        
        set /a TOTAL_SERVICES+=1
    )
    
    echo   OK - Primeros 5 servicios: !SERVICES_CREATED!, ...
    echo.
)

echo ========================================
echo OK - Se crearon !NUM_DRIVERS! ficheros de servicios
echo OK - Total de servicios generados: !TOTAL_SERVICES!
echo OK - Cada driver tiene 10 servicios aleatorios
echo ========================================

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
    set SERVICE_FILE=services\!DRIVER_ID!_services.txt
    
    echo [%%i/!NUM_DRIVERS!] Iniciando driver: !DRIVER_ID! con !SERVICE_FILE!
    start "EV Driver !DRIVER_ID!" python EV_Driver.py !DRIVER_ID! !KAFKA_HOST! !KAFKA_PORT! !SERVICE_FILE!
    timeout /t 1 /nobreak >nul
)

echo.
echo ========================================
echo   SISTEMA INICIADO CORRECTAMENTE
echo ========================================
echo.
echo   Drivers activos: !NUM_DRIVERS!
echo   Kafka: !KAFKA_HOST!:!KAFKA_PORT!
echo   CPs disponibles: 40 (CP001 a CP040)
echo   Servicios por driver: 10
echo   Ficheros: services\
echo.
echo   Para detener: cierra las ventanas
echo ========================================
echo.

pause
```

---

## **CAMBIOS REALIZADOS:**

✅ **Eliminados códigos de color ANSI** (`[32m`, `[0m`, etc.) que no funcionan en CMD  
✅ **Corregido acceso al array** con `call set CP_ID=%%CP_ARRAY[!RAND_INDEX!]%%`  
✅ **Simplificados los mensajes** (sin símbolos especiales)  
✅ **Espacios eliminados** antes de `>>` en `echo !CP_ID!>> "!SERVICE_FILE!"`  

---

## **SALIDA ESPERADA:**
```
========================================
        LEVANTADOR DE COCHES EV
========================================

Configuracion actual:
  Kafka Host: localhost
  Kafka Port: 9092
  Numero de coches: 3
  Servicios por coche: 10 (aleatorios)
  CPs disponibles: 40 (CP001 a CP040)
  Prefijo drivers: DRV

Cuantas instancias de coches quieres levantar? [3]: 3

========================================
Creando ficheros de servicios...
========================================

Seleccionando 10 CPs aleatorios de 40 disponibles para cada driver

Creando services\DRV1_services.txt...
  OK - Primeros 5 servicios: CP023, CP007, CP031, CP015, CP039, ...

Creando services\DRV2_services.txt...
  OK - Primeros 5 servicios: CP012, CP028, CP005, CP037, CP019, ...

Creando services\DRV3_services.txt...
  OK - Primeros 5 servicios: CP034, CP008, CP026, CP011, CP040, ...

========================================
OK - Se crearon 3 ficheros de servicios
OK - Total de servicios generados: 30
OK - Cada driver tiene 10 servicios aleatorios
========================================

========================================
Iniciando 3 coches...
Kafka: localhost:9092
========================================

[1/3] Iniciando driver: DRV1 con services\DRV1_services.txt
[2/3] Iniciando driver: DRV2 con services\DRV2_services.txt
[3/3] Iniciando driver: DRV3 con services\DRV3_services.txt

========================================
   SISTEMA INICIADO CORRECTAMENTE
========================================

  Drivers activos: 3
  Kafka: localhost:9092
  CPs disponibles: 40 (CP001 a CP040)
  Servicios por driver: 10
  Ficheros: services\

  Para detener: cierra las ventanas
========================================