@echo off && setlocal
	
SET HMIS_HOME=C:\Program Files (x86)\DHIS2
SET DHIS2_HOME=%HMIS_HOME%\DHIS
SET FILES_HOME=%DHIS2_HOME%\files

SET COMMON_FUNCTIONS_SQL=common_function.sql
SET MENTAL_HEALTH_SQL=mental_health.sql
SET MAIN_SQL=main.sql

FOR /F "usebackq eol=; tokens=1,2* delims==" %%i IN ("%DHIS2_HOME%\dhis.conf") DO CALL :read_params %%i %%j

	GOTO execute_sql
	:read_params

		IF "%1%"=="hibernate.connection.username" (
			SET DHIS2_DB_USERNAME=%2
		)

		IF "%1%"=="hibernate.connection.password" (
			SET DHIS2_DB_PASSWORD=%2
		)

		IF "%1%"=="hibernate.connection.url" (
			SET FULL_URL=%2

			:: Let's suppose the url has a syntax like "jdbc:postgresql://localhost:5544/dhis2"
			FOR /F "tokens=1,2,3,4,5 delims=/:" %%a in ("%FULL_URL%") do (
				SET DHIS2_HOST=%%c
				SET DHIS2_DB_PORT=%%d
				SET DHIS2_DB_NAME=%%e
			)
		)

	GOTO :EOF

:execute_sql

ECHO %DHIS2_DB_USERNAME%
ECHO %DHIS2_DB_PASSWORD%
ECHO %DHIS2_HOST%
ECHO %DHIS2_DB_PORT%
ECHO %DHIS2_DB_NAME%

SET PSQL_EXE="%HMIS_HOME%\pgsql\bin\psql.exe" -t -U %DHIS2_DB_USERNAME% -d %DHIS2_DB_NAME% -p %DHIS2_DB_PORT%

:loop_sql

	:: Read common_function script
	%PSQL_EXE% -c "SELECT storagekey FROM fileresource WHERE name = '%COMMON_FUNCTIONS_SQL%'" > %temp%\fileKey.txt
	SET /p COMMON_FUNCTIONS_FILE= < %temp%\fileKey.txt
	DEL %temp%\fileKey.txt
	:: Trim file name
	FOR /f "tokens=* delims= " %%a IN ("%COMMON_FUNCTIONS_FILE%") DO SET COMMON_FUNCTIONS_FILE=%%a
	ECHO Common functions: %COMMON_FUNCTIONS_FILE%
	
	:: Read mental_health script
	%PSQL_EXE% -c "SELECT storagekey FROM fileresource WHERE name = '%MENTAL_HEALTH_SQL%'" > %temp%\fileKey.txt
	SET /p MENTAL_HEALTH_FILE= < %temp%\fileKey.txt
	DEL %temp%\fileKey.txt
	:: Trim file name
	FOR /f "tokens=* delims= " %%a IN ("%MENTAL_HEALTH_FILE%") DO SET MENTAL_HEALTH_FILE=%%a
	ECHO Mental health: %MENTAL_HEALTH_FILE%
	
	:: Read main script
	%PSQL_EXE% -c "SELECT storagekey FROM fileresource WHERE name = '%MAIN_SQL%'" > %temp%\fileKey.txt
	SET /p MAIN_FILE= < %temp%\fileKey.txt
	DEL %temp%\fileKey.txt
	:: Trim file name
	FOR /f "tokens=* delims= " %%a IN ("%MAIN_FILE%") DO SET MAIN_FILE=%%a
	ECHO Main: %MAIN_FILE%
	
	%PSQL_EXE% -f "%FILES_HOME%\%COMMON_FUNCTIONS_FILE%"
	%PSQL_EXE% -f "%FILES_HOME%\%MENTAL_HEALTH_FILE%"
	%PSQL_EXE% -f "%FILES_HOME%\%MAIN_FILE%"
	
	timeout /t 10 /nobreak
	
	GOTO loop_sql
	
endlocal