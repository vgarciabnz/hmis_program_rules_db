@echo off && setlocal
	
SET HMIS_HOME=C:\Program Files (x86)\DHIS2
SET DHIS2_HOME=%HMIS_HOME%\DHIS
SET FILES_HOME=%DHIS2_HOME%\files
SET PGPASSFILE=%HMIS_HOME%\pgpass.conf

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
			:: Let's suppose the url has a syntax like "jdbc:postgresql://localhost:5544/dhis2"
			FOR /F "tokens=1,2,3,4,5 delims=/:" %%a in ("%2%") do (
				SET DHIS2_HOST=%%c
				SET DHIS2_DB_PORT=%%d
				SET DHIS2_DB_NAME=%%e
			)
		)

	GOTO :EOF

:execute_sql

::ECHO %DHIS2_DB_USERNAME%
::ECHO %DHIS2_DB_PASSWORD%
::ECHO %DHIS2_HOST%
::ECHO %DHIS2_DB_PORT%
::ECHO %DHIS2_DB_NAME%

SET PSQL_EXE="%HMIS_HOME%\pgsql\bin\psql.exe" -qAtX -U %DHIS2_DB_USERNAME% -d %DHIS2_DB_NAME% -p %DHIS2_DB_PORT% -w

:loop_sql

	:: TODO Manage unmodified files. Do not generate and execute files if not modified. Execute only 'main.sql'

	:: Read files from constant description
	%PSQL_EXE% -c "SELECT description FROM constant WHERE name = '%COMMON_FUNCTIONS_SQL%'" > "%FILES_HOME%\%COMMON_FUNCTIONS_SQL%"
	%PSQL_EXE% -c "SELECT description FROM constant WHERE name = '%MENTAL_HEALTH_SQL%'" > "%FILES_HOME%\%MENTAL_HEALTH_SQL%"
	%PSQL_EXE% -c "SELECT description FROM constant WHERE name = '%MAIN_SQL%'" > "%FILES_HOME%\%MAIN_SQL%"

	%PSQL_EXE% -f "%FILES_HOME%\%COMMON_FUNCTIONS_SQL%"
	%PSQL_EXE% -f "%FILES_HOME%\%MENTAL_HEALTH_SQL%"
	%PSQL_EXE% -f "%FILES_HOME%\%MAIN_SQL%"
	
	DEL "%FILES_HOME%\%COMMON_FUNCTIONS_SQL%"
	DEL "%FILES_HOME%\%MENTAL_HEALTH_SQL%"
	DEL "%FILES_HOME%\%MAIN_SQL%"
	
	timeout /t 10 /nobreak
	
	GOTO loop_sql
	
endlocal