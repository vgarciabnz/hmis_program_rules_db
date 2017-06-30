
DO $$
	DECLARE _now TIMESTAMP WITHOUT TIME ZONE;
	DECLARE _lastexecution TIMESTAMP WITHOUT TIME ZONE;
	
	BEGIN
		-- Save execution start timestamp
		SELECT LOCALTIMESTAMP INTO _now;
		-- Create auxiliar structure if not exists
		CREATE SCHEMA IF NOT EXISTS hmisocba;
		CREATE TABLE IF NOT EXISTS hmisocba.programrule_executions (id SERIAL, executiondate TIMESTAMP WITHOUT TIME ZONE);
		
		-- Look for lastexecution value. If null, default to a very old time.
		SELECT COALESCE(MAX(executiondate), '2000-01-01 00:00:00') INTO _lastexecution FROM hmisocba.programrule_executions;

		-- Execute program rules
		
		PERFORM execute_mental_health_individual (_lastexecution);

		IF FOUND THEN 
		-- Insert new record into the log table when a program instance has been modified
			INSERT INTO hmisocba.programrule_executions (executiondate) VALUES (_now);
		END IF;
	END;
$$
LANGUAGE plpgsql;
