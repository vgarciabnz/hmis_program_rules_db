
-- MENTAL HEALTH

-- Mental Health - Individual: ORvg6A5ed7z
-- Mental Health - Group: NQF5RvfjwaX

-- First visit: bgq04wsYMp7
-- Consultation: tmsr4EJaSPz
-- Closure: XuThsezwYbZ

-- Diagnosis: TK_MH5
-- Main category events: TK_MH6
-- Main category symptoms: TK_MH1
-- Length of interventions: TK_MH24
-- Severity of symptoms: TK_MH13
-- Severity of symptoms variation: TK_MH38
-- Functioning reduction variation: TK_MH39
-- Number of face-to-face consultations: TK_MH75
-- Number of remote consultations : TK_MH74
-- Session mode: TK_MH54
-- Patient took psychiatry treatment: TK_MH72
-- Number of followups: TK_MH52
/* 
--DROPS-----------

DROP FUNCTION mh_save_length_of_intervention (pi_id INTEGER);
DROP FUNCTION mh_save_number_consultations_by_type( _pi_id integer, _value text, _de_target VARCHAR(50), _ps_target VARCHAR(11));
DROP FUNCTION mh_save_session_mode ( _pi_id integer,_de_target VARCHAR(50), _ps_target VARCHAR(11));
DROP FUNCTION mh_save_patient_under_psycotropics( _pi_id integer, _de_target VARCHAR(50), _ps_target VARCHAR(11));
DROP FUNCTION mh_save_patient_referred ( _pi_id integer, _de_target VARCHAR(50), _ps_target VARCHAR(11)) ;
DROP FUNCTION mh_execute_mental_health_individual (last_updated timestamp without time zone);

-----


 */
-----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION mh_save_length_of_intervention (pi_id INTEGER) RETURNS void AS $$

	DECLARE lenght_with_date value_with_date;
	DECLARE target_event_id INTEGER;
	
	BEGIN
		SELECT * INTO lenght_with_date FROM get_days_between_non_repeatable_stages (pi_id, 'bgq04wsYMp7', 'XuThsezwYbZ');
		
		SELECT programstageinstanceid INTO target_event_id FROM programstageinstance psi INNER JOIN programstage ps on psi.programstageid = ps.programstageid 
				WHERE psi.programinstanceid = pi_id AND ps.uid = 'XuThsezwYbZ';
				
		IF (lenght_with_date.val IS NOT NULL) AND (target_event_id IS NOT NULL)
		THEN
			PERFORM upsert_trackedentitydatavalue(
				target_event_id,
				(SELECT dataelementid FROM dataelement WHERE code = 'TK_MH24'),
				lenght_with_date.val,
				'auto-generated',
				lenght_with_date.lastupdated,
				lenght_with_date.lastupdated
			);
		END IF;
	END;
$$
LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION mh_save_number_consultations_by_type( _pi_id integer, _value text, _de_target VARCHAR(50), _ps_target VARCHAR(11)) RETURNS void
AS $$


DECLARE number_with_date value_with_date;
DECLARE target_event_id integer;


  BEGIN
	SELECT count(1),max(lastupdated) into number_with_date from get_data_value_by_program_stages(_pi_id,array['bgq04wsYMp7','tmsr4EJaSPz'], 'TK_MH53') where value=_value;
	
	SELECT programstageinstanceid INTO target_event_id FROM programstageinstance psi INNER JOIN programstage ps on psi.programstageid = ps.programstageid 
			WHERE psi.programinstanceid = _pi_id AND ps.uid = _ps_target;
	
	IF (number_with_date.val IS NOT NULL) AND (target_event_id IS NOT NULL) 
		THEN
		
		PERFORM upsert_trackedentitydatavalue(
			target_event_id,
			(SELECT dataelementid FROM dataelement WHERE code = _de_target),
			number_with_date.val,
			'auto-generated',
			number_with_date.lastupdated,
			number_with_date.lastupdated
		);
	END IF;
	
  END;
  $$
  LANGUAGE 'plpgsql';

  ------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mh_save_session_mode ( _pi_id integer,_de_target VARCHAR(50), _ps_target VARCHAR(11)) RETURNS void AS

$$
DECLARE session_mode value_with_date;
DECLARE f2f_values trackedentitydatavalue;
DECLARE remote_values trackedentitydatavalue;
DECLARE target_event_id integer;

BEGIN 
		SELECT * into f2f_values from get_data_value_by_program_stages(_pi_id,array['XuThsezwYbZ'], 'TK_MH75');
		SELECT * into remote_Values from get_data_value_by_program_stages(_pi_id,array['XuThsezwYbZ'], 'TK_MH74');
		
	IF f2f_values.value <>'0' and remote_values.value <>'0' THEN session_mode = ('2',greatest(f2f_values.lastupdated,remote_values.lastupdated));
	ELSEIF f2f_values.value <>'0' and remote_values.value ='0' THEN session_mode = ('1',greatest(f2f_values.lastupdated,remote_values.lastupdated));
	ELSEIF f2f_values.value='0' and remote_values.value<>'0' THEN session_mode = ('3',greatest(f2f_values.lastupdated,remote_values.lastupdated));

	END IF;
	
	SELECT programstageinstanceid INTO target_event_id FROM programstageinstance psi INNER JOIN programstage ps on psi.programstageid = ps.programstageid 
			WHERE psi.programinstanceid = _pi_id AND ps.uid = _ps_target;
			
	IF (session_mode.val IS NOT NULL) AND (target_event_id IS NOT NULL)
		THEN
		
		PERFORM upsert_trackedentitydatavalue(
				target_event_id,
				(SELECT dataelementid FROM dataelement WHERE code = _de_target),
				session_mode.val,
				'auto-generated',
				session_mode.lastupdated,
				session_mode.lastupdated
			);
	END IF;
END;
$$
  LANGUAGE 'plpgsql';
  
  
-----------------------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION mh_save_patient_under_psycotropics( _pi_id integer, _de_target VARCHAR(50), _ps_target VARCHAR(11)) RETURNS void
AS $$


DECLARE number_with_date value_with_date;
DECLARE target_event_id integer;


  BEGIN
	SELECT count(1),max(lastupdated) into number_with_date from get_data_value_by_program_stages(_pi_id,array['bgq04wsYMp7','tmsr4EJaSPz'], 'TK_MH17') where value='true';
	
	
	IF number_with_date.val is not null 
		THEN
		
		SELECT programstageinstanceid INTO target_event_id FROM programstageinstance psi INNER JOIN programstage ps on psi.programstageid = ps.programstageid 
			WHERE psi.programinstanceid = _pi_id AND ps.uid = _ps_target;
			
		IF (target_event_id IS NOT NULL)
			THEN
			
			IF number_with_date.val <> '0'
			
				THEN
					
				PERFORM upsert_trackedentitydatavalue(
					target_event_id,
					(SELECT dataelementid FROM dataelement WHERE code = _de_target),
					'true',
					'auto-generated',
					number_with_date.lastupdated,
					number_with_date.lastupdated
				);
			
			ELSE 
			
				PERFORM upsert_trackedentitydatavalue(
					target_event_id,
					(SELECT dataelementid FROM dataelement WHERE code = _de_target),
					'false',
					'auto-generated',
					number_with_date.lastupdated,
					number_with_date.lastupdated
				);
			
			END IF;
		END IF;
	END IF;
	
  END;
  $$
  LANGUAGE 'plpgsql';

 ------------------------------------------------------------

CREATE OR REPLACE FUNCTION mh_save_patient_referred ( _pi_id integer, _de_target VARCHAR(50), _ps_target VARCHAR(11)) RETURNS void
AS $$

DECLARE number_with_date value_with_date;
DECLARE target_event_id integer;

BEGIN
	SELECT count(1),max(lastupdated) into number_with_date from get_data_value_by_program_stages(_pi_id,array['bgq04wsYMp7','tmsr4EJaSPz'], 'TK_MH61');
	
	SELECT programstageinstanceid INTO target_event_id FROM programstageinstance psi INNER JOIN programstage ps on psi.programstageid = ps.programstageid 
			WHERE psi.programinstanceid = _pi_id AND ps.uid = _ps_target;
	
	IF target_event_id IS NOT NULL 

		THEN

		RAISE NOTICE 'value %', number_with_date.val;
		
		IF number_with_date.val <> '0'
			THEN
			PERFORM upsert_trackedentitydatavalue(
				target_event_id,
				(SELECT dataelementid FROM dataelement WHERE code = _de_target),
				'true',
				'auto-generated',
				number_with_date.lastupdated,
				number_with_date.lastupdated
			);

			ELSE 
					
			PERFORM upsert_trackedentitydatavalue(
				target_event_id,
				(SELECT dataelementid FROM dataelement WHERE code = _de_target),
				'false',
				'auto-generated',
				number_with_date.lastupdated,
				number_with_date.lastupdated
			);

		
		END IF;
	END IF;
	
END;
$$
LANGUAGE 'plpgsql';




-------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION execute_mental_health_individual (last_updated timestamp without time zone)
  RETURNS SETOF integer AS
$$

	DECLARE program_instance_id INTEGER;
	
	BEGIN
		FOR program_instance_id in (SELECT * FROM get_programinstance_modified_after('ORvg6A5ed7z', last_updated))
		LOOP
			-- Copy Diagnosis, Main category events, Main category symptoms
			PERFORM copy_datavalue_between_non_repeatable_stages (program_instance_id, 'TK_MH5', 'bgq04wsYMp7', 'TK_MH5', 'XuThsezwYbZ'); --Diagnosis
			PERFORM copy_datavalue_between_non_repeatable_stages (program_instance_id, 'TK_MH6', 'bgq04wsYMp7', 'TK_MH6', 'XuThsezwYbZ'); -- Events
			PERFORM copy_datavalue_between_non_repeatable_stages (program_instance_id, 'TK_MH1', 'bgq04wsYMp7', 'TK_MH1', 'XuThsezwYbZ'); --Symptoms
			-- Save length of intervention
			PERFORM mh_save_length_of_intervention (program_instance_id);
			
			-- Severity of symptoms and function variation
			PERFORM substract_datavalue_between_non_repeatable_stages (program_instance_id,'TK_MH13','bgq04wsYMp7','TK_MH13','XuThsezwYbZ','TK_MH38','XuThsezwYbZ');
			PERFORM substract_datavalue_between_non_repeatable_stages (program_instance_id,'TK_MH14','bgq04wsYMp7','TK_MH14','XuThsezwYbZ','TK_MH39','XuThsezwYbZ');
				
			-- number of consultations
			PERFORM save_events_count(program_instance_id,array['bgq04wsYMp7','tmsr4EJaSPz'],'TK_MH58','XuThsezwYbZ');
			-- number of followups
			PERFORM save_events_count(program_instance_id,array['tmsr4EJaSPz'],'TK_MH52','XuThsezwYbZ');
			
			--average time between sessions
			PERFORM divide_datavalue_between_non_repeatable_stages  (program_instance_id,'TK_MH24','XuThsezwYbZ','TK_MH52','XuThsezwYbZ','TK_MH23','XuThsezwYbZ');
			
			-- number of face-to-face consultations
			
			PERFORM mh_save_number_consultations_by_type (program_instance_id,'true','TK_MH75','XuThsezwYbZ');
			
			-- number of remote consultations 
			
			PERFORM mh_save_number_consultations_by_type (program_instance_id,'false','TK_MH74','XuThsezwYbZ');
			
			--Session mode
			
			PERFORM mh_save_session_mode ( program_instance_id,'TK_MH54','XuThsezwYbZ'); 
			
			-- Patient took psychiatry treatment at least once		
			
			PERFORM mh_save_patient_under_psycotropics( program_instance_id, 'TK_MH72', 'XuThsezwYbZ');
			
			-- Patient referred 
			PERFORM mh_save_patient_referred ( program_instance_id, 'TK_MH73',  'XuThsezwYbZ');
			
			
			
			RETURN QUERY SELECT program_instance_id;
		END LOOP;
	
	END;
$$
  LANGUAGE plpgsql;

