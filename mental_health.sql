
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
-- Consultation number: TK_MH76
-- Type of individual intervention: TK_MH11
-- Type of consultation: TK_MH10
-- Severity of symptoms: TK_MH13
-- Functioning reduction: TK_MH14
-- Complementary service - Medical care: TK_MH60
-- Complementary service - Psychiatric care: TK_MH61
-- Complementary service - Social service: TK_MH63
-- Complementary service - Legal service: TK_MH64
-- Complementary service - Other: TK_MH65
-- New beneficiaries: TK_MH67
-- Total beneficiaries: TK_MH68

/* 
--DROPS-----------

DROP FUNCTION mh_save_length_of_intervention (pi_id INTEGER,_ps_start VARCHAR(11),_ps_end VARCHAR(11), _ps_target VARCHAR(11) )  ;
DROP FUNCTION mh_save_number_consultations_by_type( _pi_id integer, _value text, _de_target VARCHAR(50), _ps_target VARCHAR(11));
DROP FUNCTION mh_save_session_mode ( _pi_id integer,_de_target VARCHAR(50), _ps_target VARCHAR(11));
DROP FUNCTION mh_save_patient_under_psycotropics( _pi_id integer, _de_target VARCHAR(50), _ps_target VARCHAR(11));
DROP FUNCTION mh_save_patient_referred ( _pi_id integer, _de_target VARCHAR(50), _ps_target VARCHAR(11)) ;
DROP FUNCTION mh_save_followup_count (_pi_id INTEGER, _ps_target varchar (11));
DROP FUNCTION mh_save_total_beneficiaries (_pi_id INTEGER, _de_target VARCHAR(50), _ps_target VARCHAR(11));
DROP FUNCTION execute_mental_health_individual (last_updated timestamp without time zone);

-----


 */
-----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION mh_save_length_of_intervention (pi_id INTEGER,_ps_start VARCHAR(11),_ps_end VARCHAR(11), _ps_target VARCHAR(11) )  RETURNS void AS $$

	DECLARE lenght_with_date value_with_date;
	DECLARE target_event_id INTEGER;
	
	BEGIN
		SELECT * INTO lenght_with_date FROM get_days_between_non_repeatable_stages (pi_id, _ps_start, _ps_end);
		
		SELECT programstageinstanceid INTO target_event_id FROM get_programstageinstance (pi_id,_ps_target);
				
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
	SELECT count(1),max(lastupdated) into number_with_date from get_data_value_by_program_stages(_pi_id,array['tmsr4EJaSPz'], 'TK_MH53') where value=_value;
	
	SELECT programstageinstanceid INTO target_event_id FROM get_programstageinstance (_pi_id,_ps_target);
	
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
	
	SELECT programstageinstanceid INTO target_event_id FROM get_programstageinstance (_pi_id,_ps_target);
			
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
		ELSE IF session_mode.val IS NULL THEN 
		
			DELETE FROM trackedentitydatavalue where programstageinstanceid = target_event_id and dataelementid= (SELECT dataelementid FROM dataelement WHERE code = _de_target);
		END IF;
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
	SELECT count(1),max(lastupdated) into number_with_date from get_data_value_by_program_stages(_pi_id,array['tmsr4EJaSPz'], 'TK_MH17') where value='true';
	
	
	IF number_with_date.val is not null 
		THEN
		
		SELECT programstageinstanceid INTO target_event_id FROM get_programstageinstance (_pi_id,_ps_target);
			
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
	SELECT count(1),max(lastupdated) into number_with_date from get_data_value_by_program_stages(_pi_id,array['tmsr4EJaSPz'], 'TK_MH61');
	
	SELECT programstageinstanceid INTO target_event_id FROM get_programstageinstance (_pi_id,_ps_target);
	
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


-----------------------------------------------------------------------------------------------

 CREATE OR REPLACE FUNCTION mh_save_followup_count (_pi_id INTEGER, _ps_target varchar (11)) RETURNS void as
 
 $$
 

	DECLARE count_with_date value_with_date;
	DECLARE target_event_id INTEGER;
	
	BEGIN
		SELECT greatest(value::integer - 1,0)::text,lastupdated INTO count_with_date FROM get_data_value_of_first_event (_pi_id, 'XuThsezwYbZ','TK_MH58');
		
		SELECT programstageinstanceid INTO target_event_id FROM get_programstageinstance (_pi_id, _ps_target);
		
		IF (count_with_date.val IS NOT NULL) AND (target_event_id IS NOT NULL)
		THEN			

			RAISE NOTICE 'value %', count_with_date.val;	
				
			PERFORM upsert_trackedentitydatavalue(
				target_event_id,
				(SELECT dataelementid FROM dataelement WHERE code = 'TK_MH52'),
				count_with_date.val,
				'auto-generated',
				count_with_date.lastupdated,
				count_with_date.lastupdated
			);
		END IF;
	END;
$$
LANGUAGE plpgsql;

------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mh_save_patient_referred_MSF ( _pi_id integer, _de_target VARCHAR(50), _ps_target VARCHAR(11)) RETURNS void
AS $$

DECLARE number_with_date value_with_date;
DECLARE target_event_id integer;

BEGIN
	SELECT count(1),max(lastupdated) into number_with_date from get_data_value_by_program_stages(_pi_id,array['tmsr4EJaSPz'], 'TK_MH61') where value='2.1';
	
	SELECT programstageinstanceid INTO target_event_id FROM get_programstageinstance (_pi_id,_ps_target);
	
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

CREATE OR REPLACE FUNCTION mh_save_total_beneficiaries (_pi_id INTEGER, _de_target VARCHAR(50), _ps_target VARCHAR(11)) RETURNS void AS $$

	DECLARE dst_event programstageinstance;
	DECLARE total value_with_date;
	
	BEGIN
		SELECT * INTO dst_event FROM get_programstageinstance (_pi_id, _ps_target);
		
		IF dst_event.programstageinstanceid IS NOT NULL
		THEN
			SELECT * INTO total FROM get_datavalue_addition_in_repeatable_stage (_pi_id, 'TK_MH67', 'tmsr4EJaSPz');
			
			IF total.val IS NULL
			THEN
				-- If datavalue "New beneficiaries" has no value, it means that type of consultation is individual. Place a '1' in that case.
				SELECT '1', MAX(lastupdated) + interval '100 milliseconds' INTO total FROM get_programstageinstance (_pi_id, 'tmsr4EJaSPz');
			END IF;
				
			PERFORM upsert_trackedentitydatavalue(
				dst_event.programstageinstanceid,
				(SELECT dataelementid FROM dataelement WHERE code = _de_target),
				total.val,
				'auto-generated',
				total.lastupdated,
				total.lastupdated);
								
		END IF;		
	END;
$$
LANGUAGE plpgsql;


-------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION execute_mental_health_individual (last_updated timestamp without time zone)
  RETURNS SETOF integer AS
$$

	DECLARE program_instance_id INTEGER;
	
	BEGIN
		FOR program_instance_id in (SELECT * FROM get_programinstance_modified_after('ORvg6A5ed7z', last_updated))
		LOOP
			-- Copy Diagnosis, Main category events, Main category symptoms
			PERFORM copy_datavalue_between_non_repeatable_stages (program_instance_id, 'TK_MH5', 'bgq04wsYMp7', 'TK_MH5', 'XuThsezwYbZ'); --Diagnosis (from first consultation to closure)
			PERFORM copy_datavalue_between_non_repeatable_stages (program_instance_id, 'TK_MH6', 'bgq04wsYMp7', 'TK_MH6', 'XuThsezwYbZ'); -- Events (from first consultation to closure)
			PERFORM copy_datavalue_between_non_repeatable_stages (program_instance_id, 'TK_MH1', 'bgq04wsYMp7', 'TK_MH1', 'XuThsezwYbZ'); --Symptoms (from first consultation to closure)
			PERFORM copy_datavalue_between_non_repeatable_stages (program_instance_id, 'TK_MH11', 'tmsr4EJaSPz', 'TK_MH11', 'bgq04wsYMp7'); -- Type of individual intervention (from first consultation to admission)
			PERFORM copy_datavalue_between_non_repeatable_stages (program_instance_id, 'TK_MH10', 'tmsr4EJaSPz', 'TK_MH10', 'bgq04wsYMp7'); -- Type of consultation (from first consultation to admission)
			PERFORM copy_datavalue_between_non_repeatable_stages (program_instance_id, 'TK_MH13', 'tmsr4EJaSPz', 'TK_MH13', 'bgq04wsYMp7'); -- Severity of symptoms (from first consultation to admission)
			PERFORM copy_datavalue_between_non_repeatable_stages (program_instance_id, 'TK_MH14', 'tmsr4EJaSPz', 'TK_MH14', 'bgq04wsYMp7'); -- Functioning reduction (from first consultation to admission)
			PERFORM copy_datavalue_between_non_repeatable_stages (program_instance_id, 'TK_MH61', 'tmsr4EJaSPz', 'TK_MH61', 'bgq04wsYMp7'); -- Complementary service - Psychiatric  care (from first consultation to admission)
			
			-- Save length of intervention
			PERFORM mh_save_length_of_intervention (program_instance_id,'bgq04wsYMp7','XuThsezwYbZ','XuThsezwYbZ'); -- program instance, program stage start date, ps end date, ps target
			
			-- Severity of symptoms and function variation
			PERFORM substract_datavalue_between_non_repeatable_stages (program_instance_id,'TK_MH13','tmsr4EJaSPz','TK_MH13','XuThsezwYbZ','TK_MH38','XuThsezwYbZ'); -- pi, dataelement1, ps1, dt2,ps2, dt target, ps target
			PERFORM substract_datavalue_between_non_repeatable_stages (program_instance_id,'TK_MH14','tmsr4EJaSPz','TK_MH14','XuThsezwYbZ','TK_MH39','XuThsezwYbZ');
				
			-- number of consultations
			PERFORM save_events_count(program_instance_id,array['tmsr4EJaSPz'],'TK_MH58','XuThsezwYbZ');
			-- number of followups
			PERFORM mh_save_followup_count (program_instance_id,'XuThsezwYbZ');
			
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
			
			-- Save consultation number in each consultation (consultation index: 1st, 2nd, 3rd, etc)
			PERFORM number_events_in_repeatable_program_stage ( program_instance_id, 'tmsr4EJaSPz', 'TK_MH76');
			
			-- Patient referred MSF
			PERFORM mh_save_patient_referred_MSF ( program_instance_id, 'TK_MH77',  'XuThsezwYbZ');
			
			-- Total number of beneficiaries
			PERFORM mh_save_total_beneficiaries ( program_instance_id, 'TK_MH68', 'XuThsezwYbZ');
			
			
			RETURN QUERY SELECT program_instance_id;
		END LOOP;
	
	END;
$$
  LANGUAGE plpgsql;

