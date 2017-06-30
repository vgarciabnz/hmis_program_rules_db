
-- GENERAL CONSIDERATIONS

-- The value of 'lastupdated' property for new/updated datavalues must be the latest one of the dataelements involved in the calculation. The programstageinstance must be updated accordingly.


-----------------------------------------------------------------------------------------------------------------------------
-------------------------------------------- DROP FUNCTION ------------------------------------------------------------------
 DROP FUNCTION get_data_value_by_program_stages( _programinstanceid integer, _programstageuid text [], _dataelementcode text) ;
 DROP FUNCTION upsert_trackedentitydatavalue(integer, integer, character varying, character varying, timestamp without time zone, timestamp without time zone);
 DROP FUNCTION get_programinstance_modified_after(character varying, timestamp without time zone);
 DROP FUNCTION copy_datavalue_between_non_repeatable_stages(integer, character varying, character varying, character varying, character varying);
 DROP FUNCTION get_days_between_non_repeatable_stages(integer, character varying, character varying);
 DROP FUNCTION get_events_count(integer, character varying[]);
 DROP FUNCTION substract_datavalue_between_non_repeatable_stages(integer, character varying, character varying, character varying, character varying, character varying, character varying);
 DROP FUNCTION divide_datavalue_between_non_repeatable_stages(integer, character varying, character varying, character varying, character varying, character varying, character varying);
 DROP FUNCTION save_events_count (_pi_id INTEGER,ps_array VARCHAR(11)[],_de_target VARCHAR(50), _ps_target VARCHAR(11));

 
 -----------------------------------------------------------------------------------------------------------------------------
-------------------------------------------- FUNCTIONS ------------------------------------------------------------------

--- Obtain data value from a dataelement within a specific event


CREATE OR REPLACE FUNCTION get_data_value_by_program_stages(
    _pi_id integer,
    _ps_array text[],
    _de_code text)
  RETURNS SETOF trackedentitydatavalue AS
$BODY$
  BEGIN
    RETURN QUERY (SELECT tedv.* FROM trackedentitydatavalue tedv
      INNER JOIN programstageinstance psi ON tedv.programstageinstanceid = psi.programstageinstanceid
      WHERE psi.programinstanceid = _pi_id
      AND psi.programstageid IN (SELECT programstageid FROM programstage WHERE uid = any( _ps_array))
      AND dataelementid IN (SELECT dataelementid FROM dataelement WHERE code = _de_code));
  END;
  $BODY$
  LANGUAGE plpgsql;

-- Upsert trackedentitydatavalue
CREATE OR REPLACE FUNCTION upsert_trackedentitydatavalue (
 _psi_id integer,
 _de_id integer,
 _value varchar(50000),
 _storedby varchar(255),
 _created timestamp without time zone, 
 _lastupdated timestamp without time zone)

 RETURNS INTEGER AS 

$$

	DECLARE result integer;

	BEGIN
	IF EXISTS (SELECT 1 FROM trackedentitydatavalue
		WHERE dataelementid = _de_id
		AND programstageinstanceid = _psi_id)

	THEN
		-- If destination value exists, update
		UPDATE trackedentitydatavalue SET value = _value, lastupdated = _lastupdated
			WHERE dataelementid = _de_id
			AND programstageinstanceid = _psi_id;

		GET DIAGNOSTICS result = ROW_COUNT;
		RAISE NOTICE 'Updated % values for dataelement %', result, _de_id;
	ELSE
		-- If not exists, insert new value
		INSERT INTO trackedentitydatavalue VALUES (_psi_id,	_de_id,	_value,	false, _storedby, _created, _lastupdated);

		GET DIAGNOSTICS result = ROW_COUNT;
		RAISE NOTICE 'Inserted % values for dataelement %', result, _de_id;
	END IF;
	
	-- Update lastupdated record in the programstageinstance
	UPDATE programstageinstance SET lastupdated = GREATEST(lastupdated, _lastupdated) WHERE programstageinstanceid = _psi_id;

	RETURN result;
	END;

$$
LANGUAGE plpgsql;


-- Starting Point: get list of programinstanceid with any programstage modified after the timestamp provided
--
-- program: program uid
-- last_updated: timestamp


CREATE OR REPLACE FUNCTION get_programinstance_modified_after(
    _program_uid character varying,
    last_run timestamp without time zone)
	
  RETURNS SETOF integer AS

$$

	BEGIN 

		RETURN QUERY SELECT DISTINCT programinstance.programinstanceid from programinstance 

			inner join program on program.programid=programinstance.programid
				
			
			inner join programstageinstance on programinstance.programinstanceid = programstageinstance.programinstanceid 
			
			
			where programstageinstance.lastupdated > last_run and program.uid=_program_uid;
			

	END;

$$
  LANGUAGE plpgsql;
  
-- Given a specific programinstanceid, copy the value from one dataelement in a programstage into another dataelement in another programstage.
-- Check that both source and destination program stages exist. If any of them does not exist, do nothing.
--
-- _pi_id: programinstanceid
-- _de_src: dataelement code (source)
-- _ps_src: programstage uid (source)
-- _de_dst: dataelement code (destination)
-- _ps_dst: programstage uid (destination)



CREATE OR REPLACE FUNCTION copy_datavalue_between_non_repeatable_stages(
    _pi_id integer,
    _de_src character varying,
    _ps_src character varying,
    _de_dst character varying,
    _ps_dst character varying)
  RETURNS void AS
$BODY$
 
DECLARE event_src programstageinstance;
DECLARE event_dst programstageinstance;


DECLARE aux_datavalue trackedentitydatavalue;
	    
	BEGIN
	
		select programstageinstance.*  into event_src from  programstageinstance, programstage
			where programstage.programstageid=programstageinstance.programstageid 
			and programstage.uid=_ps_src and programstageinstance.programinstanceid=_pi_id and programstageinstance.deleted='f';

		select programstageinstance.* into event_dst from  programstageinstance, programstage
			where programstage.programstageid=programstageinstance.programstageid 
			and programstage.uid=_ps_dst and programstageinstance.programinstanceid=_pi_id and programstageinstance.deleted='f'; --deleted set to 'f' in order to avoid deleted records

		IF  event_src.programstageinstanceid is not null and event_dst.programstageinstanceid is not null 
		
		-- if both events exists search for the datavalue related to the source event (do nothing if one of them has been erased or not created yet
		
			THEN 
							
			aux_datavalue = get_data_value_by_program_stages(
			_pi_id,
			array [_ps_src],
			_de_src
			);

				IF aux_datavalue.value IS NOT NULL -- if there is a value for the source, perform an upsert 
					THEN
						PERFORM upsert_trackedentitydatavalue(
						event_dst.programstageinstanceid,
						(SELECT dataelementid FROM dataelement WHERE code = _de_dst),
						aux_datavalue.value,
						aux_datavalue.storedby,
						aux_datavalue.created,
						aux_datavalue.lastupdated);

					
					ELSE --if there is no value for the source, then delete the target value 			

						DELETE FROM trackedentitydatavalue where programstageinstanceid = event_dst.programstageinstanceid and dataelementid= (SELECT dataelementid FROM dataelement WHERE code = _de_dst);
						RAISE NOTICE 'Deleted  values for dataelement %',  _de_dst;
							
				END IF;
		
		END IF;
	END;
  $BODY$
  LANGUAGE plpgsql;


		
-- Given a specific programinstanceid, calculates the days between two non-repeatable events
--
-- _pi: programinstanceid
-- _ps_start: programstage uid (start)
-- _ps_end: programstage uid (end)

CREATE OR REPLACE FUNCTION get_days_between_non_repeatable_stages (
_pi_id INTEGER, 
_ps_start VARCHAR(11),
_ps_end VARCHAR(11)
) 
RETURNS value_with_date AS $$
	DECLARE start_event programstageinstance;
	DECLARE end_event programstageinstance;
	
	BEGIN
		select psi.* INTO start_event FROM programstageinstance psi INNER JOIN programstage ps on psi.programstageid = ps.programstageid 
			WHERE psi.programinstanceid = _pi_id AND ps.uid = _ps_start and psi.deleted='f';
		select psi.* INTO end_event FROM programstageinstance psi INNER JOIN programstage ps on psi.programstageid = ps.programstageid 
			WHERE psi.programinstanceid = _pi_id AND ps.uid = _ps_end and psi.deleted='f';
			
		IF (start_event.executiondate IS NOT NULL) AND (end_event.executiondate IS NOT NULL) -- if there is an executiondate defined for both events
			THEN
				RETURN ((end_event.executiondate::date - start_event.executiondate::date)::text, GREATEST(start_event.lastupdated, end_event.lastupdated)); --optain the difference between the two fields and the max lastupdated
			ELSE
				RETURN NULL;
		END IF;
		 
	END;
$$
LANGUAGE 'plpgsql';




-- Get the count of events. Get the number of events within one or more program stages
--
-- pi: programinstanceid
-- ps_array: array of programstage uids meaning that multiple programstage can be introduced

CREATE OR REPLACE FUNCTION get_events_count (
_pi_id INTEGER, 
ps_array VARCHAR(11)[])

 RETURNS value_with_date AS

$$

DECLARE result value_with_date;

	BEGIN

	select count(psi.*),max(psi.lastupdated) into result  from programstageinstance psi 
			inner join programstage ps on psi.programstageid=ps.programstageid
				where psi.programinstanceid=_pi_id
					and ps.uid = any(ps_array) and psi.deleted='f';

	RETURN result;

	

	END;
$$

LANGUAGE 'plpgsql';



--- Calculates the difference between datavalues of dataelement within different stages and save them into a third dataelement
-- Function: substract_datavalue_between_non_repeatable_stages(integer, character varying, character varying, character varying, character varying, character varying, character varying)

-- _pi_id: programinstanceid
-- _de_src1: dataelement code (one of the sources)
-- _ps_src1: programstage uid (one of the sources)
-- _de_src2: dataelement code (the other of the sources)
-- _ps_src2: programstage uid (the other of the sources)
-- _de_target:  dataelement code  (destination)
-- _ps_target: programstage uid (destination)

CREATE OR REPLACE FUNCTION substract_datavalue_between_non_repeatable_stages(
    _pi_id integer,
    _de_src1 character varying,
    _ps_src1 character varying,
    _de_src2 character varying,
    _ps_src2 character varying,
    _de_target character varying,
    _ps_target character varying)
  RETURNS void AS
$BODY$

DECLARE event_src1 programstageinstance;
DECLARE event_src2 programstageinstance;
DECLARE target_event_id integer;
DECLARE aux_datavalue_src1 trackedentitydatavalue;
DECLARE aux_datavalue_src2 trackedentitydatavalue;
DECLARE substract value_with_date;

		BEGIN 
		
		select psi.* into event_src1 from programstageinstance psi 			
			inner join programstage ps on psi.programstageid=ps.programstageid
				where psi.programinstanceid=_pi_id
					and ps.uid = _ps_src1 and psi.deleted='f';
					
		select psi.* into event_src2 from programstageinstance psi 			
			inner join programstage ps on psi.programstageid=ps.programstageid
				where psi.programinstanceid=_pi_id
					and ps.uid = _ps_src2 and psi.deleted='f';
				


			IF  event_src1.programstageinstanceid is not null and event_src2.programstageinstanceid is not null 
			
				-- if both events exists search for the datavalue related to both events in order to make the calculation (do nothing if one of them has been erased or not created yet)
			
				THEN 
								
				aux_datavalue_src1 = get_data_value_by_program_stages(
				_pi_id,
				array[_ps_src1],
				_de_src1
				);		
				
				aux_datavalue_src2= get_data_value_by_program_stages(
				_pi_id,
				array[_ps_src2],
				_de_src2
				);		
				
				SELECT programstageinstanceid INTO target_event_id FROM programstageinstance psi INNER JOIN programstage ps on psi.programstageid = ps.programstageid 
						WHERE psi.programinstanceid = _pi_id AND ps.uid = _ps_target;
				
				
					IF aux_datavalue_src1.value is not null and aux_datavalue_src2.value is not null -- if there is value for both datavalues within the events
					
						THEN
							
							substract= ((aux_datavalue_src1.value::numeric - aux_datavalue_src2.value::numeric)::text , greatest (aux_datavalue_src1.lastupdated,aux_datavalue_src2.lastupdated)); --calculate the difference and show the value from the biggest last updated field
							
												
							PERFORM upsert_trackedentitydatavalue(
							target_event_id,
							(SELECT dataelementid FROM dataelement WHERE code = _de_target),
							substract.val,
							'auto-generated',
							substract.lastupdated,
							substract.lastupdated);

					ELSE	--if one of the values hasn't been entered or has been erased, delete the value from the target event			
						
							DELETE FROM trackedentitydatavalue where programstageinstanceid = target_event_id and dataelementid= (SELECT dataelementid FROM dataelement WHERE code = _de_target);
					
					END IF;
					
				
			END IF ;
			
		END;
				
$BODY$
  LANGUAGE plpgsql;
  
  -------------------------------------------------------------

--- Calculates the division between datavalues of dataelement within different stages and save them into a third dataelement
-- Function: substract_datavalue_between_non_repeatable_stages(integer, character varying, character varying, character varying, character varying, character varying, character varying)

-- _pi_id: programinstanceid
-- _de_src1: dataelement code (numerator)
-- _ps_src1: programstage uid (numerator)
-- _de_src2: dataelement code (denominator)
-- _ps_src2: programstage uid (denominator)
-- _de_target:  dataelement code  (destination)
-- _ps_target: programstage uid (destination)

CREATE OR REPLACE FUNCTION divide_datavalue_between_non_repeatable_stages(
    _pi_id integer,
    _de_src1 character varying,
    _ps_src1 character varying,
    _de_src2 character varying,
    _ps_src2 character varying,
    _de_target character varying,
    _ps_target character varying)
  RETURNS void AS
$BODY$

DECLARE event_src1 programstageinstance;
DECLARE event_src2 programstageinstance;
DECLARE target_event_id integer;
DECLARE aux_datavalue_src1 trackedentitydatavalue;
DECLARE aux_datavalue_src2 trackedentitydatavalue;
DECLARE division value_with_date;

		BEGIN 
		
		select psi.* into event_src1 from programstageinstance psi 			
			inner join programstage ps on psi.programstageid=ps.programstageid
				where psi.programinstanceid=_pi_id
					and ps.uid = _ps_src1 and psi.deleted='f';
					
		select psi.* into event_src2 from programstageinstance psi 			
			inner join programstage ps on psi.programstageid=ps.programstageid
				where psi.programinstanceid=_pi_id
					and ps.uid = _ps_src2 and psi.deleted='f';
				


			IF  event_src1.programstageinstanceid is not null and event_src2.programstageinstanceid is not null 
					
				-- if both events exists search for the datavalue related to both events in order to make the calculation (do nothing if one of them has been erased or not created yet)
			
				THEN 
								
				SELECT * INTO aux_datavalue_src1 FROM get_data_value_by_program_stages(_pi_id, array[_ps_src1],	_de_src1);		
				SELECT * INTO aux_datavalue_src2 FROM get_data_value_by_program_stages(_pi_id, array[_ps_src2], _de_src2);
				
				SELECT programstageinstanceid INTO target_event_id FROM programstageinstance psi INNER JOIN programstage ps on psi.programstageid = ps.programstageid 
						WHERE psi.programinstanceid = _pi_id AND ps.uid = _ps_target; -- if there is value for both datavalues within the events
				
				
					IF aux_datavalue_src1.value is not null and aux_datavalue_src2.value is not null 
					
						THEN
							
							IF aux_datavalue_src2.value::integer <> 0
							
								THEN	
									division= ((aux_datavalue_src1.value::numeric / aux_datavalue_src2.value::numeric)::text , greatest (aux_datavalue_src1.lastupdated,aux_datavalue_src2.lastupdated));
									
														
									
								ELSE -- if denominator is 0, set divide as 0
									division= ('0',greatest (aux_datavalue_src1.lastupdated,aux_datavalue_src2.lastupdated));

								
							END IF;

						PERFORM upsert_trackedentitydatavalue(
							target_event_id,
							(SELECT dataelementid FROM dataelement WHERE code = _de_target),
							division.val,
							'auto-generated',
							division.lastupdated,
							division.lastupdated);
						
						ELSE	--if one of the values hasn't been entered or has been erased, delete the value from the target event		
						
							DELETE FROM trackedentitydatavalue where programstageinstanceid = target_event_id and dataelementid= (SELECT dataelementid FROM dataelement WHERE code = _de_target);
					
					END IF;
					
				
			END IF ;
			
		END;
				
$BODY$
  LANGUAGE plpgsql ;
  
  
-- Count the number of events within one or more program stages and save the value
--  _pi_id: programinstanceid
-- ps_array: array of programstage uids
-- _de_target:  dataelement code  (destination)
-- _ps_target: programstage uid (destination)


 ------------------------------------


 CREATE OR REPLACE FUNCTION save_events_count (_pi_id INTEGER,ps_array VARCHAR(11)[],_de_target VARCHAR(50), _ps_target VARCHAR(11)) RETURNS void as
 
 $$
 

	DECLARE count_with_date value_with_date;
	DECLARE target_event_id INTEGER;
	
	BEGIN
		SELECT * INTO count_with_date FROM get_events_count (_pi_id, ps_array);
		
		SELECT programstageinstanceid INTO target_event_id FROM programstageinstance psi INNER JOIN programstage ps on psi.programstageid = ps.programstageid 
				WHERE psi.programinstanceid = _pi_id AND ps.uid = _ps_target;
		
		IF (count_with_date.val IS NOT NULL) AND (target_event_id IS NOT NULL)
		THEN			
			
				
			PERFORM upsert_trackedentitydatavalue(
				target_event_id,
				(SELECT dataelementid FROM dataelement WHERE code = _de_target),
				count_with_date.val,
				'auto-generated',
				count_with_date.lastupdated,
				count_with_date.lastupdated
			);
		END IF;
	END;
$$
LANGUAGE plpgsql;


-- Put a number to each event belonging to a program stage. Events are ordered by 'executiondate' followed by 'created'. The rank is saved in the dataelement provided.
-- It is necessary to sum an index to each program lastupdate date: if they are exactly the same, the system has an strange behaviour.
--
-- _pi_id: programinstanceid
-- _ps_src: programstage uid
-- _de_dst: dataelement code (destination). The event rank will be saved in this dataelement.

CREATE OR REPLACE FUNCTION number_events_in_repeatable_program_stage (_pi_id integer, _ps_src character varying, _de_dst character varying) RETURNS void AS $$

	DECLARE event_id_with_rank RECORD;

	BEGIN
		FOR event_id_with_rank IN (
			SELECT psi.programstageinstanceid, rank() OVER (ORDER BY psi.executiondate, psi.created), MAX(psi.lastupdated) OVER () AS lastupdated FROM programstageinstance psi 
				INNER JOIN programstage ps ON psi.programstageid = ps.programstageid
				WHERE ps.uid = _ps_src
				AND psi.programinstanceid = _pi_id
				AND psi.deleted = false
		)
		
		LOOP
			PERFORM upsert_trackedentitydatavalue(
				event_id_with_rank.programstageinstanceid,
				(SELECT dataelementid FROM dataelement WHERE code = _de_dst),
				event_id_with_rank.rank::text,
				'auto-generated',
				event_id_with_rank.lastupdated + event_id_with_rank.rank * interval '1 milliseconds',
				event_id_with_rank.lastupdated + event_id_with_rank.rank * interval '1 milliseconds');
		END LOOP;
	END;
$$
LANGUAGE plpgsql;


