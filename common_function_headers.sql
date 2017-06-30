
-- GENERAL CONSIDERATIONS

-- The value of 'lastupdated' property for new/updated datavalues must be the latest one of the dataelements involved in the calculation. The programstageinstance must be updated accordingly.

-- CUSTOM TYPES
CREATE TYPE value_with_date as (val TEXT, lastupdated TIMESTAMP WITHOUT TIME ZONE);


-- Upsert trackedentitydatavalue
FUNCTION upsert_trackedentitydatavalue (_psi_id integer, _de_id integer, _value varchar(50000), _storedby varchar(255), _created timestamp without time zone, _lastupdated timestamp without time zone) RETURNS INTEGER



-- Starting Point: get list of programinstanceid with any programstage modified after the timestamp provided
--
-- _program: program uid
-- _lastupdated: timestamp

FUNCTION get_programinstance_modified_after (_program VARCHAR(11), _lastupdated TIMESTAMP) RETURNS SETOF INTEGER



-- Given a specific programinstanceid, copy the value from one dataelement in a programstage into another dataelement in another programstage.
-- Check that both source and destination program stages exist. If any of them does not exist, do nothing.
--
-- _pi_id: programinstanceid
-- _de_src: dataelement code (source)
-- _ps_src: programstage uid (source)
-- _de_dst: dataelement code (destination)
-- _ps_dst: programstage uid (destination)

FUNCTION copy_datavalue_between_non_repeatable_stages (_pi_id INTEGER, de_src VARCHAR(50), ps_src VARCHAR(11), de_dst VARCHAR(50), ps_dst VARCHAR(11)) RETURNS void



-- Given a specific programinstanceid, calculates the days between two non-repeatable events
--
-- _pi_id: programinstanceid
-- _ps_start: programstage uid (start)
-- _ps_end: programstage uid (end)

FUNCTION get_days_between_non_repeatable_stages (_pi_id INTEGER, _ps_start VARCHAR(11), _ps_end VARCHAR(11)) RETURNS value_with_date



-- Get the count of events.
--
-- _pi_id: programinstanceid
-- _ps_array: array of programstage uids

FUNCTION get_events_count (_pi_id INTEGER, _ps_array VARCHAR(11)[]) RETURNS value_with_date



-- Get set of trackedentitydatavales of a dataelement filtered by an array of program stages uids.
--
-- _pi_id: programinstanceid
-- _ps_array: array of programstage uids
-- _de_code: dataelement code

FUNCTION get_data_value_by_program_stages ( _pi_id integer, _ps_array text[], _de_code text) RETURNS SETOF trackedentitydatavalue


--- Calculates the difference between datavalues of dataelement within different stages and save them into a third dataelement
-- Function: substract_datavalue_between_non_repeatable_stages(integer, character varying, character varying, character varying, character varying, character varying, character varying)
-
-- _pi_id: programinstanceid
-- _de_src1: dataelement code (one of the sources)
-- _ps_src1: programstage uid (one of the sources)
-- _de_src2: dataelement code (the other of the sources)
-- _ps_src2: programstage uid (the other of the sources)
-- _de_target:  dataelement code  (destination)
-- _ps_target: programstage uid (destination)

CREATE OR REPLACE FUNCTION substract_datavalue_between_non_repeatable_stages(_pi_id integer,_de_src1 character varying,_ps_src1 character varying,_de_src2 character varying,_ps_src2 character varying,_de_target character varying,_ps_target character varying)  
  RETURNS void AS
  
    -------------------------------------------------------------

--- Calculates the division between datavalues of dataelement within different stages and save them into a third dataelement
-- Function: substract_datavalue_between_non_repeatable_stages(integer, character varying, character varying, character varying, character varying, character varying, character varying)
-
-- _pi_id: programinstanceid
-- _de_src1: dataelement code (numerator)
-- _ps_src1: programstage uid (numerator)
-- _de_src2: dataelement code (denominator)
-- _ps_src2: programstage uid (denominator)
-- _de_target:  dataelement code  (destination)
-- _ps_target: programstage uid (destination)



CREATE OR REPLACE FUNCTION divide_datavalue_between_non_repeatable_stages (_pi_id INTEGER, _de_src1 VARCHAR(50), _ps_src1 VARCHAR(11), _de_src2 VARCHAR(50), _ps_src2 VARCHAR(11),_de_target VARCHAR(50), _ps_target VARCHAR(11) ) RETURNS void AS

-- Count the number of events within one or more program stages and save the value
--  _pi_id: programinstanceid
-- ps_array: array of programstage uids
-- _de_target:  dataelement code  (destination)
-- _ps_target: programstage uid (destination)


CREATE OR REPLACE FUNCTION save_events_count (_pi_id INTEGER,ps_array VARCHAR(11)[],_de_target VARCHAR(50), _ps_target VARCHAR(11)) 


