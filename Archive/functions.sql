CREATE OR REPLACE FUNCTION upsert_trackedentitydatavalue (_psi_id integer, _de_id integer, _value varchar(50000), _storedby varchar(255), _created timestamp without time zone, _lastupdated timestamp without time zone) RETURNS integer AS $$

	DECLARE result integer;

	BEGIN
	IF EXISTS (SELECT 1 FROM trackedentitydatavalue
		WHERE dataelementid = _de_id
		AND programstageinstanceid = _psi_id)

	THEN
		-- If destination value exists, update
		UPDATE trackedentitydatavalue SET value = _value
			WHERE dataelementid = _de_id
			AND programstageinstanceid = _psi_id;

		GET DIAGNOSTICS result = ROW_COUNT;
	ELSE
		-- If not exists, insert new value
		INSERT INTO trackedentitydatavalue VALUES (_psi_id,	_de_id,	_value,	false, _storedby, _created, _lastupdated);

		GET DIAGNOSTICS result = ROW_COUNT;
	END IF;

	RETURN result;
	END;
	$$
	LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getEventsByProgramStage ( _programstageuid text ) RETURNS SETOF programstageinstance AS $$
	BEGIN
		RETURN QUERY SELECT * FROM programstageinstance WHERE programstageid IN (SELECT programstageid FROM programstage WHERE uid = _programstageuid);
	END;
	$$
	LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION getDataValueInStageByProgramInstance( _programinstanceid integer, _programstageuid text, _dataelementcode text) RETURNS SETOF trackedentitydatavalue AS $$
  BEGIN
    RETURN QUERY (SELECT tedv.* FROM trackedentitydatavalue tedv
      INNER JOIN programstageinstance psi ON tedv.programstageinstanceid = psi.programstageinstanceid
      WHERE psi.programinstanceid = _programinstanceid
      AND psi.programstageid IN (SELECT programstageid FROM programstage WHERE uid = _programstageuid)
      AND dataelementid IN (SELECT dataelementid FROM dataelement WHERE code = _dataelementcode));
  END;
  $$
  LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION getEventCountByProgramStagesAndProgramInstance( _programinstanceid integer, _programstageuids text[] ) RETURNS integer AS $$
  BEGIN
    RETURN (SELECT count(*) FROM programstageinstance psi
      WHERE psi.programinstanceid = _programinstanceid
      AND psi.programstageid IN (SELECT programstageid FROM programstage WHERE uid = ANY (_programstageuids)));
  END;
  $$
  LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION getEventDateByProgramStageAndProgramInstance( _programinstanceid integer, _programstageuid text ) RETURNS timestamp without time zone AS $$
  BEGIN
    RETURN (SELECT executiondate FROM programstageinstance psi
      WHERE psi.programinstanceid = _programinstanceid
      AND psi.programstageid IN (SELECT programstageid FROM programstage WHERE uid = _programstageuid));
  END;
  $$
  LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION getDaysBetweenDates( _start_date timestamp without time zone, _end_date timestamp without time zone) RETURNS integer AS $$
  BEGIN
    RETURN (SELECT _end_date::date - _start_date::date);
  END;
  $$
  LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION getProgramInstanceIdFromProgramStageInstance( _programstageinstanceid integer ) RETURNS integer AS $$
  BEGIN
    RETURN (SELECT programinstanceid FROM programstageinstance WHERE programstageinstanceid = _programstageinstanceid);
  END;
  $$
  LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION copyTrackedEntityDataValue(_src_de_code text, _src_stage_uid text, _dst_de_code text, _dst_stage_uid text) RETURNS void AS $$
  DECLARE event programstageinstance;
  DECLARE aux_datavalue trackedentitydatavalue;

  BEGIN
    FOR event IN (SELECT * FROM getEventsByProgramStage( _dst_stage_uid ))
    LOOP
      aux_datavalue = getDataValueInStageByProgramInstance(
        getProgramInstanceIdFromProgramStageInstance( event.programstageinstanceid ),
        _src_stage_uid,
        _src_de_code
      );

      IF aux_datavalue.value IS NOT NULL
      THEN
        PERFORM upsert_trackedentitydatavalue(
          event.programstageinstanceid,
          (SELECT dataelementid FROM dataelement WHERE code = _dst_de_code),
          aux_datavalue.value,
          aux_datavalue.storedby,
          aux_datavalue.created,
          aux_datavalue.lastupdated
        );
      END IF;
    END LOOP;
  END;
  $$
  LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION copyEventCount(_src_stage_uid text[], _dst_de_code text, _dst_stage_uid text) RETURNS void AS $$
  DECLARE event programstageinstance;
  DECLARE event_count integer;

  BEGIN
    FOR event IN (SELECT * FROM getEventsByProgramStage( _dst_stage_uid ))
    LOOP
      event_count = getEventCountByProgramStagesAndProgramInstance(
        getProgramInstanceIdFromProgramStageInstance( event.programstageinstanceid ),
        _src_stage_uid
      );

      PERFORM upsert_trackedentitydatavalue(
        event.programstageinstanceid,
        (SELECT dataelementid FROM dataelement WHERE code = _dst_de_code),
        event_count::text,
        'auto-generated',
        LOCALTIMESTAMP,
        LOCALTIMESTAMP
      );
    END LOOP;
  END;
  $$
  LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION saveDaysBetweenStages( _start_stage_uid text, _end_stage_uid text, _dst_de_code text, _dst_stage_uid text) RETURNS void AS $$
  DECLARE event programstageinstance;
  DECLARE days_between integer;
  DECLARE aux_programinstanceid integer;

  BEGIN
    FOR event IN (SELECT * FROM getEventsByProgramStage( _dst_stage_uid ))
    LOOP
      aux_programinstanceid = getProgramInstanceIdFromProgramStageInstance( event.programstageinstanceid );
      days_between = getDaysBetweenDates(
        getEventDateByProgramStageAndProgramInstance( aux_programinstanceid, _start_stage_uid ),
        getEventDateByProgramStageAndProgramInstance( aux_programinstanceid, _end_stage_uid )
      );

      PERFORM upsert_trackedentitydatavalue(
        event.programstageinstanceid,
        (SELECT dataelementid FROM dataelement WHERE code = _dst_de_code),
        days_between::text,
        'auto-generated',
        LOCALTIMESTAMP,
        LOCALTIMESTAMP
      );
    END LOOP;
  END;
  $$
  LANGUAGE 'plpgsql';


-- "Mental Health: Diagnosis": "TK_MH5"
-- "Mental Health: Number of follow ups": "TK_MH52"
-- "Mental Health: Number of consultations": "TK_MH58"
-- "Mental Health: Length of interventions": "TK_MH24"

-- "First visit": "bgq04wsYMp7"
-- "Consultation": "tmsr4EJaSPz"
-- "Closure": "XuThsezwYbZ"
SELECT copyTrackedEntityDataValue( 'TK_MH5', 'bgq04wsYMp7', 'TK_MH5', 'XuThsezwYbZ' );
SELECT copyEventCount( array['tmsr4EJaSPz'], 'TK_MH52', 'XuThsezwYbZ' );
SELECT copyEventCount( array['tmsr4EJaSPz', 'bgq04wsYMp7'], 'TK_MH58', 'XuThsezwYbZ' );

-- Length of interventions
SELECT saveDaysBetweenStages( 'bgq04wsYMp7', 'XuThsezwYbZ', 'TK_MH24', 'XuThsezwYbZ');
