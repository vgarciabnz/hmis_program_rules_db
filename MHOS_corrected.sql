 CREATE OR REPLACE FUNCTION get_datavalue (_psi_id integer, _de_dst varchar(11)[]) RETURNS SETOF TRACKEDENTITYDATAVALUE  AS $$

        BEGIN 

            RETURN QUERY (SELECT * FROM TRACKEDENTITYDATAVALUE WHERE programstageinstanceid=_psi_id and dataelementid in (select dataelementid from dataelement where code=any(_de_dst)));
        END;

        $$
        LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION mh_MHOS_correction (_pi_id integer, _ps_src character varying, _de_dst character varying) RETURNS void AS $$

            DECLARE  target_event_id integer;
            DECLARE  age_at_enrollment  integer;
            DECLARE  score_QCH_1_14 value_with_date;
            DECLARE  score_QCH9 value_with_date;


            BEGIN

                FOR target_event_id IN (
                    SELECT programstageinstanceid from programstageinstance where programinstanceid=_pi_id and programstageid in (select programstageid from programstage where uid=_ps_src)
                )
                
                LOOP  
                
                
                select value into age_at_enrollment from get_datavalue(target_event_id,array['TK_MH36']);
                
                    IF age_at_enrollment<15

                        THEN
                            
                            SELECT greatest(0,sum(value::numeric)),coalesce (max(lastupdated),now()) into score_QCH9 from get_datavalue(target_event_id,array['TK_MH95']);
                            SELECT greatest(0,sum(value::numeric)),coalesce (max(lastupdated),now()) into score_QCH_1_14 from get_datavalue(target_event_id,array['TK_MH87','TK_MH88','TK_MH89','TK_MH90','TK_MH91','TK_MH92','TK_MH93','TK_MH94','TK_MH96','TK_MH97','TK_MH98','TK_MH99','TK_MH100']);

                                
                                PERFORM upsert_trackedentitydatavalue(target_event_id,
                                (SELECT dataelementid from dataelement where code=_de_dst),
                                (score_QCH_1_14.val::numeric - score_QCH9.val::numeric)::text, 
                                'auto-generated',
                                greatest(score_QCH9.lastupdated,score_QCH9.lastupdated),
                                greatest(score_QCH9.lastupdated,score_QCH9.lastupdated)
                                );
                    END IF;


                            
                END LOOP;
            END;
        $$
        LANGUAGE plpgsql;


        CREATE OR REPLACE FUNCTION execute_MHOS (last_updated timestamp without time zone)
        RETURNS SETOF integer AS
        $$

            DECLARE program_instance_id INTEGER;
            
            BEGIN
                FOR program_instance_id in (SELECT * FROM get_programinstance_modified_after('ORvg6A5ed7z', last_updated))
                LOOP
                    -- MHOS_corrected
                    PERFORM mh_MHOS_correction (program_instance_id,'tmsr4EJaSPz','TK_MH12'); 
                    
                    RETURN QUERY SELECT program_instance_id;
                END LOOP;
            
            END;
        $$
        LANGUAGE plpgsql;

        DO $$

        DECLARE mhos_correction text;

         
        BEGIN

            SELECT value INTO mhos_correction FROM public.hmisocba_settings where name= 'MHOS_correction';
            IF NOT FOUND THEN 

            
                    PERFORM execute_MHOS ('2000-01-01 00:00:00');
                   
                    INSERT INTO public.hmisocba_settings (name,value) VALUES ('MHOS_correction','Done');
                         
            END IF; 
        END
        $$
        LANGUAGE plpgsql;


        DROP FUNCTION get_datavalue (_psi_id integer, _de_dst varchar(11)[]);
        DROP FUNCTION mh_MHOS_correction (_pi_id integer, _ps_src character varying, _de_dst character varying);
        DROP FUNCTION execute_MHOS  (last_updated timestamp without time zone);
        

  


