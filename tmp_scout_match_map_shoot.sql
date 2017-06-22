CREATE OR REPLACE FUNCTION public.tmp_scout_match_map_shoot(_p_match_arr bigint[] DEFAULT NULL::bigint[], _p_player_id integer DEFAULT NULL::integer, _p_team_id integer DEFAULT NULL::integer, _p_opponent_player_id integer DEFAULT NULL::bigint, _p_date_from timestamp without time zone DEFAULT NULL::timestamp without time zone, _p_date_to timestamp without time zone DEFAULT NULL::timestamp without time zone)
 RETURNS TABLE(marker_id bigint, f_match bigint, c_field_type smallint, f_ttd_stat_param_option smallint, second double precision, half smallint, c_zone smallint, f_team integer, f_player integer, firstname_eng character varying, lastname_eng character varying, firstname_rus character varying, lastname_rus character varying, nickname_eng character varying, nickname_rus character varying, c_action integer, opponent_f_player integer, opponent_firstname_eng character varying, opponent_lastname_eng character varying, opponent_firstname_rus character varying, opponent_lastname_rus character varying, opponent_nickname_eng character varying, opponent_nickname_rus character varying, shoot_name character varying, opened character varying, attack_type text, kuda_prinal_shaibu text, ruka_kuda_prinal_shaibu text, chem_sigral text, shoot_type text, is_opponent boolean, pos_x real, pos_y real, pos_cage_x real, pos_cage_y real, team_name_ru character varying, team_name_en character varying, team_score integer, team_name_opponent_ru character varying, team_name_opponent_en character varying, team_opponent_score integer, field_players text, player_amplua_id smallint, player_amplua_name_ru character varying, player_amplua_name_en character varying, opponent_amplua_id smallint, opponent_amplua_name_ru character varying, opponent_amplua_name_en character varying, lexic_action text, goalkeeper_f_player integer, goalkeeper_firstname_eng character varying, goalkeeper_lastname_eng character varying, goalkeeper_firstname_rus character varying, goalkeeper_lastname_rus character varying, goalkeeper_nickname_eng character varying, goalkeeper_nickname_rus character varying)
 LANGUAGE plpgsql
AS $function$
        BEGIN
        	-- git test 3
        	RETURN QUERY(
        		WITH
        			player_matches AS ( -- Выбираем все матчи, в которых участвовал игрок
        				SELECT
        					m.id as f_match
        					,s.f_team
        					,CASE WHEN s.f_team = m.f_team1 THEN m.f_team2 ELSE m.f_team1 END as opp_f_team
        				FROM
        					f_match m
        					JOIN f_stat_online_player_match s
        						ON s.f_match = m.id
        						AND s.dl = 0
        						AND s.f_ttd_stat_param_player = 1
        						AND s.f_ttd_stat_param_option = 0
        						--AND s.f_player = 134
        				WHERE
        					m.dl = 0
							AND (_p_match_arr IS NULL OR m.id IN (SELECT UNNEST(_p_match_arr)))
							AND s.f_player = _p_player_id
							AND (_p_opponent_player_id IS NULL OR s.f_player = _p_opponent_player_id)
							AND (_p_date_from IS NULL OR m.match_date >= _p_date_from)
							AND (_p_date_to IS NULL OR m.match_date <= _p_date_to)
							AND m.c_match_status = 5
							AND m.f_tournament <> 1 -- riv
        			)
        			,team_matches AS ( -- Выбираем все матчи, в которых участвовал игрок
        				SELECT
        					m.id as f_match
        					,t.f_team
        					,CASE WHEN t.f_team = m.f_team1 THEN m.f_team2 ELSE m.f_team1 END as opp_f_team
        				FROM
        					(SELECT _p_team_id as f_team) t
        					JOIN f_match m
        						ON (m.f_team1 = t.f_team OR m.f_team2 = t.f_team)
        						AND m.dl = 0
        						AND (_p_match_arr IS NULL OR m.id = ANY(_p_match_arr))
        						AND (_p_date_from IS NULL OR m.match_date >= _p_date_from)
        						AND (_p_date_to IS NULL OR m.match_date <= _p_date_to)
        						AND m.c_match_status = 5
        						AND m.f_tournament != 1 -- riv
        			)
					,matches_opp_goalkeepers_in_out AS (
						SELECT
							p.f_match
							,p.f_team
							,p.f_player
							--,p.c_amplua
							,ps.half
							,ps.second_in
							,ps.second_out
						FROM
							(
								SELECT
									pm.f_match
									,me.f_team
									,me.f_player
									--,ceil(me.c_action%1000/100) as c_amplua
									,row_number() OVER(PARTITION BY me.f_player,pm.f_match ORDER BY me.second) as rn
								FROM
									(
										SELECT * FROM player_matches
										UNION ALL SELECT * FROM team_matches
									) pm
									JOIN f_match_event me
										ON me.f_match = pm.f_match
										AND me.dl = 0
										AND me.half = 1
										AND me.f_team = pm.opp_f_team
										AND ceil(me.c_action/100000) = 16
										AND ceil(me.c_action%1000/100) = 1
							) p
							JOIN f_player_match_subs ps
								ON ps.f_match = p.f_match
								AND ps.f_team = p.f_team
								AND ps.f_player = p.f_player
								AND ps.dl = 0
						WHERE
							p.rn = 1
					)
        			,shots AS (
        				SELECT
        					s.f_match
        					,s.f_team
        					,s.f_player
        					,s.f_ttd_stat_param_player AS param_id
        					,s.f_ttd_stat_param_option
        					,unnest(s.f_match_event_ids)::bigint AS marker_id
        				FROM
        					(
        						SELECT * FROM player_matches
        						UNION ALL SELECT * FROM team_matches
        					) pm
        					LEFT JOIN f_stat_online_player_match s
        						ON pm.f_match = s.f_match
        				WHERE
        					--s.f_ttd_stat_param_player = 5 --ANY(array[5, 42,43,44,45, 51,52,53,54, 80,81,83,84,86,87,89,90,92,93,95,96,98,99,101,102,104,105, 61,62,65,66,142,143,146,147,150,151,154,155,158,159,162,163]) -- тут все
							s.f_ttd_stat_param_player = 8 -- riv
							AND s.f_ttd_stat_param_option = 0
							AND s.dl = 0
        			)
        			,otbor_po AS (
        				SELECT
        					pt.f_match
        					,pt.f_team
        					,pt.f_player
        					,pt.param_id
        					,pt.f_ttd_stat_param_option
        					,pt.marker_id
        					,CASE WHEN pt.f_team = m.f_team1 THEN m.f_team2 ELSE m.f_team1 END as opponent_f_team
							,ROUND(opp.value::decimal,0) AS opponent_player_id -- riv
        					--,CASE WHEN pt.f_team = m.f_team2 THEN true ELSE false END as is_opponent
							,CASE WHEN pt.f_team = pm.f_team THEN false ELSE true END as is_opponent
        				FROM
        					shots pt
        					LEFT JOIN f_match_event_prop opp
        						ON opp.f_match_event = pt.marker_id
        						--AND (opp.c_match_event_prop = 11 OR opp.c_match_event_prop = 15)
								AND (opp.c_match_event_prop = 20) -- riv
        					LEFT JOIN f_match_event fme
        						ON fme.id = pt.marker_id
							LEFT JOIN player_matches pm
        						ON pm.f_match = pt.f_match
							LEFT JOIN
								f_match m
									ON m.id = pt.f_match
        				WHERE
        					(_p_player_id IS NULL OR pt.f_player = _p_player_id OR ROUND(opp.value::decimal,0) = _p_player_id) -- fmep.value::INT = _p_player_id)
							AND (_p_team_id IS NULL OR pt.f_team = _p_team_id)
							AND (_p_opponent_player_id IS NULL OR COALESCE(fme.f_player, ROUND(opp.value::decimal,0)) = _p_opponent_player_id) -- fmep.value::INT = _p_opponent_player_id))
        			)
        		,props AS (
        			SELECT
        				s.marker_id
        				,mep.c_match_event_prop
        				--,mep.c_match_event_prop_value
        				,mep.value
        			FROM
        				shots s
        				JOIN f_match_event_prop mep
        					ON mep.f_match_event = s.marker_id
        			WHERE
        				(
        				mep.c_match_event_prop IN(16,17)
        				-- OR (mep.c_match_event_prop = 1 AND mep.c_match_event_prop_value IN (1,2,3,4,5,6,8,9,36))
        				-- OR (mep.c_match_event_prop = 3 AND mep.c_match_event_prop_value IN (13,14,15,16,17,18,19,20))
        				-- OR (mep.c_match_event_prop = 5 AND mep.c_match_event_prop_value = 30)
						OR (mep.c_match_event_prop = 1)
        				OR (mep.c_match_event_prop = 3)
        				OR (mep.c_match_event_prop = 5)
        			)
        		)
        		SELECT
        			ask.marker_id
        			,fme.f_match
					,fm.c_field_type
        			,ask.f_ttd_stat_param_option::SMALLINT
        			,fme."second"
        			,fme.half
					,fmed.c_zone_type2 AS c_zone
        			,fme.f_team
        			,fme.f_player
        			,COALESCE(fp.firstname_eng :: VARCHAR, 'null') AS firstname_eng
        			,COALESCE(fp.lastname_eng :: VARCHAR, 'null') AS lastname_eng
        			,COALESCE(fp.firstname_rus :: VARCHAR, 'null') AS firstname_rus
        			,COALESCE(fp.lastname_rus :: VARCHAR, 'null') AS lastname_rus
        			,COALESCE(fp.nickname_eng :: VARCHAR, 'null') AS nickname_eng
        			,COALESCE(fp.nickname_rus :: VARCHAR, 'null') AS nickname_rus
        			,fme.c_action
        			,ask.opponent_player_id::INT AS opponent_f_player
        			,COALESCE(fp_o.firstname_eng :: VARCHAR, 'null') AS opponent_firstname_eng
        			,COALESCE(fp_o.lastname_eng :: VARCHAR, 'null') AS opponent_lastname_eng
        			,COALESCE(fp_o.firstname_rus :: VARCHAR, 'null') AS opponent_firstname_rus
        			,COALESCE(fp_o.lastname_rus :: VARCHAR, 'null') AS opponent_lastname_rus
        			,COALESCE(fp_o.nickname_eng :: VARCHAR, 'null') AS opponent_nickname_eng
        			,COALESCE(fp_o.nickname_rus :: VARCHAR, 'null') AS opponent_nickname_rus
        			,ca.name AS shoot_name
        			,CASE
        				WHEN (fme.c_action = 800201 OR fme.c_action = 800301) AND p_otkr_zakr.marker_id IS NULL THEN '0+'
        				WHEN (fme.c_action = 800202 OR fme.c_action = 800302) AND p_otkr_zakr.marker_id IS NULL THEN '0-'
        				WHEN (fme.c_action = 800201 OR fme.c_action = 800301) AND p_otkr_zakr.marker_id IS NOT NULL THEN '1+'
        				WHEN (fme.c_action = 800202 OR fme.c_action = 800302) AND p_otkr_zakr.marker_id IS NOT NULL THEN '1-'
        			END::VARCHAR(5) AS opened
					,CASE
						/*WHEN fmed.c_attack_type = 2 THEN 'Позиционная атака'
						WHEN fmed.c_attack_type = 1 THEN 'Атака с ходу'*/
						WHEN fmed.c_attack_type = 2 THEN '920'
						WHEN fmed.c_attack_type = 1 THEN '6044'
						ELSE 'null'
					END AS attack_type
					,CASE
						/*WHEN round(p_kuda_prinal_shaibu.value::decimal,0) = 20 THEN 'Над блином'
						WHEN round(p_kuda_prinal_shaibu.value::decimal,0) = 21 THEN 'Над ловушкой'
						WHEN round(p_kuda_prinal_shaibu.value::decimal,0) = 22 THEN 'Под блином'
						WHEN round(p_kuda_prinal_shaibu.value::decimal,0) = 23 THEN 'Под ловушкой'
						WHEN round(p_kuda_prinal_shaibu.value::decimal,0) = 24 THEN 'В дом'
						WHEN round(p_kuda_prinal_shaibu.value::decimal,0) = 25 THEN 'В тело'*/
						WHEN p_kuda_prinal_shaibu.value::int = 20 THEN '6045'
						WHEN p_kuda_prinal_shaibu.value::int = 21 THEN '6046'
						WHEN p_kuda_prinal_shaibu.value::int = 22 THEN '6047'
						WHEN p_kuda_prinal_shaibu.value::int = 23 THEN '6048'
						WHEN p_kuda_prinal_shaibu.value::int = 24 THEN '6049'
						WHEN p_kuda_prinal_shaibu.value::int = 25 THEN '6050'
					END AS kuda_prinal_shaibu
					,CASE
						/*WHEN round(p_ruka_kuda_prinal_shaibu.value::decimal,0) = 26 THEN 'Правой'
						WHEN round(p_ruka_kuda_prinal_shaibu.value::decimal,0) = 27 THEN 'Левой'*/
						WHEN p_ruka_kuda_prinal_shaibu.value::int = 26 THEN '781'
						WHEN p_ruka_kuda_prinal_shaibu.value::int = 27 THEN '6052'
					END AS ruka_kuda_prinal_shaibu
					,CASE
						/*WHEN round(p_chem_sigral.value::decimal,0) = 51 THEN 'Голова'
						WHEN round(p_chem_sigral.value::decimal,0) = 52 THEN 'Правое плечо'
						WHEN round(p_chem_sigral.value::decimal,0) = 53 THEN 'Левое плечо'
						WHEN round(p_chem_sigral.value::decimal,0) = 54 THEN 'Ловушка'
						WHEN round(p_chem_sigral.value::decimal,0) = 55 THEN 'Подушка'
						WHEN round(p_chem_sigral.value::decimal,0) = 56 THEN 'Правая рука'
						WHEN round(p_chem_sigral.value::decimal,0) = 57 THEN 'Левая рука'
						WHEN round(p_chem_sigral.value::decimal,0) = 58 THEN 'Тело'
						WHEN round(p_chem_sigral.value::decimal,0) = 59 THEN 'Верх правого щитка'
						WHEN round(p_chem_sigral.value::decimal,0) = 60 THEN 'Середина правого щитка'
						WHEN round(p_chem_sigral.value::decimal,0) = 61 THEN 'Низ правого щитка'
						WHEN round(p_chem_sigral.value::decimal,0) = 62 THEN 'Верх левого щитка'
						WHEN round(p_chem_sigral.value::decimal,0) = 63 THEN 'Середина левого щитка'
						WHEN round(p_chem_sigral.value::decimal,0) = 64 THEN 'Низ левого щитка'
						WHEN round(p_chem_sigral.value::decimal,0) = 65 THEN 'Клюшка(основная часть)'
						WHEN round(p_chem_sigral.value::decimal,0) = 66 THEN 'Клюшка(нижняя часть)'*/

						WHEN p_chem_sigral.value::int = 51 THEN '6053'
						WHEN p_chem_sigral.value::int = 52 THEN '6064'
						WHEN p_chem_sigral.value::int = 53 THEN '6055'
						WHEN p_chem_sigral.value::int = 54 THEN '6056'
						WHEN p_chem_sigral.value::int = 55 THEN '6057'
						WHEN p_chem_sigral.value::int = 56 THEN '6058'
						WHEN p_chem_sigral.value::int = 57 THEN '6059'
						WHEN p_chem_sigral.value::int = 58 THEN '1861'
						WHEN p_chem_sigral.value::int = 59 THEN '6060'
						WHEN p_chem_sigral.value::int = 60 THEN '6061'
						WHEN p_chem_sigral.value::int = 61 THEN '6062'
						WHEN p_chem_sigral.value::int = 62 THEN '6063'
						WHEN p_chem_sigral.value::int = 63 THEN '6064'
						WHEN p_chem_sigral.value::int = 64 THEN '6065'
						WHEN p_chem_sigral.value::int = 65 THEN '6066'
						WHEN p_chem_sigral.value::int = 66 THEN '6067'
					END AS chem_sigral
					,CASE
						/*WHEN round(p_tip_broska.value::decimal,0) = 2 THEN 'Щелчок с размахом'
						WHEN round(p_tip_broska.value::decimal,0) = 3 THEN 'Кистевой'
						WHEN round(p_tip_broska.value::decimal,0) = 4 THEN 'С неудобной руки'
						WHEN round(p_tip_broska.value::decimal,0) = 5 THEN 'Подставленная клюшка'
						WHEN round(p_tip_broska.value::decimal,0) = 6 THEN 'Бросок с близкой дистанции(добивание)'
						WHEN round(p_tip_broska.value::decimal,0) = 1 THEN 'Щелчок'*/

						WHEN round(p_tip_broska.value::decimal,0) = 2 THEN '6031'
						WHEN round(p_tip_broska.value::decimal,0) = 3 THEN '4059'
						WHEN round(p_tip_broska.value::decimal,0) = 4 THEN '5683'
						WHEN round(p_tip_broska.value::decimal,0) = 5 THEN '6043'
						WHEN round(p_tip_broska.value::decimal,0) = 6 THEN '6032'
						WHEN round(p_tip_broska.value::decimal,0) = 1 THEN '4058'
						ELSE 'null'
					END AS shoot_type
        			,ask.is_opponent
					,COALESCE(fme.pos_x, fmep_x.value, 0)::FLOAT4 AS pos_x -- riv
					,COALESCE(fme.pos_y, fmep_y.value, 0)::FLOAT4 AS pos_y -- riv
					,COALESCE(cage_x.value, 0)::FLOAT4 AS pos_cage_x -- riv
					,COALESCE(cage_y.value, 0)::FLOAT4 AS pos_cage_y -- riv
        			,t.name_rus AS team_name_ru
        			,t.name_eng AS team_name_en
        			,CASE
        				WHEN ask.f_team = fm.f_team1 THEN fm.score_team1
        				WHEN ask.f_team = fm.f_team2 THEN fm.score_team2
        				ELSE 0
        			END AS team_score
        			,t_opp.name_rus AS team_name_opponent_ru
        			,t_opp.name_eng AS team_name_opponent_en
        			,CASE
        				WHEN ask.f_team = fm.f_team1 THEN fm.score_team2
        				WHEN ask.f_team = fm.f_team2 THEN fm.score_team1
        				ELSE 0
        			END AS team_opponent_score
					,CASE
						WHEN ask.f_team = fm.f_team1 THEN COALESCE(fmed.field_players_num_team1,0)||'x'||COALESCE(fmed.field_players_num_team2,0)
						ELSE COALESCE(fmed.field_players_num_team2,0)||'x'||COALESCE(fmed.field_players_num_team1,0)
					END as field_players
					,cap.id AS player_amplua_id
					,cap.name AS player_amplua_name_ru
					,cap.name_eng AS player_amplua_name_en
					,cao.id AS opponent_amplua_id
					,cao.name AS opponent_amplua_name_ru
					,cao.name_eng AS opponent_amplua_name_en
					,CASE WHEN fme.c_action = 800100 THEN '3150' -- Гол 
						WHEN fme.c_action = 400200 THEN '6041' -- Бросок отбитый 
						WHEN fme.c_action = 400100 THEN '3344' -- Бросок мимо
						WHEN fme.c_action = 400300 THEN '3343' -- Бросок заблокированный
						ELSE 'null'
					END AS lexic_action
					,mg.f_player as goalkeeper_f_player
        			,gp.firstname_eng :: VARCHAR AS goalkeeper_firstname_eng
        			,gp.lastname_eng :: VARCHAR AS goalkeeper_lastname_eng
        			,gp.firstname_rus :: VARCHAR AS goalkeeper_firstname_rus
        			,gp.lastname_rus :: VARCHAR AS goalkeeper_lastname_rus
        			,gp.nickname_eng :: VARCHAR AS goalkeeper_nickname_eng
        			,gp.nickname_rus :: VARCHAR AS goalkeeper_nickname_rus
        		FROM
        			otbor_po ask
        		JOIN f_match_event fme
        			ON fme.id = ask.marker_id
        			AND fme.dl = 0
				LEFT JOIN
					f_match_event_data fmed
						ON fmed.f_match_event = ask.marker_id
        		LEFT JOIN f_match fm
        			ON fm.id = fme.f_match
        		LEFT JOIN f_team t
        			ON t.id = ask.f_team
        		LEFT JOIN f_team t_opp
        			ON t_opp.id = ask.opponent_f_team
				LEFT JOIN
					f_match_event_prop fmep_x
						ON fmep_x.f_match_event = ask.marker_id
						AND fmep_x.c_match_event_prop = 21
				LEFT JOIN
					f_match_event_prop fmep_y
						ON fmep_y.f_match_event = ask.marker_id
						AND fmep_y.c_match_event_prop = 22
				LEFT JOIN
					f_match_event_prop cage_x
						ON cage_x.f_match_event = ask.marker_id
						AND cage_x.c_match_event_prop = 8
				LEFT JOIN
					f_match_event_prop cage_y
						ON cage_y.f_match_event = ask.marker_id
						AND cage_y.c_match_event_prop = 9
				LEFT JOIN
					f_match_event_prop p_tip_broska
						ON p_tip_broska.f_match_event = ask.marker_id
						AND p_tip_broska.c_match_event_prop = 1
						AND round(p_tip_broska.value::decimal,0) IN (2,3,4,5,6,1)
				LEFT JOIN
					f_match_event_prop p_kuda_prinal_shaibu
						ON p_kuda_prinal_shaibu.f_match_event = ask.marker_id
						AND p_kuda_prinal_shaibu.c_match_event_prop = 5
						AND round(p_kuda_prinal_shaibu.value::decimal,0) IN (20,21,22,23,24,25)
				LEFT JOIN
					f_match_event_prop p_ruka_kuda_prinal_shaibu
						ON p_ruka_kuda_prinal_shaibu.f_match_event = ask.marker_id
						AND p_ruka_kuda_prinal_shaibu.c_match_event_prop = 7
						AND round(p_ruka_kuda_prinal_shaibu.value::decimal,0) IN (26,27)
				LEFT JOIN
					f_match_event_prop p_chem_sigral
						ON p_chem_sigral.f_match_event = ask.marker_id
						AND p_chem_sigral.c_match_event_prop = 15
						AND round(p_chem_sigral.value::decimal,0) IN (51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66)
        		LEFT JOIN props p_otkr_zakr
        			ON p_otkr_zakr.marker_id = ask.marker_id
        			AND p_otkr_zakr.c_match_event_prop = 5
        		LEFT JOIN c_action ca
        			ON ca.id = fme.c_action
        		LEFT JOIN f_player fp
        			ON fp.id = fme.f_player
				LEFT JOIN c_amplua cap
					ON fp.c_amplua1 = cap.id
        		LEFT JOIN f_player fp_o
        			ON fp_o.id = ask.opponent_player_id
				LEFT JOIN c_amplua cao
					ON fp_o.c_amplua1 = cao.id
				LEFT JOIN matches_opp_goalkeepers_in_out mg
					ON mg.f_match = ask.f_match
					--AND mg.f_team != ask.f_team
					AND mg.half = fme.half
					AND fme.second BETWEEN mg.second_in AND mg.second_out-0.001
				LEFT JOIN f_player gp
					ON gp.id = mg.f_player
        		ORDER BY
        			ask.opponent_f_team
        			,ask.opponent_player_id
        			,ask.marker_id
            );
            END
        $function$