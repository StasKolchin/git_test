CREATE OR REPLACE FUNCTION public.tmp_scout_match_pop_up(_p_player_id bigint, _p_match bigint, _p_language integer DEFAULT 1)
 RETURNS json
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$BEGIN
-- 123
	RETURN 
	(WITH
	player_match AS (
		SELECT
			fme.f_team
		FROM
			f_match_event fme
		WHERE
			fme.f_match = _p_match
			AND fme.f_player = _p_player_id
		LIMIT 1
	)
	,info_match AS (
		SELECT
			fm.match_date
			,CASE
				WHEN pm.f_team = fm.f_team1 THEN ft1.name_rus
				WHEN pm.f_team = fm.f_team2 THEN ft2.name_rus
				ELSE 'null'
			END AS team_name_ru
			,CASE
				WHEN pm.f_team = fm.f_team1 THEN ft1.name_eng
				WHEN pm.f_team = fm.f_team2 THEN ft2.name_eng
				ELSE 'null'
			END AS team_name_en			
			,CASE
				WHEN pm.f_team = fm.f_team1 THEN fm.score_team1
				WHEN pm.f_team = fm.f_team2 THEN fm.score_team2
				ELSE 0
			END AS team_score
			,CASE
				WHEN pm.f_team = fm.f_team1 THEN ft2.name_rus
				WHEN pm.f_team = fm.f_team2 THEN ft1.name_rus
				ELSE 'null'
			END AS team_name_opponent_ru
			,CASE
				WHEN pm.f_team = fm.f_team1 THEN ft2.name_eng
				WHEN pm.f_team = fm.f_team2 THEN ft1.name_eng
				ELSE 'null'
			END AS team_name_opponent_en
			,CASE
				WHEN pm.f_team = fm.f_team1 THEN fm.score_team2
				WHEN pm.f_team = fm.f_team2 THEN fm.score_team1
				ELSE 0
			END AS team_opponent_score
			,ftr.name_rus AS tournament_name_ru
			,ftr.name_eng AS tournament_name_en
		FROM
			f_match fm
		LEFT JOIN
			f_team ft1
				ON fm.f_team1 = ft1.id
		LEFT JOIN
			f_team ft2
				ON fm.f_team2 = ft2.id
		LEFT JOIN
			f_tournament ftr
				ON ftr.id = fm.f_tournament
		LEFT JOIN
			player_match pm
				ON 1=1
		WHERE
			fm.id = _p_match
			AND fm.c_match_status = 5
			AND fm.f_tournament <> 1)
	,info_match_json AS (
		SELECT
			array_to_json(array_agg(row_to_json(im))) AS val
		FROM
			info_match im)
	,full_game AS (
		SELECT
			fme.half
			--,ROUND(MIN(fme.second)) AS second_begin
			,CASE WHEN ROUND(MIN(fme.second)) = 0 THEN 0 ELSE ROUND(MIN(fme.second)) END - 2 AS second_begin -- Почему-то иногда 0 при преобразовании в json преобразуется в -0. Отрицательный ноль приходится вот так убирать.
			,ROUND(MAX(fme.second)) + 2 AS second_end
			-- ,MIN(fmed.second_clear) AS second_clear_begin
			-- ,MAX(fmed.second_clear) AS second_clear_end
		FROM
			f_match_event fme
		LEFT JOIN
			f_match fm
				ON fm.id = fme.f_match
		-- LEFT JOIN
			-- f_match_event_data fmed
				-- ON fme."id" = fmed.f_match_event
		WHERE
			fme.f_match = _p_match
			AND fme.dl = 0
		-- 	AND fmed.second_clear <> 0
		-- 	AND fme.second <> 0
			AND fm.c_match_status = 5
			--AND fm.f_tournament <> 1
		GROUP BY
			fme.half
		ORDER BY
			fme.half)
	,full_game_json AS (
		SELECT
			array_to_json(array_agg(row_to_json(fg))) AS val
		FROM
			full_game fg)
	,start_stop_data AS (
		SELECT
			me.f_match
			,me.half
			,me.second
			,me.c_action
			,CASE WHEN c_action = 100100 THEN 1 ELSE 0 END as start
			,row_number() OVER(ORDER BY half, second) as rn
		FROM
			f_match_event me
		LEFT JOIN
			f_match fm
				ON fm.id = me.f_match
		WHERE
			me.f_match = _p_match
			AND me.dl = 0
			AND me.half < 255
			--AND me.c_action IN (100100, 300800) -- 300800 стоп игра  100100 вбрасывание
			AND me.c_action IN (100100, 300800, 1800300, 1800400, 800100) -- 100100 вбрасывание, 300800 стоп игра, 1800300 перерыв, 1800400	Конец матча, 800100 гол
			AND fm.c_match_status = 5
			--AND fm.f_tournament <> 1
		)
	,ball_in_play AS (
		SELECT
			d1.f_match
			,d1.half
			,CASE WHEN d1.second - 2 < 0 THEN 0 ELSE d1.second - 2 END AS second_begin
			,d2.second + 2 AS second_end
		FROM
			start_stop_data d1
			JOIN start_stop_data d2
				ON d2.rn = d1.rn + 1
				AND d2.half = d1.half
				AND d2.start = 0
		WHERE
			d1.start = 1)
	,ball_in_play_json AS (
		SELECT
			array_to_json(array_agg(row_to_json(bip))) AS val
		FROM
			ball_in_play bip)
	,team AS (
		SELECT
			me.f_team AS f_team1
			,CASE
				WHEN fm.f_team1 = me.f_team THEN fm.f_team2
				WHEN fm.f_team2 = me.f_team THEN fm.f_team1
				ELSE NULL
			END AS f_team2
		FROM
			f_match fm
		LEFT JOIN
			f_match_event me
				ON fm.id = me.f_match
		WHERE
			fm.id = _p_match
			AND me.f_player = _p_player_id
			AND fm.dl = 0
			AND fm.c_match_status = 5
			--AND fm.f_tournament <> 1
		LIMIT 1)
	,shots AS (
		SELECT
			s.f_player
			,s.f_team
			,unnest(s.f_match_event_ids)::bigint AS marker_id
		FROM
			 f_stat_online_player_match s
		WHERE
			s.f_match = _p_match
			AND s.f_player = _p_player_id
			AND s.f_ttd_stat_param_player = 8 -- Броски
			AND s.f_ttd_stat_param_option = 0
			AND s.dl = 0
	)
	,shots_n AS (
		SELECT
			s.f_player
			,s.f_team
			,round(fmep.value::decimal,0) AS opponent_f_player
			,CASE WHEN me.f_team = t.f_team1 THEN t.f_team2 ELSE t.f_team1 END AS opponent_f_team
			,MAX(me.half) as half
			,MIN(me.second)-5::float4 as second_begin
			,MAX(me.second)+5::float4 as second_end
		FROM
			shots s
		LEFT JOIN
			f_match_event me
				ON me.id = s.marker_id
				AND me.dl = 0
		LEFT JOIN
			f_match_event_prop fmep
				ON fmep.f_match_event = s.marker_id
				AND (fmep.c_match_event_prop = 20)
		JOIN team t
			ON 1=1
		GROUP BY
			s.f_player
			,s.f_team
			,round(fmep.value::decimal,0)
			,CASE WHEN me.f_team = t.f_team1 THEN t.f_team2 ELSE t.f_team1 END
			,me.second
	)
	,goals AS (
		SELECT
			me.f_player
			,me.f_team
			,round(fmep.value::decimal,0) AS opponent_f_player
			,CASE WHEN me.f_team = t.f_team1 THEN t.f_team2 ELSE t.f_team1 END AS opponent_f_team
			,MAX(me.half) as half
			,MIN(me.second)-5::float4 as second_begin
			,MAX(me.second)+5::float4 as second_end
		FROM
			f_match_event me
		LEFT JOIN
			f_match_event_prop fmep
				ON fmep.f_match_event = me.id
				AND (fmep.c_match_event_prop = 20)
		JOIN team t
			ON 1=1
		WHERE
			me.f_match = _p_match
			AND me.c_action = 800100 -- Гол
			AND me.f_player = _p_player_id
			AND me.dl = 0
		GROUP BY
			me.dl = 0
			,me.f_player
			,me.f_team
			,round(fmep.value::decimal,0)
			,CASE WHEN me.f_team = t.f_team1 THEN t.f_team2 ELSE t.f_team1 END
			,me.second
	)
	,rifling_attack AS (
		SELECT
			me.f_player
			,me.f_team
			,round(fmep.value::decimal,0) AS opponent_f_player
			,CASE WHEN me.f_team = t.f_team1 THEN t.f_team2 ELSE t.f_team1 END AS opponent_f_team
			,MAX(me.half) as half
			,MIN(me.second)-5::float4 as second_begin
			,MAX(me.second)+5::float4 as second_end
		FROM
			f_match fm
		LEFT JOIN
			f_match_event me
				ON fm.id = me.f_match
		JOIN f_match_event_data med
				ON med.f_match_event = me.id
		LEFT JOIN
			f_match_event_prop fmep
				ON fmep.f_match_event = me.id
				AND (fmep.c_match_event_prop = 20)
		JOIN team t
			ON 1=1
		WHERE
			me.f_match = _p_match
			-- AND ((me.f_player = _p_player_id AND me.f_team = t.f_team1) OR
				-- (round(fmep.value::decimal,0) = _p_player_id AND me.f_team = t.f_team2))
			AND me.f_player = _p_player_id
			AND me.f_team = t.f_team1
			AND me.dl = 0
			AND med.possession_num IS NOT NULL
			AND me.f_player IS NOT NULL
			AND fm.c_match_status = 5
			AND fm.f_tournament <> 1
		GROUP BY
			me.f_player
			,me.f_team
			,round(fmep.value::decimal,0)
			,CASE WHEN me.f_team = t.f_team1 THEN t.f_team2 ELSE t.f_team1 END
			,med.possession_num
		ORDER BY
			me.f_player
			,med.possession_num)
	,rifling_defense AS (
		SELECT
			me.f_player
			,me.f_team
			,round(fmep.value::decimal,0) AS opponent_f_player
			,CASE WHEN me.f_team = t.f_team1 THEN t.f_team2 ELSE t.f_team1 END AS opponent_f_team
			,MAX(me.half) as half
			,MIN(me.second)-5::float4 as second_begin
			,MAX(me.second)+5::float4 as second_end
		FROM
			f_match fm
		LEFT JOIN
			f_match_event me
				ON fm.id = me.f_match
		JOIN f_match_event_data med
			ON med.f_match_event = me.id
		LEFT JOIN
			f_match_event_prop fmep
				ON fmep.f_match_event = me.id
				AND (fmep.c_match_event_prop = 20)
		JOIN team t
			ON 1=1
		WHERE
			me.f_match = _p_match
			AND me.dl = 0
			AND med.possession_num IS NOT NULL
			--AND me.f_player IS NOT NULL
			AND round(fmep.value::decimal,0) = _p_player_id
			-- AND me.f_team = t.f_team2
			AND fm.c_match_status = 5
			AND fm.f_tournament <> 1
		GROUP BY
			me.f_player
			,me.f_team
			,round(fmep.value::decimal,0)
			,CASE WHEN me.f_team = t.f_team1 THEN t.f_team2 ELSE t.f_team1 END 
			,med.possession_num
		ORDER BY
			me.f_player
			,med.possession_num)
	,all_time_player AS (
		SELECT
			ra.f_player
			,ra.f_team
			,ra.opponent_f_player
			,ra.opponent_f_team
			,ra.half
			,ra.second_begin
			,ra.second_end
		FROM
			rifling_attack ra
		UNION
		SELECT
			rd.f_player
			,rd.f_team
			,rd.opponent_f_player
			,rd.opponent_f_team
			,rd.half
			,rd.second_begin
			,rd.second_end
		FROM
			rifling_defense rd)
	-- ,rifling_attack_json AS (
		-- SELECT
			-- array_to_json(array_agg(row_to_json(ra))) AS val
		-- FROM
			-- rifling_attack ra)
	-- ,rifling_defense_json AS (
		-- SELECT
			-- array_to_json(array_agg(row_to_json(rd))) AS val
		-- FROM
			-- rifling_defense rd)
	-- ,lexic AS (
		-- SELECT
			-- fl.phrase_id AS id
			-- -- ,fl.phrase_text
			-- ,COALESCE (fl.phrase_text :: VARCHAR, 'null') AS phrase_text
		-- FROM
			-- f_lexic fl
		-- WHERE
			-- fl.c_language = _p_language
			-- AND fl.phrase_id IN (1295, 1345, 550, 2108, 2947, 2946, 4305, 3605, 5247, 5248, 2862, 3595, 2910, 3624, 5249, 5250)
		-- )
	-- ,lexic_json AS (
		-- SELECT
			-- array_to_json(array_agg(row_to_json(lx))) AS val
		-- FROM
			-- lexic lx)
	,all_time_player_json AS (
		SELECT
			array_to_json(array_agg(row_to_json(atp))) AS val
		FROM
			all_time_player atp)
	,shots_json AS (
		SELECT
			array_to_json(array_agg(row_to_json(s))) AS val
		FROM
			shots_n s)
	,goals_json AS (
		SELECT
			array_to_json(array_agg(row_to_json(g))) AS val
		FROM
			goals g)
	SELECT
		--json_build_object('lexics', t6.val, 'full_game', t1.val, 'ball_in_play', t2.val, 'all_time_player', t3.val, 'rifling_attack', t4.val, 'rifling_defense', t5.val)
		--json_build_object('info_match', t7.val, 'full_game', t1.val, 'ice_time', t2.val, 'all_time_player', t3.val, 'rifling_attack', t4.val, 'rifling_defense', t5.val)
		json_build_object('info_match', t7.val, 'full_game', t1.val, 'ice_time', t2.val, 'all_time_player', t3.val, 'shots', t8.val, 'goals', t9.val)
	FROM
		full_game_json t1
	JOIN
		ball_in_play_json t2
			ON 1=1
	JOIN
		all_time_player_json t3	
			ON 1=1
	-- JOIN
		-- rifling_attack_json t4
		 -- ON 1=1
	-- JOIN
		-- rifling_defense_json t5
		 -- ON 1=1
	-- JOIN
		-- lexic_json t6
		 -- ON 1=1
	JOIN
		info_match_json t7
			ON 1=1
	JOIN
		shots_json t8
			ON 1=1
	JOIN
		goals_json t9
			ON 1=1
	);
END

$function$