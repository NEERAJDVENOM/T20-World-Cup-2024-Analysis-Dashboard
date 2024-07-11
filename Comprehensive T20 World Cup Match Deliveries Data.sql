SELECT 
    d.match_id,
    year(d.start_date) season,
    d.start_date,
    d.venue,
    d.innings,
    CAST(d.ball AS INT) + 1 AS over,
    CAST((d.ball - FLOOR(d.ball)) * 10 AS INT) AS ball,
    d.batting_team,
    d.bowling_team,
    d.striker,
    d.non_striker,
    d.bowler,
    d.runs_off_bat,
    d.extras,
    d.wides,
    d.noballs,
    d.byes,
    d.legbyes,
    d.penalty,
    d.wicket_type,
    d.player_dismissed,
    d.other_wicket_type,
    d.other_player_dismissed,
    m.team1,
    m.team2,
    m.city,
    m.toss_winner,
    m.toss_decision,
    m.player_of_match,
    m.umpire1,
    m.umpire2,
    m.reserve_umpire,
    m.match_referee,
    m.winner,
    m.winner_runs,
    m.winner_wickets,
    m.match_type
FROM 
    worldcup_t_20.deliveries_casted d 
LEFT JOIN 
    worldcup_t_20.matches_casted m 
ON 
    d.match_id = m.match_number;