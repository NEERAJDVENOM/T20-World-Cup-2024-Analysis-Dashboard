--Batting_stats

with cte as(select distinct concat(batting_team,"_",striker ) as team_player,match_id, batting_team as Team, striker, 
sum(runs_off_bat) over(partition by match_id,striker) as runs from worldcup_t_20.deliveries_casted) ,


max_runs as (select  team_player,  max(runs) highest_individual_score from cte group by team_player,striker),


cte3 as (select concat(batting_team,"_",striker ) as team_player, batting_team as Team, striker, 
sum(runs_off_bat) as Total_runs_scored,
cast(try_divide(sum(runs_off_bat), sum(case when noballs is null and wides is null then 1 else 0 end))*100 as decimal(10,2)) as Batting_strike_rate,
sum(case when noballs is null and wides is null and runs_off_bat=6 then 1 else 0 end) as Total_6s,
sum(case when noballs is null and wides is null and runs_off_bat=4 then 1 else 0 end) as Total_4s,
(Total_6s+total_4s) Total_boundries
from worldcup_t_20.deliveries_casted
group by batting_team, striker )

select * from cte3 join max_runs on cte3.team_player =max_runs.team_player order by highest_individual_score desc
;
