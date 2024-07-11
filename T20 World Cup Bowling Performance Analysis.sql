-- select * from worldcup_t_20.deliveries_casted;

select bowling_team as Team, bowler, 
count(distinct int(ball),match_id) as Total_over,
sum(case when wicket_type != 'run out' then 1 else 0 end) as Total_wickets,
cast(try_divide(sum(case when noballs != null or wides != null  then 0 else 1 end),sum(case when wicket_type != 'run out' then 1 else 0 end))as decimal(10,2)) as Bower_strike_Rate,
sum(noballs )as Total_no_balls,
sum(runs_off_bat)+sum(extras) runs_Conceeded,
cast(try_divide(runs_Conceeded,Total_over) as decimal(10,2)) as Ecomomy
from worldcup_t_20.deliveries_casted 
group by bowling_team,bowler order by Total_wickets desc;

-- select MATCH_ID, bowler, int(ball) as  over, count(ball), sum(case when (runs_off_bat=0 and extras is null) then 0 else 1 end) as maiden from worldcup_t_20.deliveries_casted
-- group by MATCH_ID, bowler, int(ball)
-- order by maiden
-- with t1 as (select MATCH_ID, bowler, int(ball) as  over, count(ball), ase when (runs_off_bat=0 and extras is null then 0 else 1 end) as no_run_ball from worldcup_t_20.deliveries_casted
-- )
