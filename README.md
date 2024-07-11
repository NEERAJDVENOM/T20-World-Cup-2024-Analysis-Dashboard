# T20 World Cup 2024 Dashboard Project

## Overview

This project aims to analyze the T20 World Cup 2024 using Databricks to create an interactive and insightful dashboard. The dashboard leverages the power of PySpark and PySQL to process, analyze, and visualize data, providing comprehensive insights into team performances, player statistics, match outcomes, and tournament trends.

## Skills Used

- Azure Data Lake
- Databricks
- PySpark
- PySQL
- SQL

## Objective

The objective of this project is to analyze the T20 World Cup 2024 using Databricks to create an interactive and insightful dashboard. This project aims to leverage PySpark and PySQL for data processing, analysis, and visualization to provide comprehensive insights into various aspects of the tournament. The dashboard will enable users to explore team performances, player statistics, match outcomes, and tournament trends, offering a detailed understanding of the dynamics of the T20 World Cup 2024.

## Project Objectives

1. **Data Collection and Ingestion**:
    - Create an Azure blob storage account and upload the source CSV file to the `worldcupt202024` container.
    - Mount the Azure Data Lake Storage (ADLS) container to Databricks File System (DBFS) using an access key.
    - Create a dataframe and upload it as a table in the database.
    - Relevant code can be found in the ‘Import from Azure Blob Storage’ notebook.

2. **Data Transformation and Cleaning**:
    - Create a new notebook for transforming and cleaning data (`T20Worldcup_DataTransformation`).
    - Store the clean data in the database as a table.

3. **Dashboard Creation**:
    - Use the data tab on the Databricks dashboard to select the required database and tables.
    - Create custom SQL queries for the dashboard:
        - T20 World Cup Bowling Performance Analysis
        - T20 World Cup Batting Performance Analysis
        - Additional queries include:
            1. **Team_Batsman with highest Strike Rate in the Tournament**
                ```sql
                select concat(batting_team,"_",striker ) as team_player, 
                batting_team as Team, striker,  
                cast(try_divide(sum(runs_off_bat), sum(case when noballs is 
                null and wides is null then 1 else 0 end))*100 as 
                decimal(10,2)) as Batting_strike_rate 
                from worldcup_t_20.deliveries_casted 
                group by batting_team, striker order by Batting_strike_rate 
                desc limit 3;
                ```
            2. **Top 3 Highest Run Scorers in the Tournament**
                ```sql
                select concat(batting_team,"_",striker ) as team_player, 
                batting_team as Team, striker,  
                sum(runs_off_bat) as Total_runs_scored  
                from worldcup_t_20.deliveries_casted 
                group by batting_team, striker  order by Total_runs_scored 
                desc limit 3;
                ```
            3. **Top 5 Individual Run Scorers in the Tournament**
                ```sql
                with cte as(select distinct concat(batting_team,"_",striker ) 
                as team_player,match_id, batting_team as Team, striker,  
                sum(runs_off_bat) over(partition by match_id,striker) as runs 
                from worldcup_t_20.deliveries_casted)  
                select  team_player,  max(runs) highest_individual_score from 
                cte group by team_player,striker order by 
                highest_individual_score desc limit 5;
                ```
            4. **Top 5 Players with the Highest Number of Total 6's in the Tournament**
                ```sql
                select concat(batting_team,"_",striker ) as team_player, 
                batting_team as Team, striker,  
                sum(runs_off_bat) as Total_runs_scored, 
                sum(case when noballs is null and wides is null and 
                runs_off_bat=6 then 1 else 0 end) as Total_6s, 
                sum(case when noballs is null and wides is null and 
                runs_off_bat=4 then 1 else 0 end) as Total_4s 
                from worldcup_t_20.deliveries_casted 
                group by batting_team, striker 
                order by   Total_6s desc limit 5;
                ```
            5. **Top 5 Players with the Highest Number of Total Boundaries in the Tournament**
                ```sql
                select concat(batting_team,"_",striker ) as team_player, 
                batting_team as Team, striker,  
                sum(runs_off_bat) as Total_runs_scored, 
                sum(case when noballs is null and wides is null and 
                runs_off_bat=6 then 1 else 0 end) as Total_6s, 
                sum(case when noballs is null and wides is null and 
                runs_off_bat=4 then 1 else 0 end) as Total_4s, 
                (Total_6s+total_4s) Total_boundries 
                from worldcup_t_20.deliveries_casted 
                group by batting_team, striker 
                order by  Total_boundries desc limit 5;
                ```
            6. **Top 5 Wicket Takers with Their Bowling Strike Rates**
                ```sql
                select concat(bowling_team,"_",bowler ) as team_player, 
                bowling_team as Team, bowler,  
                cast(try_divide(sum(case when noballs != null or wides 
                != null  then 0 else 1 end),sum(case when wicket_type 
                != 'run out' then 1 else 0 end))as decimal(10,2)) as 
                Bower_strike_Rate, 
                sum(case when wicket_type != 'run out' then 1 else 0 
                end) as Total_wickets 
                from worldcup_t_20.deliveries_casted  
                group by bowling_team,bowler order by Total_wickets 
                desc limit 5;
                ```
            7. **Top 10 Bowlers Bowled with Lowest Economy Rate with at Least 12 Overs Bowled in the Tournament**
                ```sql
                select concat(bowling_team,"_",bowler ) as team_player, 
                bowling_team as Team, bowler,  
                count(distinct int(ball),match_id) as Total_over, 
                sum(runs_off_bat)+sum(extras) runs_Conceeded, 
                cast(try_divide(runs_Conceeded,Total_over) as decimal(10,2)) as 
                Ecomomy 
                from worldcup_t_20.deliveries_casted  
                group by bowling_team,bowler having Ecomomy is not 
                null  and  Total_over>=12 
                order by Ecomomy limit 10;
                ```
        - Publish and schedule the dashboard to automate data refresh.

4. **Automate Data Flow**:
    - Schedule the workflow to automate the data flow. Although not required for this project, automation can be achieved using the job run option in Databricks if the source data is streaming or live.

## Repository Structure

- `data/`: Contains the source CSV files.
- `notebooks/`: Contains the Databricks notebooks for data ingestion, transformation, and analysis.
- `sql_queries/`: Contains the SQL queries used in the dashboard.
- `dashboard/`: Contains the dashboard files and configurations.

## Setup Instructions

1. Create an Azure blob storage account and upload the source CSV file to the `worldcupt202024` container.
2. Mount the ADLS container to Databricks File System (DBFS) using the access key.
3. Follow the instructions in the `Import from Azure Blob Storage` notebook to create a dataframe and upload it as a table in the database.
4. Use the `T20Worldcup_DataTransformation` notebook to clean and transform the data, and store the clean data in the database.
5. Use the Databricks dashboard to create the required visualizations using the provided SQL queries.
6. Publish and schedule the dashboard for automated data refresh.

## License

This project is licensed under the MIT License - see the LICENSE.md file for details.
