WITH initial_games AS (
    SELECT
        date,
        aggregate,
        home_club_id AS club_id,
        CASE
            WHEN substring(aggregate, 3) <= substring(aggregate, 1, 1) THEN 1
            ELSE 0
        END AS did_win
    FROM
        games
    UNION ALL
    SELECT
        date,
        aggregate,
        away_club_id AS club_id,
        CASE
            WHEN substring(aggregate, 3) >= substring(aggregate, 1, 1) THEN 1
            ELSE 0
        END AS did_win
    FROM
        games
),
games_with_previous_win AS (
    SELECT
        *,
        LAG(did_win) OVER(PARTITION BY club_id ORDER BY date) AS did_win_game_before
    FROM
        initial_games
),
games_with_streak_start AS (
    SELECT
        *,
        CASE
            WHEN did_win <> did_win_game_before THEN 1
            ELSE 0
        END AS streak_start
    FROM
        games_with_previous_win
),
games_with_streak_id AS (
    SELECT
        *,
        SUM(streak_start) OVER(PARTITION BY club_id ORDER BY date) AS streak_id
    FROM
        games_with_streak_start
),
win_streaks AS (
    SELECT
        club_id,
        streak_id,
        MIN(date) AS streak_start_date,
        MAX(date) AS streak_end_date,
        COUNT(*) AS win_streak_length
    FROM
        games_with_streak_id
    WHERE
        did_win = 1
    GROUP BY
        club_id,
        streak_id
),
streak_results AS (
    SELECT 
        games_with_streak_id.streak_id,
        games_with_streak_id.club_id,
        games_with_streak_id.date,
        games_with_streak_id.aggregate,
        games_with_streak_id.did_win,
        win_streaks.streak_start_date,
        win_streaks.streak_end_date,
        win_streaks.win_streak_length,
        DENSE_RANK() OVER(PARTITION BY games_with_streak_id.club_id, games_with_streak_id.streak_id ORDER BY games_with_streak_id.date) AS streak_day
    FROM
        games_with_streak_id
    JOIN
        win_streaks ON games_with_streak_id.club_id = win_streaks.club_id AND games_with_streak_id.streak_id = win_streaks.streak_id
    ORDER BY
        win_streaks.win_streak_length DESC, games_with_streak_id.club_id, games_with_streak_id.date
)
SELECT DISTINCT 
	games.home_club_name,
    streak_results.streak_start_date,
    streak_results.streak_end_date,
    streak_results.win_streak_length
FROM
    streak_results
right JOIN games 
    ON streak_results.club_id = games.home_club_id
    where games.home_club_name != "4-3-3 Attacking"
ORDER BY
    streak_results.win_streak_length DESC;
