SELECT g.name, gen.genre, g.release_date, (gm.positive + gm.negative) AS avaliacoes_totais, ROUND((gm.positive::numeric / (gm.positive + gm.negative)::numeric), 2) AS percentual_positivo
FROM games AS g
JOIN genres_games AS gem ON (gem.app_id_game = g.app_id)
JOIN genres AS gen ON (gen.id = gem.id_genre)
JOIN game_metrics AS gm ON (gm.app_id = g.app_id)
WHERE gen.genre = 'RPG' AND g.release_date >= '2024-01-03' AND (gm.positive + gm.negative >= 10000) AND ((gm.positive::numeric / (gm.positive + gm.negative)::numeric) >= 0.8)
ORDER BY avaliacoes_totais;

SELECT g.name, (plt.windows = true AND plt.mac = true AND plt.linux = true) AS all_cross_platform, g.price, gm.avarege_palytime_forever
FROM games AS g
JOIN plataforms AS plt ON (plt.app_id = g.app_id)
JOIN game_metrics AS gm ON (gm.app_id = g.app_id)
WHERE plt.windows = true AND plt.mac = true AND plt.linux = true AND g.price <= 20 AND gm.avarege_palytime_forever >= 120;

SELECT dv.developer, COUNT(g.app_id) AS count_games, AVG(gm.score_rank)
FROM developers AS dv
JOIN developers_games AS dvg ON (dvg.id_developer = dv.id)
JOIN games AS g ON (g.app_id = dvg.app_id_game)
JOIN game_metrics AS gm ON (gm.app_id = g.app_id)
WHERE g.release_date >= '2023-01-09'
GROUP BY dv.developer
HAVING COUNT(g.app_id) > 5
ORDER BY count_games;

SELECT
  COUNT(DISTINCT g.app_id) AS total_jogos,
  (
    SELECT STRING_AGG(five_top_genre.genre, ', ')
    FROM (
      SELECT gen.genre
      FROM games AS g
      JOIN game_metrics AS gm ON gm.app_id = g.app_id
      JOIN genres_games AS gem ON gem.app_id_game = g.app_id
      JOIN genres AS gen ON gen.id = gem.id_genre
      WHERE g.release_date >= '2023-01-06' AND gm.media_playtime_forever > 240
      GROUP BY gen.genre
      ORDER BY COUNT(*) DESC
      LIMIT 5
    ) AS five_top_genre
  ) AS more_frequen_genre
FROM games AS g
JOIN game_metrics AS gm ON gm.app_id = g.app_id
WHERE g.release_date >= '2023-01-06' AND gm.media_playtime_forever > 240;