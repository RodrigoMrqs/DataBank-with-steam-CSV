SELECT g.name, gen.genre, g.release_date, (gm.positive + gm.negative) AS avaliacoes_totais, ROUND((gm.positive::numeric / (gm.positive + gm.negative)::numeric), 2) AS percentual_positivo
FROM games AS g
JOIN genres_games AS gem ON (gem.app_id_game = g.app_id)
JOIN genres AS gen ON (gen.id = gem.id_genre)
JOIN game_metrics AS gm ON (gm.app_id = g.app_id)
WHERE gen.genre = 'RPG' AND g.release_date >= '2024-01-03' AND (gm.positive + gm.negative >= 10000) AND ((gm.positive::numeric / (gm.positive + gm.negative)::numeric) >= 0.8)
ORDER BY avaliacoes_totais;
