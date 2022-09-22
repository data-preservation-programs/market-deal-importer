CREATE MATERIALIZED VIEW IF NOT EXISTS duplicate_deals_across_clients AS
SELECT SUM(C2.piece_size) AS total_piece_size, COUNT(*) AS num_deals,
         COUNT(DISTINCT C2.piece_cid) AS distinct_piece_cids,
       C1.client AS client1, C2.client AS client2
FROM current_state C1, current_state C2
WHERE C1.piece_cid = C2.piece_cid
  AND C1.verified_deal = true AND C2.verified_deal = true
  AND C1.client != C2.client
  AND C1.slash_epoch < 0 AND C1.sector_start_epoch > 0
  AND C2.slash_epoch < 0 AND C2.sector_start_epoch > 0
  AND C1.end_epoch > (extract(epoch from now())::INTEGER - 1598306400) / 30
  AND C2.end_epoch > (extract(epoch from now())::INTEGER - 1598306400) / 30
GROUP BY client1, client2;

/* CIDs stored multiple times with the same SP */
CREATE MATERIALIZED VIEW IF NOT EXISTS duplicate_deals_per_sp AS
SELECT SUM(c) AS num_deals, SUM(s) AS total_piece_size,
       COUNT(piece_cid) AS num_distinct_piece_cids, provider FROM
(SELECT COUNT(*) AS c, SUM(piece_size) AS s, piece_cid, provider FROM current_state
WHERE verified_deal = true
  AND slash_epoch < 0 AND sector_start_epoch > 0
  AND end_epoch > (extract(epoch from now())::INTEGER - 1598306400) / 30
GROUP BY piece_cid, provider
HAVING COUNT(*) > 2) AS t
GROUP BY provider;

CREATE MATERIALIZED VIEW IF NOT EXISTS duplicate_deals_per_cid AS
SELECT COUNT(DISTINCT client) AS num_distinct_clients,
       SUM(piece_size) AS total_piece_size,
       COUNT(*) AS num_deals,
       COUNT(DISTINCT provider) AS num_distinct_providers,
       piece_cid
FROM current_state
WHERE verified_deal = true
  AND slash_epoch < 0 AND sector_start_epoch > 0
  AND end_epoch > (extract(epoch from now())::INTEGER - 1598306400) / 30
GROUP BY piece_cid
HAVING COUNT(DISTINCT client) > 1
Order by total_piece_size DESC
