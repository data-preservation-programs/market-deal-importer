/* Number of piece cid and total piece size per replica
SELECT COUNT(*) AS num_piece_cid, SUM(sum) AS total_piece_size, count AS num_replica FROM (SELECT COUNT(*) AS count, SUM(piece_size) AS sum, piece_cid FROM current_state
                 WHERE verified_deal = true
                 GROUP BY piece_cid) AS t
GROUP BY count;
*/

/* CIDs that have been used across multiple clients */
WITH suspicious_cids as (SELECT count(distinct client), piece_cid FROM current_state
    WHERE verified_deal = true
    GROUP BY piece_cid
    HAVING count(distinct client) > 1)
SELECT COUNT(*) AS num_deals, SUM(piece_size) AS total_piece_size, COUNT(DISTINCT current_state.piece_cid) AS distict_piece_cids, client FROM current_state, suspicious_cids
WHERE current_state.piece_cid = suspicious_cids.piece_cid AND current_state.verified_deal = true
GROUP BY client;

/* CIDs stored multiple times with the same SP */
SELECT SUM(c) AS num_deals, SUM(s) AS total_piece_size, provider FROM
(SELECT COUNT(*) AS c, SUM(piece_size) AS s, piece_cid, provider FROM current_state
WHERE verified_deal = true
GROUP BY piece_cid, provider
HAVING COUNT(*) > 1) AS t
GROUP BY provider;

/* CIDs stored multiple times with different clients with the same SP */
SELECT SUM(c) AS num_deals, SUM(s) AS total_piece_size, MAX(d) AS max_num_clients, provider FROM
    (SELECT COUNT(*) AS c, SUM(piece_size) AS s, COUNT(DISTINCT client) as d, piece_cid, provider FROM current_state
     WHERE verified_deal = true
     GROUP BY piece_cid, provider
     HAVING COUNT(DISTINCT client) > 1) AS t
GROUP BY provider;

/* Per client behavior stats */
WITH cid_reuse AS (SELECT piece_cid, provider FROM current_state
                   WHERE verified_deal = true
                   GROUP BY piece_cid, provider
                   HAVING COUNT(*) > 1)
SELECT num_deals, total_piece_size, t1.client AS client, COALESCE(num_deals_cid_reuse, 0) AS num_deals_cid_reuse, COALESCE(total_piece_size_cid_reuse, 0) AS total_piece_size_cid_reuse FROM
(SELECT COUNT(*) AS num_deals, SUM(piece_size) AS total_piece_size, client FROM current_state
WHERE verified_deal = true
GROUP BY client) as t1
LEFT JOIN
(SELECT COUNT(*) AS num_deals_cid_reuse, SUM(piece_size) AS total_piece_size_cid_reuse, client FROM current_state, cid_reuse
WHERE current_state.piece_cid = cid_reuse.piece_cid AND current_state.provider = cid_reuse.provider AND current_state.verified_deal = true
GROUP BY client) as t2
ON t1.client = t2.client;
