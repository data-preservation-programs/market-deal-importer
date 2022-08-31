/* State of a deal is defined as
   slashed: slash_epoch > 0
   proposed: slash_epoch < 0 && sector_start_epoch < 0
   expired: slash_epoch < 0 && sector_start_epoch > 0 && end_epoch < current_epoch
   active:  slash_epoch < 0 && sector_start_epoch > 0 && end_epoch >= current_epoch
   current_epoch: (unix_timestamp - genesis_epoch) / 30
 */
/* Breakdown of deals per provider */
SELECT COUNT(*) AS num_deals, SUM(piece_size) AS total_piece_size, provider, CASE
    WHEN slash_epoch > 0 THEN 'slashed'
    WHEN slash_epoch < 0 AND sector_start_epoch < 0 THEN 'proposed'
    WHEN slash_epoch < 0 AND sector_start_epoch > 0 AND end_epoch < (extract(epoch from now())::INTEGER - 1598306400) / 30 THEN 'expired'
    ELSE 'active'
    END AS state FROM current_state
                 WHERE client = ? AND verified_deal = true
                 GROUP BY provider, state;

/* Replication factor histogram */
SELECT COUNT(*) AS num_deals, SUM(sum) AS total_piece_size, count AS num_replica FROM (SELECT COUNT(*) AS count, SUM(piece_size) AS sum, piece_cid FROM current_state
                 WHERE client = ? AND verified_deal = true
                 GROUP BY piece_cid) AS t
GROUP BY count;

/* Piece reuse with other clients */
WITH pieces AS (SELECT DISTINCT(piece_cid) AS piece_cid FROM current_state
                 WHERE client = ? AND verified_deal = true)
SELECT COUNT(*) AS num_deals, SUM(piece_size) AS total_piece_size, client FROM current_state, pieces
WHERE client != ? AND verified_deal = true AND current_state.piece_cid = pieces.piece_cid
GROUP BY client;
