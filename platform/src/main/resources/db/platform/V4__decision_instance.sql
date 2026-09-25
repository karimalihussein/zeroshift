-- Which replica handled the delivery: shows partitions moving between instances on a rebalance.
ALTER TABLE consumer_decision ADD COLUMN instance TEXT;
