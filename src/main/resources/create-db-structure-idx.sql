CREATE UNIQUE INDEX `ix_input_in_txn_pos` ON `input` (`in_transaction_id`,`in_pos`);

CREATE INDEX `ix_output_address_id` ON `output` (`address_id`);

CREATE INDEX `ix_transaction_block_height` ON `transaction` (`block_height`);

