CREATE TABLE IF NOT EXISTS `address_p2pkh` (
  `address_id` int(10) unsigned NOT NULL,
  `address` binary(20) NOT NULL,
  `wallet_id` int(10) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`address_id`),
  UNIQUE KEY `ix_address_p2pkh_address` (`address`)
) ENGINE=MyISAM;

CREATE TABLE IF NOT EXISTS `address_p2sh` (
  `address_id` int(10) unsigned NOT NULL,
  `address` binary(20) NOT NULL,
  `wallet_id` int(10) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`address_id`),
  UNIQUE KEY `ix_address_p2sh_address` (`address`)
) ENGINE=MyISAM;

CREATE TABLE IF NOT EXISTS `address_p2wpkh` (
  `address_id` int(10) unsigned NOT NULL,
  `address` binary(20) NOT NULL,
  `wallet_id` int(10) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`address_id`),
  UNIQUE KEY `ix_address_p2wpkh_address` (`address`)
) ENGINE=MyISAM;

CREATE TABLE IF NOT EXISTS `address_p2wsh` (
  `address_id` int(10) unsigned NOT NULL,
  `address` binary(32) NOT NULL,
  `wallet_id` int(10) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`address_id`),
  UNIQUE KEY `ix_address_p2wsh_address` (`address`)
) ENGINE=MyISAM;

CREATE TABLE IF NOT EXISTS `block` (
  `height` int(10) unsigned NOT NULL,
  `hash` binary(32) NOT NULL,
  `txn_count` int(10) unsigned NOT NULL,
  PRIMARY KEY (`height`),
  UNIQUE KEY `ix_block_hash` (`hash`)
) ENGINE=MyISAM;

CREATE TABLE IF NOT EXISTS `input` (
  `transaction_id` int(10) unsigned NOT NULL,
  `pos` smallint(5) unsigned NOT NULL,
  `in_transaction_id` int(10) unsigned NOT NULL,
  `in_pos` smallint(5) unsigned NOT NULL,
  PRIMARY KEY (`transaction_id`,`pos`)
) ENGINE=MyISAM;

CREATE TABLE IF NOT EXISTS `input_special` (
  `transaction_id` int(10) unsigned NOT NULL,
  `pos` smallint(5) unsigned NOT NULL,
  `sighash_type` bit(8) NOT NULL,
  `segwit` bit(1) NOT NULL,
  `multisig` bit(1) NOT NULL,
  PRIMARY KEY (`transaction_id`,`pos`)
) ENGINE=MyISAM;

CREATE TABLE IF NOT EXISTS `output` (
  `transaction_id` int(10) unsigned NOT NULL,
  `pos` smallint(5) unsigned NOT NULL,
  `address_id` int(10) unsigned NOT NULL,
  `amount` bigint(16) NOT NULL,
  `spent` tinyint(3) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`transaction_id`,`pos`)
) ENGINE=MyISAM;

CREATE TABLE IF NOT EXISTS `transaction` (
  `transaction_id` int(10) unsigned NOT NULL,
  `txid` binary(32) NOT NULL,
  `block_height` int(10) unsigned NOT NULL,
  `nInputs` smallint(5) unsigned NOT NULL DEFAULT '0',
  `nOutputs` smallint(5) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`transaction_id`),
  UNIQUE KEY `ix_txid` (`txid`)
) ENGINE=MyISAM;
