CREATE TABLE IF NOT EXISTS `address_p2pkh` (
  `address_id` int(10) unsigned NOT NULL,
  `address` binary(20) NOT NULL,
  `wallet_id` int(10) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`address_id`),
  UNIQUE KEY `ix_address` (`address`),
  KEY `ix_wallet_id` (`wallet_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `address_p2sh` (
  `address_id` int(10) unsigned NOT NULL,
  `address` binary(20) NOT NULL,
  `wallet_id` int(10) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`address_id`),
  UNIQUE KEY `ix_address` (`address`),
  KEY `ix_wallet_id` (`wallet_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `address_p2wpkh` (
  `address_id` int(10) unsigned NOT NULL,
  `address` binary(20) NOT NULL,
  `wallet_id` int(10) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`address_id`),
  UNIQUE KEY `ix_address` (`address`),
  KEY `ix_wallet_id` (`wallet_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `address_p2wsh` (
  `address_id` int(10) unsigned NOT NULL,
  `address` binary(32) NOT NULL,
  `wallet_id` int(10) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`address_id`),
  UNIQUE KEY `ix_address` (`address`),
  KEY `ix_wallet_id` (`wallet_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `block` (
  `height` int(10) unsigned NOT NULL,
  `hash` binary(32) NOT NULL,
  `txn_count` int(10) unsigned NOT NULL,
  PRIMARY KEY (`height`),
  UNIQUE KEY `ix_hash` (`hash`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `input` (
  `transaction_id` int(10) unsigned NOT NULL,
  `pos` smallint(5) unsigned NOT NULL,
  `in_transaction_id` int(10) unsigned NOT NULL,
  `in_pos` smallint(5) unsigned NOT NULL,
  PRIMARY KEY (`transaction_id`,`pos`),
  UNIQUE KEY `ix_in_txn_pos` (`in_transaction_id`,`in_pos`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `output` (
  `transaction_id` int(10) unsigned NOT NULL,
  `pos` smallint(5) unsigned NOT NULL,
  `address_id` int(10) unsigned NOT NULL,
  `amount` bigint(16) NOT NULL,
  `spent` tinyint(3) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`transaction_id`,`pos`),
  KEY `ix_address` (`address_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `transaction` (
  `transaction_id` int(10) unsigned NOT NULL,
  `txid` binary(32) NOT NULL,
  `block_height` int(10) unsigned NOT NULL,
  `nInputs` smallint(5) unsigned NOT NULL DEFAULT '0',
  `nOutputs` smallint(5) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`transaction_id`),
  UNIQUE KEY `ix_txid` (`txid`),
  KEY `ix_block_height` (`block_height`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `wallet` (
  `wallet_id` int(10) unsigned NOT NULL,
  `name` varchar(45) DEFAULT NULL,
  `details` varchar(2000) DEFAULT NULL,
  PRIMARY KEY (`wallet_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
