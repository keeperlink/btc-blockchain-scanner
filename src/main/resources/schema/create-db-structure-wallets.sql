CREATE TABLE IF NOT EXISTS `wallet` (
  `wallet_id` int(10) unsigned NOT NULL,
  `name` varchar(45) DEFAULT NULL,
  `details` varchar(2000) DEFAULT NULL,
  PRIMARY KEY (`wallet_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE INDEX `ix_wallet_id` ON `address_p2pkh` (`wallet_id`);

CREATE INDEX `ix_wallet_id` ON `address_p2sh` (`wallet_id`);

CREATE INDEX `ix_wallet_id` ON `address_p2wpkh` (`wallet_id`);

CREATE INDEX `ix_wallet_id` ON `address_p2wsh` (`wallet_id`);
