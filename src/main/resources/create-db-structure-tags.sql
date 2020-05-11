CREATE TABLE `tag` (
  `tag_id` int(10) unsigned NOT NULL,
  `name` varchar(30) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `input_tag` (
  `transaction_id` int(10) unsigned NOT NULL,
  `pos` int(10) unsigned NOT NULL,
  `tag_id` int(10) unsigned NOT NULL,
  PRIMARY KEY (`transaction_id`,`pos`,`tag_id`)
) ENGINE=MyISAM;

CREATE TABLE `output_tag` (
  `transaction_id` int(10) unsigned NOT NULL,
  `pos` int(10) unsigned NOT NULL,
  `tag_id` int(10) unsigned NOT NULL,
  PRIMARY KEY (`transaction_id`,`pos`,`tag_id`)
) ENGINE=MyISAM;

CREATE TABLE `transaction_tag` (
  `transaction_id` int(10) unsigned NOT NULL,
  `tag_id` int(10) unsigned NOT NULL,
  PRIMARY KEY (`transaction_id`,`tag_id`)
) ENGINE=MyISAM;

CREATE TABLE `wallet_tag` (
  `wallet_id` int(10) unsigned NOT NULL,
  `tag_id` int(10) unsigned NOT NULL,
  PRIMARY KEY (`wallet_id`,`tag_id`)
) ENGINE=MyISAM;
