CREATE TABLE `t` (
  `id` int NOT NULL AUTO_INCREMENT,
  `test_type` varchar(255) DEFAULT NULL,
  `test_category` int DEFAULT NULL,
  `created` timestamp NULL DEFAULT NULL,
  `calendar_date` datetime DEFAULT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_type_cat` (`test_type`,`test_category`),
  KEY `idx_created` (`created`)
)
