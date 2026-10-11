CREATE TABLE `communities` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `updated_at` datetime(3) DEFAULT NULL,
  `deleted_at` datetime(3) DEFAULT NULL,
  `name` longtext,
  `owner_id` bigint(20) unsigned DEFAULT NULL,
  `img` longtext,
  `desc` longtext,
  PRIMARY KEY (`id`),
  KEY `idx_communities_deleted_at` (`deleted_at`)
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=utf8;

CREATE TABLE `contact` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `updated_at` datetime(3) DEFAULT NULL,
  `deleted_at` datetime(3) DEFAULT NULL,
  `owner_id` bigint(20) unsigned DEFAULT NULL,
  `target_id` bigint(20) unsigned DEFAULT NULL,
  `type` bigint(20) DEFAULT NULL,
  `desc` longtext,
  PRIMARY KEY (`id`),
  KEY `idx_contact_deleted_at` (`deleted_at`)
) ENGINE=InnoDB AUTO_INCREMENT=185 DEFAULT CHARSET=utf8;

CREATE TABLE `group_basic` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `updated_at` datetime(3) DEFAULT NULL,
  `deleted_at` datetime(3) DEFAULT NULL,
  `name` longtext,
  `owner_id` bigint(20) unsigned DEFAULT NULL,
  `icon` longtext,
  `type` bigint(20) DEFAULT NULL,
  `desc` longtext,
  PRIMARY KEY (`id`),
  KEY `idx_group_basic_deleted_at` (`deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 冷存储消息表（全量归档）：id 显式写入雪花ID（53bit），非自增；落库与读路径见 models/store.go
-- 旧结构（form_id/target_id longtext）与本 DDL 不兼容；直接跑本脚本会先 DROP 重建，
-- 也可以在开发库手动 DROP TABLE `message`; 后由服务启动时的 models.Migrate()（AutoMigrate）建表
DROP TABLE IF EXISTS `message`;
CREATE TABLE `message` (
  `id` bigint(20) unsigned NOT NULL,
  `created_at` datetime(3) DEFAULT NULL,
  `updated_at` datetime(3) DEFAULT NULL,
  `deleted_at` datetime(3) DEFAULT NULL,
  `user_id` bigint(20) unsigned DEFAULT NULL,
  `target_id` bigint(20) DEFAULT NULL,
  `type` bigint(20) DEFAULT NULL,
  `media` bigint(20) DEFAULT NULL,
  `content` longtext,
  `create_time` bigint(20) unsigned DEFAULT NULL,
  `read_time` bigint(20) unsigned DEFAULT NULL,
  `pic` longtext,
  `url` longtext,
  `desc` longtext,
  `amount` bigint(20) DEFAULT NULL,
  `conv_key` varchar(64) DEFAULT NULL,
  `seq` bigint DEFAULT NULL,
  `client_msg_id` varchar(64) DEFAULT NULL,
  `read_count` bigint DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `idx_message_deleted_at` (`deleted_at`),
  KEY `idx_message_conv_key` (`conv_key`),
  UNIQUE KEY `uk_conv_seq` (`conv_key`,`seq`),
  UNIQUE KEY `uk_conv_cmid` (`conv_key`,`client_msg_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 会话已读游标（Redis read: 键的持久账本；私聊 read_time 回填与 Type=5 回执依据）
CREATE TABLE `user_read_cursor` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `updated_at` datetime(3) DEFAULT NULL,
  `deleted_at` datetime(3) DEFAULT NULL,
  `user_id` bigint(20) unsigned DEFAULT NULL,
  `conv_key` varchar(64) DEFAULT NULL,
  `last_seq` bigint DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_conv` (`user_id`,`conv_key`),
  KEY `idx_user_read_cursor_deleted_at` (`deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `user_basic` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `updated_at` datetime(3) DEFAULT NULL,
  `deleted_at` datetime(3) DEFAULT NULL,
  `name` longtext,
  `pass_word` longtext,
  `phone` longtext,
  `email` longtext,
  `identity` longtext,
  `client_ip` longtext,
  `client_port` longtext,
  `login_time` datetime(3) DEFAULT NULL,
  `heartbeat_time` datetime(3) DEFAULT NULL,
  `login_out_time` datetime(3) DEFAULT NULL,
  `is_logout` tinyint(1) DEFAULT NULL,
  `device_info` longtext,
  `salt` longtext,
  `avatar` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_user_basic_deleted_at` (`deleted_at`)
) ENGINE=InnoDB AUTO_INCREMENT=26 DEFAULT CHARSET=utf8;














