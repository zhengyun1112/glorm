CREATE SCHEMA IF NOT EXISTS `blog` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ;
USE blog;

begin;

DROP TABLE IF EXISTS `article`;
DROP TABLE IF EXISTS `user`;
DROP TABLE IF EXISTS `comment`;

CREATE TABLE IF NOT EXISTS `user` (
    `user_id` BIGINT NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(50) NOT NULL,
    `password` VARCHAR(50) NOT NULL,
    `is_married` BOOL DEFAULT NULL,
    `age` INT DEFAULT NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE current_timestamp,
    PRIMARY KEY (`user_id`),
    INDEX `INDEX_user_age` (`age` ASC),
    UNIQUE INDEX `UNIQUE_INDEX_user_name` (`name` ASC)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `article` (
    `article_id` BIGINT NOT NULL AUTO_INCREMENT,
    `user_id` BIGINT NOT NULL,
    `title` VARCHAR(512) NOT NULL,
    `state` TINYINT NOT NULL DEFAULT 0 COMMENT "0: published, 1: draft, 2: hidden",
    `content` TEXT DEFAULT NULL,
    `donation` DECIMAL(12, 2) DEFAULT 0.5,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE current_timestamp,
    PRIMARY KEY (`article_id`)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `comment` (
    `comment_id` BIGINT NOT NULL,
    `user_id` BIGINT NOT NULL,
    `article_id` BIGINT NOT NULL,
    `content` TEXT DEFAULT NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE current_timestamp,
    PRIMARY KEY (`comment_id`),
    UNIQUE INDEX UNIQUE_user_article_id (`user_id`, `article_id`)
) ENGINE = InnoDB;

commit;