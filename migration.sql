-- ============================================================
-- Migration: Add network_slug to cashback_webhooks
-- Run this ONCE on your WordPress database before starting
-- ============================================================

-- 1. Add network_slug column to cashback_webhooks (if not exists)
ALTER TABLE `wp_cashback_webhooks`
    ADD COLUMN IF NOT EXISTS `network_slug` VARCHAR(64) DEFAULT NULL AFTER `payload_norm`,
    ADD INDEX IF NOT EXISTS `idx_network_slug` (`network_slug`);

-- 2. Create affiliate_networks table if it doesn't exist yet
CREATE TABLE IF NOT EXISTS `wp_cashback_affiliate_networks` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(255) NOT NULL,
    `slug` VARCHAR(64) NOT NULL,
    `is_active` TINYINT(1) NOT NULL DEFAULT 1,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_slug` (`slug`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 3. Insert common CPA networks (skip if already exist)
INSERT IGNORE INTO `wp_cashback_affiliate_networks` (`name`, `slug`) VALUES
    ('Admitad', 'admitad'),
    ('ActionPay', 'actionpay'),
    ('CityAds', 'cityads'),
    ('GdeSlon', 'gdeslon'),
    ('Leads.su', 'leads_su');

-- ============================================================
-- Migration: Replace payload_norm with payload_hash for dedup
-- Run this ONCE after upgrading to hash-based deduplication
-- ============================================================

-- 4. Add stored generated column with SHA-256 hash
ALTER TABLE `wp_cashback_webhooks`
    ADD COLUMN `payload_hash` CHAR(64) GENERATED ALWAYS AS (SHA2(JSON_NORMALIZE(`payload`), 256)) STORED AFTER `payload_norm`;

-- 5. Move unique key from payload_norm to payload_hash
ALTER TABLE `wp_cashback_webhooks`
    DROP INDEX `uk_payload_norm`,
    ADD UNIQUE KEY `uk_payload_hash` (`payload_hash`);

-- 6. Drop payload_norm (no longer needed)
ALTER TABLE `wp_cashback_webhooks`
    DROP COLUMN `payload_norm`;
