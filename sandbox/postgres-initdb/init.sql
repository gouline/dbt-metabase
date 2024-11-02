CREATE SCHEMA IF NOT EXISTS inventory;
DROP TABLE IF EXISTS inventory.skus;
CREATE TABLE inventory.skus (sku_id INT PRIMARY KEY, product VARCHAR(50) NOT NULL);
INSERT INTO inventory.skus (sku_id, product) VALUES (1, 'jaffle');
