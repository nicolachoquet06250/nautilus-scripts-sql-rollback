/* Database: MySQL */

CREATE DATABASE IF NOT EXISTS test_nautilus_scripts;

USE test_nautilus_scripts;

CREATE TABLE test_table (
	id INT PRIMARY KEY AUTO_INCREMENT NOT NULL
);

ALTER TABLE test_table ADD `name` VARCHAR(255) NOT NULL;
