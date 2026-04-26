-- Dim coins
CREATE TABLE IF NOT EXISTS dwh.coins (
	id serial4 NOT NULL,
	"name" text NULL,
	symbol text NULL,
	CONSTRAINT coins_pkey PRIMARY KEY (id)
);
ALTER TABLE dwh.coins
ADD CONSTRAINT uq_coins UNIQUE (name,symbol);