-- Dim cities
CREATE TABLE IF NOT EXISTS dwh.cities (
	id serial4 NOT NULL,
	name text NULL,
	CONSTRAINT cities_pkey PRIMARY KEY (id)
);
