-- Dim sources
CREATE TABLE IF NOT EXISTS dwh.sources (
	id            serial4   NOT NULL,
	source_name   text      NULL,
	CONSTRAINT sources_pkey PRIMARY KEY (id)
);
