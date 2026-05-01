-- Dim exchanges
CREATE TABLE IF NOT EXISTS dwh.exchanges (
	id          serial4   NOT NULL,
	exchange    text      NULL
	CONSTRAINT exchanges_pkey PRIMARY KEY (id)
);
