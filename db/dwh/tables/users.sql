-- Dim users
CREATE TABLE IF NOT EXISTS dwh.users (
	id          serial4   NOT NULL,
	full_name   text      NULL,
	email       text      NULL,
	CONSTRAINT users_pkey PRIMARY KEY (id)
);
