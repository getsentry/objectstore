CREATE TABLE IF NOT EXISTS parts (
	id bigserial NOT NULL PRIMARY KEY,
	part_size int4 NOT NULL,
	"compression" int2 NOT NULL,
	compressed_size int4 NOT NULL,
	segment_id uuid NOT NULL,
	segment_offset int4 NOT NULL
);


CREATE TABLE IF NOT EXISTS blobs (
	id varchar NOT NULL PRIMARY KEY,
	parts int8 ARRAY NOT NULL
);
