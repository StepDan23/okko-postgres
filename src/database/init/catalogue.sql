DROP TABLE IF EXISTS catalogue CASCADE;

CREATE TABLE catalogue
(
  "element_uid" integer PRIMARY KEY,
  "type" character(10) NULL,
  "duration" smallint NOT NULL,
  "attributes" smallint[] NULL,
  "availability" text[] NULL,
  "feature_1" timestamp NOT NULL,
  "feature_2" real NOT NULL,
  "feature_3" smallint NOT NULL,
  "feature_4" real NOT NULL,
  "feature_5" real NOT NULL
);