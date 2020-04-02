DROP TABLE IF EXISTS catalogue CASCADE;
DROP TYPE IF EXISTS MOVIE_TYPE;

CREATE TYPE MOVIE_TYPE AS ENUM ('movie', 'series', 'multipart_movie');

CREATE TABLE catalogue
(
  "element_uid" integer PRIMARY KEY,
  "type" MOVIE_TYPE NOT NULL,
  "duration" integer NOT NULL,
  "attributes" int[] NULL,
  "availability" text[] NULL,
  "feature_1" double precision NULL,
  "feature_2" real NULL,
  "feature_3" smallint NULL,
  "feature_4" real NULL,
  "feature_5" real NULL
);