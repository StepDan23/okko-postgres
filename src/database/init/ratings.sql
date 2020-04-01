DROP TABLE IF EXISTS ratings CASCADE;

CREATE TABLE ratings
(
  "user_uid" integer NOT NULL,
  "element_uid" integer NOT NULL,
  "rating" smallint NOT NULL,
  "ts" timestamp NOT NULL
);

ALTER TABLE ratings
ADD CONSTRAINT ratings_user_uid_element_uid
    UNIQUE ("user_uid", "element_uid");