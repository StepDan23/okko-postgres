DROP TABLE IF EXISTS bookmarks CASCADE;

CREATE TABLE bookmarks
(
  "user_uid" integer NOT NULL,
  "element_uid" integer NOT NULL,
  "ts" double precision NOT NULL
);

ALTER TABLE bookmarks
ADD CONSTRAINT bookmarks_user_uid_element_uid
    UNIQUE ("user_uid", "element_uid");
