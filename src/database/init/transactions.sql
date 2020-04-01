DROP TABLE IF EXISTS transactions CASCADE;

CREATE TYPE payment_type AS ENUM ('S', 'R', 'P');

CREATE TABLE transactions
(
  "element_uid" integer NOT NULL,
  "user_uid" integer NOT NULL,
  "consumption_mode" payment_type,
  "ts" timestamp,
  "watched_time" integer,
  "device_type" smallint,
  "device_manufacturer" smallint
);
