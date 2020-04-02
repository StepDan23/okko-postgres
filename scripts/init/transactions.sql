DROP TABLE IF EXISTS transactions CASCADE;
DROP TYPE IF EXISTS PAYMENT_TYPE;

CREATE TYPE PAYMENT_TYPE AS ENUM ('S', 'R', 'P');

CREATE TABLE transactions
(
  "element_uid" integer NOT NULL,
  "user_uid" integer NOT NULL,
  "consumption_mode" PAYMENT_TYPE,
  "ts" double precision,
  "watched_time" integer,
  "device_type" smallint,
  "device_manufacturer" smallint
);
