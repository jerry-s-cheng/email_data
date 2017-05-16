
BEGIN;
CREATE TABLE "activity_list" (
    "list_id" varchar(20) NOT NULL PRIMARY KEY,
    "name" varchar(255) NOT NULL,
    "member_count" integer NOT NULL,
    "unsubscribe_count" integer NOT NULL,
    "rlid" integer,
    "src" varchar(1) NOT NULL,
    UNIQUE ("list_id", "src")
)

;
CREATE TABLE "activity_customer" (
    "id" INT IDENTITY(0,1) NOT NULL PRIMARY KEY,
    "customer_id" varchar(30) ,
    "new_customer_id" varchar(30) ,
    "list_id" varchar(20) NOT NULL REFERENCES "activity_list" ("list_id"),
    "timestamp_opt" timestamp with time zone,
    "last_changed" timestamp with time zone,
    "email" varchar(75) NOT NULL,
    "status" varchar(1),
    "segment" varchar(20),
    "avg_open" double precision,
    "avg_click" double precision,
    "RIID" integer,
    "src" varchar(1),
    UNIQUE ("list_id", "email")
)
;
CREATE TABLE "activity_campaign" (
    "id" INT IDENTITY(0,1) NOT NULL PRIMARY KEY,
    "campaign_id" varchar(30) NOT NULL,
    "list_id" varchar(20) NOT NULL REFERENCES "activity_list" ("list_id"),
    "send_time" timestamp with time zone,
    "emails_sent" integer,
    "opens" integer,
    "clicks" integer,
    "unique_opens" integer,
    "unique_clicks" integer,
    "open_rate" double precision,
    "click_rate" double precision,
    "src" varchar(1) NOT NULL
)
;
CREATE TABLE "activity_activity" (
    "id" INT IDENTITY(0,1) NOT NULL PRIMARY KEY,
    "campaign_id" varchar(20) NOT NULL REFERENCES "activity_campaign" ("id"),
    "customer_id" varchar(20),
    "list_id" varchar(20) NOT NULL REFERENCES "activity_list" ("list_id"),
    "action" integer NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    "ip" varchar(100),
    "device" varchar(100),
    "url" varchar(500),
    "email_format" varchar(1),
    "timestamp_complaint" timestamp with time zone,
    "reason" varchar(100),
    "source" varchar(100),
    "src" varchar(1),
    "email" varchar(75),
    "RIID" integer
)
;
CREATE TABLE "activity_responsysfile" (
    "name" varchar(100) NOT NULL PRIMARY KEY,
    "is_processed" boolean NOT NULL,
    "created_at" timestamp with time zone NOT NULL,
    "updated_at" timestamp with time zone NOT NULL
)
;


COMMIT;