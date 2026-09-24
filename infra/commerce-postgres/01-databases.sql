-- One PostgreSQL server, one database per service: services share hardware, never tables.
-- Each service owns its schema through its own Flyway migrations.
CREATE DATABASE orders;
CREATE DATABASE payments;
CREATE DATABASE inventory;
CREATE DATABASE shipping;
CREATE DATABASE order_query;
