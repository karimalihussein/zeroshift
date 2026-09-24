IF OBJECT_ID('dbo.migration_gate') IS NULL
BEGIN
    CREATE TABLE dbo.migration_gate(id INT PRIMARY KEY CHECK(id=1), frozen BIT NOT NULL);
    INSERT dbo.migration_gate VALUES(1,0);
    CREATE TABLE dbo.customers(id BIGINT IDENTITY PRIMARY KEY, name NVARCHAR(180) NOT NULL, email NVARCHAR(220) NULL, active BIT NOT NULL);
    CREATE TABLE dbo.orders(id BIGINT IDENTITY PRIMARY KEY, customer_id BIGINT NOT NULL REFERENCES dbo.customers(id), amount DECIMAL(19,4) NOT NULL CHECK(amount>=0), status VARCHAR(24) NOT NULL);
    ALTER TABLE dbo.customers ENABLE CHANGE_TRACKING;
    ALTER TABLE dbo.orders ENABLE CHANGE_TRACKING;
END
IF OBJECT_ID('dbo.live_experiment') IS NULL
CREATE TABLE dbo.live_experiment(
 id BIGINT IDENTITY PRIMARY KEY, order_id BIGINT NOT NULL, customer_id BIGINT NOT NULL, operation VARCHAR(12) NOT NULL,
 before_name NVARCHAR(180),before_amount DECIMAL(19,4),before_status VARCHAR(24),
 after_name NVARCHAR(180),after_amount DECIMAL(19,4),after_status VARCHAR(24));
