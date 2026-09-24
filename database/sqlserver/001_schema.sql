IF DB_ID('zeroshift') IS NULL CREATE DATABASE zeroshift;
GO
USE zeroshift;
GO
IF OBJECT_ID('customers') IS NULL BEGIN
CREATE TABLE customers (id BIGINT IDENTITY(1,1) PRIMARY KEY, public_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID() UNIQUE, name NVARCHAR(180) NOT NULL, email NVARCHAR(220) NOT NULL UNIQUE, is_active BIT NOT NULL DEFAULT 1, notes NVARCHAR(MAX) NULL, created_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(), updated_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME());
CREATE TABLE products (id BIGINT IDENTITY(1,1) PRIMARY KEY, sku VARCHAR(40) NOT NULL UNIQUE, name NVARCHAR(180) NOT NULL, description NVARCHAR(MAX) NULL, price DECIMAL(19,4) NOT NULL, is_available BIT NOT NULL DEFAULT 1, created_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(), updated_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME());
CREATE TABLE orders (id BIGINT IDENTITY(1,1) PRIMARY KEY, public_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID() UNIQUE, customer_id BIGINT NOT NULL REFERENCES customers(id), status VARCHAR(24) NOT NULL, total DECIMAL(19,4) NOT NULL, shipping_address NVARCHAR(MAX) NULL, placed_at DATETIME2(3) NOT NULL, updated_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME());
CREATE TABLE order_items (id BIGINT IDENTITY(1,1) PRIMARY KEY, order_id BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE, product_id BIGINT NOT NULL REFERENCES products(id), quantity INT NOT NULL CHECK(quantity > 0), unit_price DECIMAL(19,4) NOT NULL, created_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME());
CREATE TABLE payments (id BIGINT IDENTITY(1,1) PRIMARY KEY, public_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID() UNIQUE, order_id BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE, amount DECIMAL(19,4) NOT NULL, method VARCHAR(24) NOT NULL, status VARCHAR(24) NOT NULL, paid_at DATETIME2(3) NULL, created_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME());
CREATE TABLE migration_change_log (change_id BIGINT IDENTITY(1,1) PRIMARY KEY, table_name VARCHAR(80) NOT NULL, operation CHAR(1) NOT NULL, record_id BIGINT NOT NULL, row_data NVARCHAR(MAX) NULL, changed_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME());
CREATE INDEX ix_change_log_unapplied ON migration_change_log(change_id) INCLUDE(table_name,operation,record_id);
END
GO
CREATE OR ALTER TRIGGER customers_capture ON customers AFTER INSERT, UPDATE, DELETE AS BEGIN SET NOCOUNT ON; INSERT migration_change_log(table_name,operation,record_id,row_data) SELECT 'customers',CASE WHEN d.id IS NULL THEN 'I' WHEN i.id IS NULL THEN 'D' ELSE 'U' END,COALESCE(i.id,d.id),(SELECT i.id,i.public_id,i.name,i.email,i.is_active,i.notes,i.created_at,i.updated_at FOR JSON PATH,WITHOUT_ARRAY_WRAPPER) FROM inserted i FULL JOIN deleted d ON i.id=d.id; END;
GO
CREATE OR ALTER TRIGGER products_capture ON products AFTER INSERT, UPDATE, DELETE AS BEGIN SET NOCOUNT ON; INSERT migration_change_log(table_name,operation,record_id,row_data) SELECT 'products',CASE WHEN d.id IS NULL THEN 'I' WHEN i.id IS NULL THEN 'D' ELSE 'U' END,COALESCE(i.id,d.id),(SELECT i.id,i.sku,i.name,i.description,i.price,i.is_available,i.created_at,i.updated_at FOR JSON PATH,WITHOUT_ARRAY_WRAPPER) FROM inserted i FULL JOIN deleted d ON i.id=d.id; END;
GO
CREATE OR ALTER TRIGGER orders_capture ON orders AFTER INSERT, UPDATE, DELETE AS BEGIN SET NOCOUNT ON; INSERT migration_change_log(table_name,operation,record_id,row_data) SELECT 'orders',CASE WHEN d.id IS NULL THEN 'I' WHEN i.id IS NULL THEN 'D' ELSE 'U' END,COALESCE(i.id,d.id),(SELECT i.id,i.public_id,i.customer_id,i.status,i.total,i.shipping_address,i.placed_at,i.updated_at FOR JSON PATH,WITHOUT_ARRAY_WRAPPER) FROM inserted i FULL JOIN deleted d ON i.id=d.id; END;
GO
CREATE OR ALTER TRIGGER order_items_capture ON order_items AFTER INSERT, UPDATE, DELETE AS BEGIN SET NOCOUNT ON; INSERT migration_change_log(table_name,operation,record_id,row_data) SELECT 'order_items',CASE WHEN d.id IS NULL THEN 'I' WHEN i.id IS NULL THEN 'D' ELSE 'U' END,COALESCE(i.id,d.id),(SELECT i.id,i.order_id,i.product_id,i.quantity,i.unit_price,i.created_at FOR JSON PATH,WITHOUT_ARRAY_WRAPPER) FROM inserted i FULL JOIN deleted d ON i.id=d.id; END;
GO
CREATE OR ALTER TRIGGER payments_capture ON payments AFTER INSERT, UPDATE, DELETE AS BEGIN SET NOCOUNT ON; INSERT migration_change_log(table_name,operation,record_id,row_data) SELECT 'payments',CASE WHEN d.id IS NULL THEN 'I' WHEN i.id IS NULL THEN 'D' ELSE 'U' END,COALESCE(i.id,d.id),(SELECT i.id,i.public_id,i.order_id,i.amount,i.method,i.status,i.paid_at,i.created_at FOR JSON PATH,WITHOUT_ARRAY_WRAPPER) FROM inserted i FULL JOIN deleted d ON i.id=d.id; END;
GO
