SET @balance = 0;
CREATE TABLE account(id INT NOT NULL AUTO_INCREMENT, name VARCHAR(20), amount DECIMAL(6,2), PRIMARY KEY (id));
CREATE TRIGGER upd_before_balance BEFORE UPDATE ON account FOR EACH ROW SET @balance = NEW.amount;
