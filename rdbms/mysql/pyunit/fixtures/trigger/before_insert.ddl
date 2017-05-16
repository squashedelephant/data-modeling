SET @sum = 0;
CREATE TABLE account(id INT NOT NULL AUTO_INCREMENT, name VARCHAR(20), amount DECIMAL(6,2), PRIMARY KEY (id));
CREATE TRIGGER ins_before_sum BEFORE INSERT ON account FOR EACH ROW SET @sum = @sum + NEW.amount;
