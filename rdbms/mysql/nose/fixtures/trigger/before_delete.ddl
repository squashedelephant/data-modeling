SET @deleted_rows = 0;
CREATE TABLE account(id INT NOT NULL AUTO_INCREMENT, name VARCHAR(20), amount DECIMAL(6,2), PRIMARY KEY (id));
CREATE TRIGGER del_before_rows BEFORE DELETE ON account FOR EACH ROW SET @deleted_rows = @deleted_rows + 1;
