CREATE TABLE transaction(id INT NOT NULL AUTO_INCREMENT, name CHAR(20), PRIMARY KEY (id));
CREATE TABLE account(id INT NOT NULL AUTO_INCREMENT, first_name CHAR(20), last_name CHAR(20), code SMALLINT, PRIMARY KEY(id));
CREATE TABLE account_history(id INT NOT NULL AUTO_INCREMENT, acct_id INT, xact_id INT, transaction_date DATE, transaction_time TIME, amount DECIMAL(6,2), PRIMARY KEY(id), FOREIGN KEY (acct_id) REFERENCES account(id), FOREIGN KEY (xact_id) REFERENCES transaction(id));
