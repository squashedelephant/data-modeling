CREATE TABLE inventory(id INT NOT NULL AUTO_INCREMENT, name VARCHAR(20) NOT NULL, sku INT, quantity INT, PRIMARY KEY (id));
CREATE TABLE client_order(id INT NOT NULL AUTO_INCREMENT, inv_id INT, quantity INT, price DECIMAL(5,2), PRIMARY KEY (id), FOREIGN KEY (inv_id) REFERENCES inventory(id));
