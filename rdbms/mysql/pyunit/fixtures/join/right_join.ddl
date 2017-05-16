CREATE TABLE customer(id INT NOT NULL AUTO_INCREMENT, name CHAR(15) NOT NULL, contact CHAR(30) NOT NULL, PRIMARY KEY (id));
CREATE TABLE purchase_order(id INT NOT NULL AUTO_INCREMENT, submitted DATE, required DATE, shipped DATE, customer_id INT, PRIMARY KEY (id), INDEX cust_idx (customer_id), FOREIGN KEY (customer_id) REFERENCES customer(id) ON DELETE CASCADE);
