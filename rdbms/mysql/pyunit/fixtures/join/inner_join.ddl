CREATE TABLE user(id INT NOT NULL AUTO_INCREMENT, name CHAR(15) NOT NULL, PRIMARY KEY (id));
CREATE TABLE profile(id INT NOT NULL AUTO_INCREMENT, user_id INT, password VARCHAR(30), email VARCHAR(50), PRIMARY KEY (id), INDEX user_idx (user_id), FOREIGN KEY (user_id) REFERENCES user(id) ON DELETE CASCADE);