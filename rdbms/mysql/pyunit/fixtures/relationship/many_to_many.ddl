CREATE TABLE employee(id INT NOT NULL AUTO_INCREMENT, first_name CHAR(15) NOT NULL, last_name CHAR(15) NOT NULL, PRIMARY KEY (id));
CREATE TABLE dept(id INT NOT NULL AUTO_INCREMENT, name VARCHAR(12), acct_code INT UNSIGNED, PRIMARY KEY (id));
CREATE TABLE employee_dept(emp_id INT NOT NULL, dept_id INT NOT NULL, PRIMARY KEY (emp_id, dept_id), FOREIGN KEY (emp_id) REFERENCES employee(id), FOREIGN KEY (dept_id) REFERENCES dept(id));
