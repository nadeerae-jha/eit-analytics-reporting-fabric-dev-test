-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "e2151f83-1def-4511-915d-7999b38fe96b",
-- META       "default_lakehouse_name": "Lakehouse",
-- META       "default_lakehouse_workspace_id": "2b945bca-7740-4ed7-b63d-76a54c3390ad",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "e2151f83-1def-4511-915d-7999b38fe96b"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- Welcome to your new notebook
-- Type here in the cell editor to add code!
-- Create a table in the Lakehouse
CREATE TABLE employees (
    emp_id INT,
    emp_name STRING,
    department STRING,
    salary DECIMAL(10,2)
);

INSERT INTO employees VALUES
(1, 'John Smith', 'IT', 85000.00),
(2, 'Mary Johnson', 'Finance', 78000.00),
(3, 'David Lee', 'HR', 65000.00);

INSERT INTO employees VALUES
(1, 'John Smith', 'IT', 85000.00),
(2, 'Mary Johnson', 'Finance', 78000.00),
(3, 'David Lee', 'HR', 65000.00);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
