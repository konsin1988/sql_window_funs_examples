import sqlite3

connection = sqlite3.connect('employees.db')
cursor = connection.cursor()

# cursor.execute('''
# SELECT DENSE_RANK() OVER rank_salary AS rank, name, department, salary 
# FROM employees
# WINDOW rank_salary AS (ORDER BY salary DESC)
# ORDER BY rank, id;
# ''')

# ---- IN ORACLE AND SQL SERVER --------------------
cursor.execute('''
SELECT DENSE_RANK() OVER (
        ORDER BY salary DESC
    ) AS rank, name, department, salary 
FROM employees
ORDER BY rank, id;
''')


print(*[x for x in cursor.fetchall()], sep='\n')



connection.commit()
connection.close()