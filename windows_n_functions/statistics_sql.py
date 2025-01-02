import sqlite3

table = 'employees'

connection = sqlite3.connect('window.db')
cursor = connection.cursor()

# colnames
cursor.execute(f'PRAGMA table_info("employees")')
column_names = [i[1] for i in cursor.fetchall()]
print(*[str(x).center(12) for x in column_names], sep = '|')
print('-'*66)
# solution
cursor.execute('''
    SELECT id, name, department, 
        salary, SUM(salary) OVER w AS total
    FROM employees
    WINDOW w AS (
        PARTITION BY department 
        ORDER BY salary
        ROWS BETWEEN unbounded preceding and current row
               )
    ORDER BY department, salary, id
''')
# output
for x in cursor.fetchall():
    for y in list(x):
        print(str(y).center(12) + '|', end='')
    print()

connection.commit()
connection.close()