import sqlite3

connection = sqlite3.connect('employees.db')
cursor = connection.cursor()

cursor.execute("""
    SELECT * FROM employees
""")

print(*[x for x in cursor.fetchall()], sep='\n')


connection.commit()
connection.close()