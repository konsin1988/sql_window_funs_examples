import sqlite3

connection = sqlite3.connect('employees.db')
cursor = connection.cursor()

# cursor.execute("""
#     WITH prev AS (
    
#     SELECT id, name, department, salary,
#         LAG(salary, 1) OVER w AS prev
#     FROM employees
#     WINDOW w as (ORDER BY salary)
#     )
#     SELECT name, department, salary,
#         ROUND((salary - prev) * 100 / prev) AS diff
#     FROM prev
#     ORDER BY salary, id;
# """)


# ------ Зарплата соседнего сотрудника ---------
# cursor.execute('''
#     SELECT name, department, 
#         LAG(salary, 1) OVER w AS prev, 
#         salary,
#         LEAD(salary, 1) OVER w AS next
#     FROM employees
#     WINDOW w AS (ORDER BY salary)
#     ORDER BY salary, id;

# ''')

# ----- Сравнение с границами --------------
# cursor.execute('''
#     SELECT name, department, salary, 
#             FIRST_VALUE(salary) OVER w AS low, 
#             LAST_VALUE(salary) OVER w AS high
#     FROM employees
#     WINDOW w AS (
#                PARTITION BY department 
#                ORDER BY salary
#                ROWS BETWEEN unbounded preceding AND unbounded following)
#     ORDER BY department, salary, id
# ''')

# -------Процент от максимальной зарплаты в городе --------
cursor.execute('''
    SELECT name, city, salary, ROUND(salary * 100.0/ LAST_VALUE(salary) OVER w) AS percent
    FROM employees
    WINDOW w AS (PARTITION BY city 
               ORDER BY salary
               ROWS BETWEEN unbounded preceding AND unbounded following)
    ORDER BY city, salary, id;
''')


print(*[x for x in cursor.fetchall()], sep='\n')

connection.commit()
connection.close()

