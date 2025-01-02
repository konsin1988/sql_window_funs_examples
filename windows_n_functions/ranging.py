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
# cursor.execute('''
# SELECT DENSE_RANK() OVER (
#         ORDER BY salary DESC
#     ) AS rank, name, department, salary 
# FROM employees
# ORDER BY rank, id;
# ''')

# Ranging by ascending and descending in one 
# cursor.execute('''
# SELECT DENSE_RANK() OVER (
#         ORDER BY salary DESC
#     ) AS rank, 
#     DENSE_RANK() OVER (
#         ORDER BY salary
#     ) AS rank_asc,
#     name, department, salary 
# FROM employees
# ORDER BY rank_asc, id;
# ''')


# Ranging by salary in each department
# cursor.execute('''
# SELECT DENSE_RANK() OVER w AS rank, name, department, salary 
# FROM employees
# WINDOW w as (
#     PARTITION BY department
#     ORDER BY salary DESC
# )
# ORDER BY department, salary DESC, id;
# ''')

# -------- TASK ---------------------------
# cursor.execute('''
# select
#   dense_rank() over w as rank,
#   city, name, salary
# from employees
# window w as (
#     PARTITION BY city 
#     ORDER BY salary)
# order by city, rank, id;
# ''')

# ---------- partition into three groups by salary ----
# cursor.execute('''
# SELECT ntile(4) OVER w AS tile, 
#   name, department, salary
# FROM employees
# WINDOW w AS (
#     ORDER BY salary DESC
# )
# ORDER BY salary DESC, id;
# ''')

# ------ Группы по зарплате в городах --------
# cursor.execute('''
# SELECT ntile(2) OVER w AS tile, 
#   name, city, salary
# FROM employees
# WINDOW w AS (
#     PARTITION BY city
#     ORDER BY salary
# )
# ORDER BY city, salary, id;
# ''')

# ------- Самые «дорогие» коллеги -----------
cursor.execute('''
WITH query_1 AS(
SELECT DENSE_RANK() OVER w AS rank_salary, id, name, department, salary
FROM employees
WINDOW w AS (
    PARTITION BY department
    ORDER BY salary DESC
)
ORDER BY department, salary DESC, id
) 
SELECT id, name, department, salary
FROM query_1
WHERE rank_salary = 1;
''')

print(*[x for x in cursor.fetchall()], sep='\n')



connection.commit()
connection.close()