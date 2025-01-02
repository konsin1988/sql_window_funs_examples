import sqlite3

connection = sqlite3.connect('window.db')
cursor = connection.cursor()

# cursor.execute('''
#     SELECT year, month, expense, 
#     PRINTF('%g', ROUND(AVG(expense) OVER w)) as roll_avg 
#     FROM expenses
#     WINDOW w AS (ORDER BY year, month
#                ROWS BETWEEN 1 preceding AND 1 following)
#     ORDER BY year, month
# ''')

# -------------- Скользящее среднее по доходам -------
# cursor.execute('''
#     SELECT year, month, income,
#         ROUND(AVG(income) OVER w) AS roll_avg
#     FROM expenses
#     WINDOW w AS (
#         ORDER BY year, month
#         ROWS BETWEEN 1 preceding and CURRENT ROW
#                )
#     ORDER BY year, month
# ''')


# ----------- Сумма нарастающим итогом ---------------
# cursor.execute('''
#     SELECT year, month, income, expense, 
#         SUM(income) OVER w AS t_income, 
#         SUM(expense) OVER w AS t_expense,
#         SUM(income) OVER w - SUM(expense) OVER w AS t_profit
#     FROM expenses
#     WINDOW w AS (
#                ORDER BY year, month
#                ROWS BETWEEN unbounded preceding AND current row
#                )
#     ORDER BY year, month
# ''')


#--------------Фонд оплаты труда нарастающим итогом -----------------
# colnames
cursor.execute('PRAGMA table_info("employees")')
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
