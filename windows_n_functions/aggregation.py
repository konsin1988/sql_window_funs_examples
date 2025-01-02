import sqlite3

connection = sqlite3.connect('employees.db')
cursor = connection.cursor()

# --- процент от этого фонда составляет зарплата каждого сотрудника ------

# round without zeros !!!!!!!!!!!!!!!!!!!!!!!!

# cursor.execute("""
#     SELECT name, department, salary, 
#         SUM(salary) OVER w AS fund, 
#         CONCAT(PRINTF('%g', 
#         ROUND(salary * 100.0/SUM(salary) OVER w)), '%') AS perc     /*round without zeros*/
#     FROM employees
#     WINDOW w as (PARTITION BY department)
#     ORDER BY department, salary, id
# """)

# ВАЖНО: если делать в оконной функции сортировку, то сумма считается по фрейму, а если не делать,
# то по секции целиком. Т.е. при сортировке нужно использовать 
# ROWS BETWEEN unbounded preceding AND unbounded following, а без сортировки этого не требуется
# 'partition by department задает окно, которое состоит из трех секций (по секции на департамент). 
# Если добавить order by — в окне появится фрейм, которым можно управлять через инструкцию rows. 
# Окна с фреймами подробно рассматриваются дальше по курсу.'


# ---- Фонд оплаты труда по городу -------------------------
# cursor.execute('''
#     SELECT name, city, salary,
#         SUM(salary) OVER w AS fund,
#         PRINTF('%g', ROUND(salary * 100.0/SUM(salary) OVER w)) AS perc
#     FROM employees
#     WINDOW w AS (PARTITION BY city)
#     ORDER BY city, salary, id;
# ''')


# ---- Средняя зарплата по департаменту -------
# Мы хотим для каждого сотрудника увидеть:

#     сколько человек трудится в его отделе (emp_cnt);
#     какая средняя зарплата по отделу (sal_avg);
#     на сколько процентов отклоняется его зарплата от средней по отделу (diff).

cursor.execute('''
    SELECT name, department, salary,
        COUNT(*) OVER w AS emp_cnt,
        ROUND(AVG(salary) OVER w) AS sal_avg,
        CONCAT(PRINTF('%g', ROUND((salary - AVG(salary) OVER w) * 100.0 / AVG(salary) OVER w)), '%') AS diff
    FROM employees
    WINDOW w AS (PARTITION BY department)
    ORDER BY department, salary, id;
''')

    # Взять нужные таблицы (from) и соединить их при необходимости (join).
    # Отфильтровать строки (where).
    # Сгруппировать строки (group by).
    # Отфильтровать результат группировки (having).
    # Взять конкретные столбцы из результата (select), убрать дубли строк (distinct).
    # Рассчитать значения оконных функций (function() over window).
    # Отсортировать то, что получилось (order by)
    # Ограничить количество результатов (limit / fetch / top).
# Таким образом, окна отрабатывают уже после фильтрации и группировки результатов. Поэтому в нашем запросе fund отражает не сумму всех зарплат по департаменту, а сумму только по самарским сотрудникам.

# Решение — использовать подзапрос с окном и фильтровать его в основном запросе:
cursor.execute('''
    WITH emp AS (
  SELECT
    name, city, salary,
    SUM(salary) OVER w AS fund
  FROM employees
  WINDOW w AS (PARTITION BY department)
  ORDER BY department, salary, id
)
SELECT name, salary, fund
FROM emp
WHERE city = 'Самара';
''')

# emp_count покажет общее количество сотрудников, 
# а fund — общий фонд оплаты труда по всем записям employees.

cursor.execute('''
    SELECT
  name, department, salary,
  COUNT(*) OVER () AS emp_count,
  SUM(salary) OVER () AS fund
FROM employees
ORDER BY department, salary, id;
''')

#  --------------- Порядок выполнения 1 --------------
cursor.execute('''
    select
  city,
  department,
  sum(salary) as dep_salary, 
  sum(sum(salary)) over (partition by city) as x,
  sum(sum(salary)) over () as y 
from employees
group by city, department
order by city, department;
''')


print(*[x for x in cursor.fetchall()], sep='\n')

connection.commit()
connection.close()
