from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime

default_args = {
  'owner' : 'nemtsev',
  'depends_on_past' : False,
  'start_date' : datetime(2018, 1, 1),
  'retries' : 0
} # owner - любая строка, но лучше фамилия, depends_on_past - будет ли след раз запускаться, если в прошлый раз упал(False-будет), start_date - начиная с какой даты 
# будет работать, retries - сколько раз будет пытаться запустить, если в 1 раз не сработал
dag = DAG('my_example', 
          default_args = default_args,
          catchup = False,
          schedule_interval = '00 20 * * *')# 1 параметр - произвольное название, default_args - просто аргументы, которые мы задали заранее(можно так не делать и прям
# в функции DAG писать все аргументы), catchup - , schedule_internal - расписание minutes, hours, days, month, year(вроде так, но ласт могут другое быть) (если нам надо
# каждый день или месяц и тд запускать, то ставим на это место *
def hello():
  return print('hello, world')
def sum_int():
  return print(2+2)

t1 = PythonOperator(
  task_id='print_hello_world',
  python_callable=hello,
  dag=dag)# 1 аргумент - название, 2-й - функция, которую нужно запускать, dag - даг, который мы до этого написали
t2 = PythonOperator(
  task_id='print_hello_world',
  python_callable=sum_int,
  dag=dag)
t1 >> t2# строим зависимость 2 task от 1(если не сработает 1, то и 2 не будет запускаться)
