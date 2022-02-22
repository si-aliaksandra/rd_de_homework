#!/usr/bin/env python
# coding: utf-8

# In[93]:


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import *
import psycopg2
from hdfs import InsecureClient
from pyspark.sql.types import IntegerType

import os
from datetime import datetime


# In[2]:


pg_creds = {
'host': '127.0.0.1',
'port': '5432',
'database': 'pagila' ,
'user': 'pguser',
'password': 'secret'
}


# In[3]:


client = InsecureClient('127.0.0.1:50070', user='user')


# In[4]:


spark = SparkSession.builder.config('spark.driver.extraClassPath'
            , '/home/user/Shared/postgresql-42.3.2.jar')\
    .master('local')\
    .appName("hw6_spark")\
    .getOrCreate()


# In[12]:


df_film_category = spark.read     .format("jdbc")     .option("url", "jdbc:postgresql://localhost:5432/pagila")     .option("dbtable", "film_category")     .option("user", "pguser")     .option("password", "secret")     .option("driver", "org.postgresql.Driver")     .load()


# In[63]:


df_category = spark.read.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/pagila")\
    .option("dbtable", "category")\
    .option("user", "pguser")\
    .option("password", "secret")\
    .option("driver", "org.postgresql.Driver")\
    .load()


# In[64]:


df_film = spark.read.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/pagila")\
    .option("dbtable", "film")\
    .option("user", "pguser")\
    .option("password", "secret")\
    .option("driver", "org.postgresql.Driver")\
    .load()


# In[67]:


df_film_actor = spark.read.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/pagila")\
    .option("dbtable", "film_actor")\
    .option("user", "pguser")\
    .option("password", "secret")\
    .option("driver", "org.postgresql.Driver")\
    .load()


# In[68]:


df_actor = spark.read.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/pagila")\
    .option("dbtable", "actor")\
    .option("user", "pguser")\
    .option("password", "secret")\
    .option("driver", "org.postgresql.Driver")\
    .load()


# In[69]:


df_inventory= spark.read.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/pagila")\
    .option("dbtable", "inventory")\
    .option("user", "pguser")\
    .option("password", "secret")\
    .option("driver", "org.postgresql.Driver")\
    .load()


# In[70]:


df_rental = spark.read.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/pagila")\
    .option("dbtable", "rental")\
    .option("user", "pguser")\
    .option("password", "secret")\
    .option("driver", "org.postgresql.Driver")\
    .load()


# In[83]:


df_payment = spark.read.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/pagila")\
    .option("dbtable", "payment")\
    .option("user", "pguser")\
    .option("password", "secret")\
    .option("driver", "org.postgresql.Driver")\
    .load()


# In[129]:


df_customer = spark.read.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/pagila")\
    .option("dbtable", "customer")\
    .option("user", "pguser")\
    .option("password", "secret")\
    .option("driver", "org.postgresql.Driver")\
    .load()


# In[130]:


df_address = spark.read.format("jdbc")\
.option("url", "jdbc:postgresql://localhost:5432/pagila")\
.option("dbtable", "address")\
.option("user", "pguser")\
.option("password", "secret")\
.option("driver", "org.postgresql.Driver")\
.load()


# In[131]:


df_city = spark.read     .format("jdbc")     .option("url", "jdbc:postgresql://localhost:5432/pagila")     .option("dbtable", "city")     .option("user", "pguser")     .option("password", "secret")     .option("driver", "org.postgresql.Driver")     .load()


# 1. вывести количество фильмов в каждой категории, отсортировать по убыванию.

# In[50]:


task1 = df_category.join(df_film_category, df_category.category_id == df_film_category.category_id).groupby("name").count()
task1.sort(desc("count")).show()


# 2. вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.

# In[82]:


df_actor = df_actor.withColumn("full_name", F.concat(
                             F.col('first_name')
                             ,F.lit(' ')
                             ,F.col('last_name')
                         ))
task2 = df_film_actor.join(df_film, df_film_actor.film_id == df_film.film_id)\
    .join(df_actor, df_film_actor.actor_id == df_actor.actor_id)\
    .join(df_inventory, df_inventory.film_id == df_film.film_id)\
    .join(df_rental, df_rental.inventory_id == df_inventory.inventory_id)\
    .groupby(df_actor.actor_id, df_actor.full_name).count().sort(desc("count"))
task2.select("full_name","count").show(n=10)


# 3. вывести категорию фильмов, на которую потратили больше всего денег.

# In[106]:


df_payment.describe()


# In[119]:


task3 = df_film_category.join(df_category, 
                              df_category.category_id == df_film_category.category_id)\
.join(df_film, df_film.film_id == df_film_category.film_id)\
.join(df_inventory, df_film.film_id == df_inventory.film_id)\
.join(df_rental, df_rental.inventory_id == df_inventory.inventory_id)\
.join(df_payment, df_rental.rental_id == df_payment.rental_id)\
.groupby(df_category.name)\
.agg({'amount': 'sum'})\
.sort(desc("sum(amount)"))\
task3.show(n=1)


# 4. вывести названия фильмов, которых нет в inventory.

# In[121]:


df_film.createOrReplaceTempView('film')
df_inventory.createOrReplaceTempView('inventory')


# In[122]:


task4 = spark.sql('''
select
	f.title
from
	film f
where
	not exists (
	select
		film_id
	from
		inventory i
	where
		i.film_id = f.film_id)
''')
task4.show()


# 5. вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех..

# In[123]:


df_film_actor.createOrReplaceTempView('film_actor')
df_film.createOrReplaceTempView('film')
df_actor.createOrReplaceTempView('actor')
df_film_category.createOrReplaceTempView('film_category')
df_category.createOrReplaceTempView('category')


# In[128]:


task5 = spark.sql('''
select
	actor_name
from    (
select
	a.actor_id,
	full_name as actor_name,
	rank() over (
	order by count(f.film_id) desc) as actor_rank
from
	actor a
join film_actor fa on
	a.actor_id = fa.actor_id
join film f on
	fa.film_id = f.film_id
join film_category fc on
	f.film_id = fc.category_id
join category c on
	fc.category_id = c.category_id
where
	c.name = 'Children'
group by
	a.actor_id, full_name )
where
	actor_rank <= 3
''')
task5.show()


# 6. вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.
# 

# In[146]:


df_customer.createOrReplaceTempView('customer')
df_address.createOrReplaceTempView('address')
df_city.createOrReplaceTempView('city')


# In[137]:


task6 = spark.sql('''
select
	c2.city ,
	count(distinct case when c.active = 1 then c.customer_id end) as active_customers,
	count(distinct case when coalesce(c.active, 0) = 0 then c.customer_id end) as inactive_customers
from
	customer c
left join address a on
	c.address_id = a.address_id
left join city c2 on
	a.city_id = c2.city_id
group by
	city
order by
	inactive_customers desc
''')
task6.show(n=20)


# 7. вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”.

# In[149]:


df_rental.createOrReplaceTempView('rental')
task7 = spark.sql('''
select
	city_name,
	category_name
from
	(
select
	c2.city_name,
	c.name category_name,
	row_number () over (partition by city_name
order by
	sum(date_part('day', r.return_date - r.rental_date) * 24 + date_part('hour', r.return_date - r.rental_date)) desc) as rnum
from
	category c
join film_category fc on
	c.category_id = fc.category_id
join film f on
	fc.film_id = f.film_id
join inventory i on
	i.film_id = f.film_id
join rental r on
	i.inventory_id = r.inventory_id
join customer c3 on
	r.customer_id = c3.customer_id
join address a on
	c3.address_id = a.address_id
join 
(
	select
		city_id,
		'city begins with "a"' city_name
	from
		city
	where
		city like 'A%'
union
	select
		city_id,
		'city contains "-"' city_name
	from
		city
	where
		city like '%-%') c2 on
	a.city_id = c2.city_id
group by
	c2.city_name,
	c.name)
where
	rnum = 1
''')
task7.show()


# In[ ]:




