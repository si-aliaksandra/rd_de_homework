/*Используя демо базу данных, напишите запросы для того, чтобы:

1. вывести количество фильмов в каждой категории, отсортировать по убыванию.

2. вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.

3. вывести категорию фильмов, на которую потратили больше всего денег.

4. вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.

5. вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.

6. вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.

7. вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе. */

--1 вывести количество фильмов в каждой категории, отсортировать по убыванию.

select
	c.name as category,
	count(distinct film_id) film_cnt
from
	film_category fc
join category c on
	fc.category_id = c.category_id
group by
	c.name
order by
	cnt desc;

--2 вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.

select
	concat(a.first_name, ' ', a.last_name) as actor,
	count(r.rental_id) as rental_count
from
	film_actor fa
join film f on
	fa.film_id = f.film_id
join actor a on
	fa.actor_id = a.actor_id
join inventory i on
	i.film_id = f.film_id 
join rental r on
	i.inventory_id = r.inventory_id 
group by
	a.actor_id,
	concat( a.first_name, ' ', a.last_name)
order by
	rental_count desc
limit 10;

--3 вывести категорию фильмов, на которую потратили больше всего денег.
select
	c.name as category,
	sum(p.amount) amount
from
	film_category fc
join category c on
	fc.category_id = c.category_id
join film f on
	fc.film_id = f.film_id
join inventory i on
	f.film_id = i.film_id
join rental r on
	i.inventory_id =r.inventory_id
join payment p on
	r.rental_id = p.rental_id 
group by
	c.name
order by
	2 desc
limit 1;

--4 вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.

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

--5 вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
		
with cte as (
select
	a.actor_id,
	concat(a.first_name, ' ', a.last_name) as actor_name,
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
	c."name" = 'Children'
group by
	a.actor_id )
select
	actor_name
from
	cte
where
	actor_rank <= 3;

--6 вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.

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
	inactive_customers desc;

--7 вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.


with cte as (
select
	c2.city_name,
	c."name" category_name,
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
		'city name begins with "a"' city_name
	from
		city
	where
		city ilike 'A%'
union
	select
		city_id,
		'city name contains "-"' city_name
	from
		city
	where
		city ilike '%-%') c2 on
	a.city_id = c2.city_id
group by
	c2.city_name,
	c."name")
select
	city_name,
	category_name
from
	cte
where
	rnum = 1
;