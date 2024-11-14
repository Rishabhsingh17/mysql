#assigning schema 
use project1;
#database viewing
select * from tested;
#total passengers
select count(PassengerId) as total_passengers from tested;
#checking for duplicate values
select PassengerId, count(*) AS Count
FROM tested
GROUP BY PassengerId
HAVING COUNT(*) > 1;
#checking for null values of each column
select PassengerId from tested where PassengerId is null;
select Survived from tested where Survived is null;
select Pclass from tested where Pclass is null;
select Name from tested where Name is null;
select Sex from tested where Sex is null;
select Age from tested where Age is null;
select SibSp from tested where SibSp is null;
select Parch from tested where Parch is null;
select Ticket from tested where Ticket is null;
select Fare from tested where Fare is null;
select Cabin from tested where Cabin is null;
select Embarked from tested where Embarked is null;
#how many passengers survived or not
select 
sum(case when Survived = 1 then 1 else 0 end) as survived_passengers,
sum(case when Survived = 0 then 1 else 0 end) as not_survived_passengers
from tested;
#how many male and female passengers present
select 
sum(case when Sex = 'male' then 1 else 0 end) as male_passengers,
sum(case when Sex = "female" then 1 else 0 end) as female_passengers
from tested;
#no. of survival based on gender
select 
sum(case when Sex = "male" and Survived = 1 then 1 else 0 end) as survived_male,
sum(case when Sex = "female" and Survived = 1 then 1 else 0 end) as survived_female
from tested;
#age group 
select 
sum(case when Age <= 19 then 1 else 0 end) as teenager,
sum(case when Age >= 20 and Age <= 35 then 1 else 0 end) as young_adult,
sum(case when Age >= 36 and Age <= 55 then 1 else 0 end) as middle_aged_adult,
sum(case when Age > 55 then 1 else 0 end) as senior
from tested;
#how many siblings survived
select Survived,
sum(case when SibSp = 0 then 1 else 0 end) as no_siblings,
sum(case when SibSp = 1 then 1 else 0 end) as one_siblings,
sum(case when SibSp = 2 then 1 else 0 end) as two_siblings,
sum(case when SibSp = 3 then 1 else 0 end) as three_siblings,
sum(case when SibSp = 4 then 1 else 0 end) as four_siblings,
sum(case when SibSp = 5 then 1 else 0 end) as five_siblings,
sum(case when SibSp = 6 then 1 else 0 end) as six_siblings,
sum(case when SibSp > 6 then 1 else 0 end) as many_siblings
from tested
group by Survived;
#passengers boarding with survival status
select Embarked,
sum(case when Survived = 0 then 1 else 0 end) as survived,
sum(case when Survived = 1 then 1 else 0 end) as not_survived
from tested
group by Embarked;
#checking for passengers with same ticket may belong to same family
select Ticket, count(*) as ticket_count
from tested
group by Ticket
having Ticket_count > 1;
#survival based on class and fare price
select Pclass, 
avg(Fare) as avg_fare,
sum(case when Survived = 0 then 1 else 0 end ) as survived,
sum(case when Survived = 1 then 1 else 0 end) as not_survived
from tested
group by Pclass;
#finding family size with survuval
SELECT PassengerId,
       (SibSp + Parch) AS FamilySize,
       Survived
FROM tested;
