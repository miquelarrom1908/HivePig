########### PART 1 ###########
# a)
hive -e "SELECT id, text, label,
CASE WHEN score > 0 THEN 1 ELSE 0 END AS opinio_optinguda,
CASE 
    WHEN label = (CASE WHEN score > 0 THEN 1 ELSE 0 END) THEN 1
    ELSE 0 
END AS correcte
FROM practica_pig_hive
" | sed 's/[\t]/,/g' | head -n -6 > "part1_apartat_a.csv"

# b)
hive -e "SELECT
SUM(label) AS n_correctes,
COUNT(label)-SUM(label) AS n_incorrectes
FROM(
    SELECT id, text, label,
    CASE WHEN score > 0 THEN 1 ELSE 0 END AS opinio_optinguda,
    CASE 
        WHEN label = (CASE WHEN score > 0 THEN 1 ELSE 0 END) THEN 1
        ELSE 0 
    END AS correcte
    FROM practica_pig_hive
) AS opinions
" | sed 's/[\t]/,/g' | head -n -2 > "part1_apartat_b.csv"

########### PART 2 ###########
#a)
wget https://raw.githubusercontent.com/JoanBF20/Pelis_csv/main/pelis.csv

# b)
hdfs dfs -put pelis.csv /user/cloudera/pig_analisis_opinions
hdfs dfs -chmod 777 /user/cloudera/pig_analisis_opinions/pelis.csv

hive -e "CREATE TABLE IF NOT EXISTS default.pelis (
 id int,
 film string)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ',';"
 
hive -e "LOAD DATA INPATH '/user/cloudera/pig_analisis_opinions/pelis.csv' INTO TABLE default.pelis;"

# c)
hive -e "SELECT film,
SUM(label) AS nlabels_positius,
COUNT(label)-SUM(label) AS nlabels_negatius,
COUNT(label) AS n_opinions,
CASE WHEN AVG(score) > 0 THEN 1 ELSE 0 END AS opinio_optinguda,
CASE WHEN AVG(score) > 0 THEN 'Positiu' ELSE 'Negatiu' END AS opinio_optinguda_text
FROM (
    SELECT film, practica_pig_hive.* FROM practica_pig_hive
    INNER JOIN pelis ON practica_pig_hive.id=pelis.id
) AS joined_tables
GROUP BY film
" | sed 's/[\t]/,/g' | head -n -2 > "part2_apartat_c.csv"

# d)
hive -e "SELECT film,
CASE 
    WHEN AVG(label) > 0.80 THEN 4
    WHEN AVG(label) > 0.60 THEN 3
    WHEN AVG(label) > 0.40 THEN 2
    WHEN AVG(label) > 0.20 THEN 1
    ELSE 0
END AS opinio_labels,
CASE 
    WHEN AVG(score) > 1675 THEN 4
    WHEN AVG(score) > 975 THEN 3
    WHEN AVG(score) > 275 THEN 2
    WHEN AVG(score) > -425 THEN 1
    ELSE 0
END AS opinio_text
FROM (
    SELECT film, practica_pig_hive.* FROM practica_pig_hive
    INNER JOIN pelis ON practica_pig_hive.id=pelis.id
) AS joined_tables
GROUP BY film
" | sed 's/[\t]/,/g' | head -n -2 > "part2_apartat_d.csv"