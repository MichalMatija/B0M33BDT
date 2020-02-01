#Vytvorenie stlpca slova_poc_unik a jeho naplnenie
from pyspark.sql.types import IntegerType
uniqueWordsCount = F.udf(lambda s: len(set(s)), IntegerType())
songsDF2 = songsDF2.withColumn('slova_poc_unik', uniqueWordsCount(F.split(songsDF2['text'], ' ')))

#Skontrolovanie poctu slov pre prazdny string a jeho korekcia
songsDF2.filter('text=""').show()
songsDF2 = songsDF2.withColumn('slova_poc_unik', F.when(songsDF2['text']=='', 0).otherwise(songsDF2['slova_poc_unik']))
songsDF2.filter('text=""').show()

songsDF2.cache()

#Analyticky dotaz
songsDF2.filter('slova_poc_unik > 0') \
.groupBy('zanr').agg({'*':'count', 'slova_poc_unik':'avg'}) \
.toDF('zanr', 'pocet', 'prumer').orderBy('prumer', ascending=False) \
.show()

'''
+-------------+------+------------------+
|         zanr| pocet|            prumer|
+-------------+------+------------------+
|      Hip-Hop| 24844|209.52302366768637|
|        Other|  5176| 95.52917310664606|
|          Pop| 40413| 93.29064904857348|
|Not Available| 23454| 91.75249424405219|
|          R&B|  3401| 89.06733313731256|
|        Metal| 23737| 88.40561149260648|
|      Country| 14387| 87.91339403628275|
|        Indie|  3148| 86.72045743329097|
|         Folk|  2236| 86.64266547406082|
|         Rock|108632| 85.69962810221666|
|         Jazz|  7971| 76.57633922970768|
|   Electronic|  7964| 72.86288297338021|
+-------------+------+------------------+
'''