# Queries
## Graph queries
### Line-3
```sql
SELECT G1.src AS A, G2.src AS B, G3.src AS C, G3.dst AS D
FROM G AS G1, G AS G2, G AS G3 
WHERE G1.dst = G2.src AND G2.dst = G3.src 
```

### Line-4
```sql
SELECT G1.src AS A, G2.src AS B, G3.src AS C, G4.src AS D, G4.dst AS E
FROM G AS G1, G AS G2, G AS G3, G AS G4
WHERE G1.dst = G2.src AND G2.dst = G3.src AND G3.dst = G4.src
```

### Line-5
```sql
SELECT G1.src AS A, G2.src AS B, G3.src AS C, G4.src AS D, G5.src AS E, G5.dst AS F
FROM G AS G1, G AS G2, G AS G3, G AS G4, G AS G5
WHERE G1.dst = G2.src AND G2.dst = G3.src AND G3.dst = G4.src AND G4.dst = G5.src
```

### Star-4
```sql
SELECT *
FROM G AS G1, G AS G2, G AS G3, G AS G4
WHERE G1.src = G2.src AND G1.src = G3.src AND G1.src = G4.src
```

### Star-5
```sql
SELECT *
FROM G AS G1, G AS G2, G AS G3, G AS G4, G AS G5
WHERE G1.src = G2.src AND G1.src = G3.src AND G1.src = G4.src AND G1.src = G5.src
```

### Star-6
```sql
SELECT *
FROM G AS G1, G AS G2, G AS G3, G AS G4, G AS G5, G AS G6
WHERE G1.src = G2.src AND G1.src = G3.src AND G1.src = G4.src AND G1.src = G5.src AND G1.src = G6.src
```

### Dumbbell
```sql
SELECT *
FROM G AS G1, G AS G2, G AS G3, G AS G4, G AS G5, G AS G6, G AS G7
WHERE G1.dst = G2.src AND G2.dst = G3.src AND G3.dst = G1.src 
AND G4.dst = G5.src AND G5.dst = G6.src AND G6.dst = G4.src 
AND G1.src = G7.src AND G4.src = G7.dst
```

## Relational queries
### QX
```sql
SELECT *
FROM store_sales, store_returns, catalog_sales,
date_dim d1, date_dim d2 WHERE ss_item_sk = sr_item_sk
AND ss_ticket_number = sr_ticket_number AND sr_customer_sk = cs_bill_customer_sk AND d1.d_date_sk = ss_sold_date_sk
AND d2.d_date_sk = cs_sold_date_sk;
```

### QY
```sql
SELECT *
FROM store_sales, customer c1, household_demographics d1,
customer c2, household_demographics d2 WHERE ss_customer_sk = c1.c_customer_sk
AND c1.c_current_hdemo_sk = d1.hd_demo_sk
AND d1.hd_income_band_sk = d2.hd_income_band_sk AND d2.hd_demo_sk = c2.c_current_hdemo_sk;
```

### QZ
```sql
SELECT *
FROM store_sales, customer c1, household_demographics d1,
item i1, customer c2, household_demographics d2, item i2 WHERE ss_customer_sk = c1.c_customer_sk
AND c1.c_current_hdemo_sk = d1.hd_demo_sk
AND d1.hd_income_band_sk = d2.hd_income_band_sk AND d2.hd_demo_sk = c2.c_current_hdemo_sk
AND ss_item_sk = i1.i_item_sk
AND i1.i_category_id = i2.i_category_id;
```

### Q10
```sql
SELECT *
FROM Message, Tag AS Tag1, Tag AS Tag2, City, Country,
HasTag AS HasTag1, HasTag AS HasTag2,
TagClass, Person AS Person1, Person AS Person2, Knows
WHERE Message.id = HasTag1.message_id 
AND HasTag1.tag_id = Tag1.id 
AND Message.id = HasTag2.message_id 
AND HasTag2.tag_id = Tag2.id 
AND Tag2.type_tag_class_id = TagClass.id 
AND Message.creator_person_id = Person1.id 
AND Person1.location_city_id = City.id 
AND City.part_of_place_id = Country.id 
AND Person1.id = Knows.person1_id 
AND Knows.person2_id = Person2.id
```