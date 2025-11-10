-- a. คนที่มี quote_count มากที่สุด
SELECT 
    name,
    quote_count
FROM authors
ORDER BY quote_count DESC
LIMIT 1;


-- b. คนที่แก่ที่สุด มีใครบ้าง เกิดปีไหน
SELECT 
    name,
    birth_year
FROM authors
WHERE birth_year IS NOT NULL
ORDER BY CAST(birth_year AS INT)
LIMIT 1;


-- c. คนที่เกิดในประเทศเยอรมันมีทั้งหมดกี่คนและมี quote รวมทั้งหมดเท่าไหร่
SELECT 
    COUNT(*) AS total_authors,
    SUM(quote_count) AS total_quotes
FROM authors
WHERE birth_country = 'Germany';


-- ex. คนที่เกิดในประเทศเยอรมันทั้งหมดมีใครบ้าง
SELECT 
    name,
    birth_year,
    birth_country,
    quote_count
FROM authors
WHERE birth_country = 'Germany';
