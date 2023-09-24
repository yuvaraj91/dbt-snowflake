-- Create table with sample data
CREATE OR REPLACE TABLE public.vnt
(id varchar,src variant)
AS SELECT column1, parse_json(column2) as src
FROM values
('aaa', '{"a": 1,"b": 2,"c": 3}'),
('bbb', '{"a": 1,"b": 2,"c": 3,"d": 4}'),
('ccc', '{"b": 2,"c": 3,"e": 5}');
