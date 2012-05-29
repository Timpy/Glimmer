--This version also sorts the data
--NOTE: we do not unescape NTriples encoding

SET job.name GroupBySubject;

REGISTER '$swaJar';
SET default_parallel $nTasks

tuples = LOAD '$input' USING PigStorage('\n') AS (line:chararray);
spoc = FOREACH tuples GENERATE $0 AS raw, com.yahoo.research.barcelona.swa.pig.SplitTuple($0) AS parsed;

projection = FOREACH spoc GENERATE parsed.s AS subject, com.yahoo.research.barcelona.swa.pig.ReplaceWhitespace($0) AS raw;

grouped = GROUP projection BY subject PARALLEL $nTasks;

-- filter out subjects with more than 1000 stmts
filtered = FILTER grouped BY COUNT($1) < 10000;

flat = FOREACH filtered GENERATE group, com.yahoo.research.barcelona.swa.pig.ConcatValuesInBag($1, '  ');
nonempty = FILTER flat BY ($0 neq '');
sorted = ORDER nonempty BY $0;
STORE sorted INTO '$output' USING PigStorage();

-- These will be sorted in the same order as the collection
subjects = FOREACH sorted GENERATE $0;
STORE subjects INTO '$subjects' USING PigStorage();

predicates = FOREACH spoc GENERATE parsed.p;
pred_noempty = FILTER predicates BY ($0 neq '');
pred_uniq = DISTINCT pred_noempty;
pred_sorted = ORDER pred_uniq BY $0 PARALLEL 1;
STORE pred_sorted INTO '$predicates' USING PigStorage();

objprop = FILTER spoc BY parsed.type eq 'o';
objects = FOREACH objprop GENERATE parsed.o;
obj_noempty = FILTER objects BY ($0 neq '');
obj_uniq = DISTINCT obj_noempty;

-- Take only objects that are resources and not appear as subjects
-- obj_group = COGROUP subjects by $0, objects by $0;
-- obj_filt = FILTER obj_group BY IsEmpty(subjects);
-- obj_proj = FOREACH obj_filt GENERATE flatten(objects);

obj_sorted = ORDER obj_uniq BY $0;
STORE obj_sorted INTO '$objects' USING PigStorage();

contexts = FOREACH spoc GENERATE parsed.c;
cont_noempty = FILTER contexts BY ($0 neq '');
cont_uniq = DISTINCT cont_noempty;
cont_sorted = ORDER cont_uniq BY $0;
STORE cont_sorted INTO '$contexts' USING PigStorage();
