Newtable= load '/home/sujay/Withredlinks' AS(outgoing:chararray,ingoing:chararray);


Newtable_for = foreach Newtable generate *;
joint_table= JOIN Newtable by ingoing,Newtable_for by outgoing;
prj= FOREACH joint_table generate Newtable::outgoing;
Ds_prj = distinct prj;
group_all= GROUP Ds_prj ALL;
title_count= FOREACH group_all GENERATE COUNT(Ds_prj);
pri= FOREACH join_table generate Newtable::outgoing,Newtable::ingoing;
Ds_prj1 = distinct pri;
group_outgoing = GROUP Ds_prj1 by $0;
outgoing_count= FOREACH group_outgoing GENERATE group, COUNT($1);

PR = FOREACH Ds_prj1 GENERATE Newtable::outgoing as out,Newtable::ingoing as in,1.0/title_count.$0 as cnt,0.15/title_count.$0 as fml,0.85 as cons;


outlink = JOIN PR by out ,outgoing_count by group; 


cal =  FOREACH outlink GENERATE $0 as out, $1 as in, $2 as cnt, $3 as fml, $4 as cons, $6 as out_cnt, ($2/$6)*$4 as splitter;


PR_group = GROUP cal by in;


calculation = FOREACH PR_group GENERATE group, SUM(cal.splitter) + 0.15/title_count.$0 as cnt;


cal1 = JOIN PR by out, calculation by group;



PR1 = FOREACH cal1 GENERATE PR::out as out, PR::in as in, calculation::cnt as cnt, PR::fml as fml, PR::cons as cons; 


outlink = JOIN PR1 by out ,outgoing_count by group; 


cal =  FOREACH outlink GENERATE $0 as out, $1 as in, $2 as cnt, $3 as fml, $4 as cons, $6 as out_cnt, ($2/$6)*$4 as splitter;


PR_group = GROUP cal by in;


calculation1 = FOREACH PR_group GENERATE group, SUM(cal.splitter) + 0.15/title_count.$0 as cnt;


cal1 = JOIN PR1 by out, calculation1 by group;



PR2 = FOREACH cal1 GENERATE PR1::out as out, PR1::in as in, calculation1::cnt as cnt, PR1::fml as fml, PR1::cons as cons;


outlink = JOIN PR2 by out ,outgoing_count by group;


cal =  FOREACH outlink GENERATE $0 as out, $1 as in, $2 as cnt, $3 as fml, $4 as cons, $6 as out_cnt, ($2/$6)*$4 as splitter;


PR_group = GROUP cal by in;
calculation2 = FOREACH PR_group GENERATE group, SUM(cal.splitter) + 0.15/title_count.$0 as cnt;


cal1 = JOIN PR2 by out, calculation2 by group;



PR3 = FOREACH cal1 GENERATE PR2::out as out, PR2::in as in, calculation2::cnt as cnt, PR2::fml as fml, PR2::cons as cons;


outlink = JOIN PR3 by out ,outgoing_count by group; 


cal =  FOREACH outlink GENERATE $0 as out, $1 as in, $2 as cnt, $3 as fml, $4 as cons, $6 as out_cnt, ($2/$6)*$4 as splitter;


PR_group = GROUP cal by in;


calculation3 = FOREACH PR_group GENERATE group, SUM(cal.splitter) + 0.15/title_count.$0 as cnt;


cal1 = JOIN PR3 by out, calculation3 by group;



PR4 = FOREACH cal1 GENERATE PR3::out as out, PR3::in as in, calculation3::cnt as cnt, PR3::fml as fml, PR3::cons as cons;


outlink = JOIN PR4 by out ,outgoing_count by group; 


cal =  FOREACH outlink GENERATE $0 as out, $1 as in, $2 as cnt, $3 as fml, $4 as cons, $6 as out_cnt, ($2/$6)*$4 as splitter;

PR_group = GROUP cal by in;


calculation4 = FOREACH PR_group GENERATE group, SUM(cal.splitter) + 0.15/title_count.$0 as cnt;


cal1 = JOIN PR4 by out, calculation4 by group;



PR5 = FOREACH cal1 GENERATE PR4::out as out, PR4::in as in, calculation4::cnt as cnt, PR4::fml as fml, PR4::cons as cons;


outlink = JOIN PR5 by out ,outgoing_count by group; 


cal =  FOREACH outlink GENERATE $0 as out, $1 as in, $2 as cnt, $3 as fml, $4 as cons, $6 as out_cnt, ($2/$6)*$4 as splitter;

PR_group = GROUP cal by in;


calculation5 = FOREACH PR_group GENERATE group, SUM(cal.splitter) + 0.15/title_count.$0 as cnt;


cal1 = JOIN PR5 by out, calculation5 by group;



PR6 = FOREACH cal1 GENERATE PR5::out as out, PR5::in as in, calculation5::cnt as cnt, PR5::fml as fml, PR5::cons as cons;



outlink = JOIN PR6 by out ,outgoing_count by group;


cal =  FOREACH outlink GENERATE $0 as out, $1 as in, $2 as cnt, $3 as fml, $4 as cons, $6 as out_cnt, ($2/$6)*$4 as splitter;

PR_group = GROUP cal by in;


calculation6 = FOREACH PR_group GENERATE group, SUM(cal.splitter) + 0.15/title_count.$0 as cnt;


cal1 = JOIN PR6 by out, calculation6 by group;



PR7 = FOREACH cal1 GENERATE PR6::out as out, PR6::in as in, calculation6::cnt as cnt, PR6::fml as fml, PR6::cons as cons;


outlink = JOIN PR7 by out ,outgoing_count by group; 


cal =  FOREACH outlink GENERATE $0 as out, $1 as in, $2 as cnt, $3 as fml, $4 as cons, $6 as out_cnt, ($2/$6)*$4 as splitter;


PR_group = GROUP cal by in;


calculation7 = FOREACH PR_group GENERATE group, SUM(cal.splitter) + 0.15/title_count.$0 as cnt;


cal1 = JOIN PR7 by out, calculation7 by group;



PR8 = FOREACH cal1 GENERATE PR7::out as out, PR7::in as in, calculation7::cnt as cnt, PR7::fml as fml, PR7::cons as cons;




outlink = JOIN PR8 by out ,outgoing_count by group; 


cal =  FOREACH outlink GENERATE $0 as out, $1 as in, $2 as cnt, $3 as fml, $4 as cons, $6 as out_cnt, ($2/$6)*$4 as splitter;


PR_group = GROUP cal by in;


calculation8 = FOREACH PR_group GENERATE group, SUM(cal.splitter) + 0.15/title_count.$0 as cnt;


cal1 = JOIN PR8 by out, calculation8 by group;



PR9 = FOREACH cal1 GENERATE PR8::out as out, PR8::in as in, calculation8::cnt as cnt, PR8::fml as fml, PR8::cons as cons;




outlink = JOIN PR9 by out ,outgoing_count by group; 


cal =  FOREACH outlink GENERATE $0 as out, $1 as in, $2 as cnt, $3 as fml, $4 as cons, $6 as out_cnt, ($2/$6)*$4 as splitter;


PR_group = GROUP cal by in;


calculation9 = FOREACH PR_group GENERATE group, SUM(cal.splitter) + 0.15/title_count.$0 as cnt;


cal1 = JOIN PR9 by out, calculation9 by group;


extract = FILTER cal1 by (university_name matches 'university of.*') OR (university_name matches '.*university');

Ordering = order extract by cnt DESC;

filtered= FILTER Ordering BY cnt < 5/title_count.$0;

STORE result_ord_desc INTO 's3://sujay/Project/output11';
