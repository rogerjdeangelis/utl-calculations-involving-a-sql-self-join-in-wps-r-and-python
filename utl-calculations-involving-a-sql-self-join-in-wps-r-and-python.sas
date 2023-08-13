%let pgm=utl-calculations-involving-a-sql-self-join-in-wps-r-and-python;

Calculations involving a sql self join in wps r and python

github
https://tinyurl.com/26wc9yr3
https://github.com/rogerjdeangelis/utl-calculations-involving-a-sql-self-join-in-wps-r-and-python

No need for sepaate tables

    Five solutions

        1 wps proc sql
        2 wps proc r sql
        3 wps proc python sql
        4 R solution by
          https://stackoverflow.com/users/3358272/r2evans
        5 wps merge

https://stackoverflow.com/questions/76885825/find-the-ratio-between-two-groups

I know these SQL solutions are trivial for the SAS-L brain trust, but I get a lot more
traffic on github for simple solutions like this, (python & r users maybe).

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

/*---- changes groups to groups due to restritions in some languages     ----*/

options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;

  input Var1 $1. Groups Value;

cards4;
a 0 1
b 0 2
c 0 3
a 1 2
b 1 10
c 1 9
;;;;
run;quit;

/**************************************************************************************************************************/
/*                            |                                                   |                                       */
/*                            |                                                   |                                       */
/* SD1.HAVE total obs=6       | RILES                                             | OUTPUT                                */
/*                            |                                                   |                                       */
/* Obs  VAR1  GROUPS  VALUE   |     var1          DIV                             |     var1          DIV                 */
/*                            |                                                   |                                       */
/*  1    a       0       1    |  1     a    0.5000000  a(1)/a(4)  = 1/2  = .5     |  1     a    0.5000000                 */
/*  2    b       0       2    |  2     b    0.2000000  b(2)/a(5)  = 2/10 = .2     |  2     b    0.2000000                 */
/*  3    c       0       3    |  3     c    0.3333333  c(3)/a(6)  = 3/9  = .3333  |  3     c    0.3333333                 */
/*  4    a       1       2    |                                                   |                                       */
/*  5    b       1      10    |                                                   |                                       */
/*  6    c       1       9    |                                                   |                                       */
/*                            |                                                   |                                       */
/**************************************************************************************************************************/

/*                                                         _
/ | __      ___ __  ___   _ __  _ __ ___   ___   ___  __ _| |
| | \ \ /\ / / `_ \/ __| | `_ \| `__/ _ \ / __| / __|/ _` | |
| |  \ V  V /| |_) \__ \ | |_) | | | (_) | (__  \__ \ (_| | |
|_|   \_/\_/ | .__/|___/ | .__/|_|  \___/ \___| |___/\__, |_|
             |_|         |_|                            |_|
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x('
libname sd1 "d:/sd1";
options validvarname=any;
proc sql;
  create
     table sd1.want as
  select
    l.var1
   ,l.value/r.value as DIV
  from
    sd1.have as l, sd1.have as r
  where
        l.groups = 0 and r.groups = 1
    and l.var1   = r.var1
;quit;
proc print data=sd1.want;
run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/*  Obs    VAR1        div                                                                                                */
/*                                                                                                                        */
/*   1      a               0.5                                                                                           */
/*   2      b               0.2                                                                                           */
/*   3      c      0.3333333333                                                                                           */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                                                                 _
|___ \  __      ___ __  ___   _ __  _ __ ___   ___   _ __   ___  __ _| |
  __) | \ \ /\ / / `_ \/ __| | `_ \| `__/ _ \ / __| | `__| / __|/ _` | |
 / __/   \ V  V /| |_) \__ \ | |_) | | | (_) | (__  | |    \__ \ (_| | |
|_____|   \_/\_/ | .__/|___/ | .__/|_|  \___/ \___| |_|    |___/\__, |_|
                 |_|         |_|                                   |_|
*/
%utl_submit_wps64x('

libname sd1 "d:/sd1";
proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

proc r;
export data=sd1.have r=have;
submit;
library(sqldf);
have;
want <- sqldf("
  select
    l.var1
   ,l.value/r.value as div
  from
    have as l inner join have as r
  on
        l.groups  = 0 and r.groups = 1
    and l.var1   = r.var1
";);
want;
endsubmit;
import r=want data=sd1.want;
run;quit;
');

/*____                                                          _   _                             _
|___ /  __      ___ __  ___   _ __  _ __ ___   ___  _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
  |_ \  \ \ /\ / / `_ \/ __| | `_ \| `__/ _ \ / __|| `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
 ___) |  \ V  V /| |_) \__ \ | |_) | | | (_) | (__ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
|____/    \_/\_/ | .__/|___/ | .__/|_|  \___/ \___|| .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
                 |_|         |_|                   |_|    |___/                                |_|

*/

%utl_submit_wps64x('

libname sd1 "d:/sd1";
proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

proc python;
export data=sd1.have python=have;
submit;
 from os import path;
 import pandas as pd;
 import numpy as np;
 import pandas as pd;
 from pandasql import sqldf;
 mysql = lambda q: sqldf(q, globals());
 from pandasql import PandaSQL;
 pdsql = PandaSQL(persist=True);
 sqlite3conn = next(pdsql.conn.gen).connection.connection;
 sqlite3conn.enable_load_extension(True);
 sqlite3conn.load_extension("c:/temp/libsqlitefunctions.dll");
 mysql = lambda q: sqldf(q, globals());
 want=pdsql("""
  select
    l.var1
   ,l.value/r.value as div
  from
    have as l inner join have as r
  on
        l.groups = 0 and r.groups = 1
    and l.var1   = r.var1

 """);
print(want);
endsubmit;
import data=sd1.want python=want;
run;quit;
proc print data=sd1.want;
run;quit;
');

/*  _
| || |    _ __
| || |_  | `__|
|__   _| | |
   |_|   |_|

*/

%utl_submit_wps64x('
libname sd1 "d:/sd1";
proc r;
export data=sd1.have r=have;
submit;
library(dplyr);
have %>%
  group_by(VAR1) %>%
  filter(all(0:1 %in% GROUPS)) %>%
  summarize(DIV = VALUE[GROUPS==0]/VALUE[GROUPS==1]);
have;
endsubmit;
import r=want data=sd1.want;
run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/*                                                                                                                        */
/* R                                                                                                                      */
/*                                                                                                                        */
/* # A tibble: 3 x 2                                                                                                      */
/*                                                                                                                        */
/*   VAR1    DIV                                                                                                          */
/*   <chr> <dbl>                                                                                                          */
/*                                                                                                                        */
/* 1 a     0.5                                                                                                            */
/* 2 b     0.2                                                                                                            */
/* 3 c     0.333                                                                                                          */
/*                                                                                                                        */
/* WPS                                                                                                                    */
/*                                                                                                                        */
/*   VAR1 GROUPS VALUE                                                                                                    */
/* 1    a      0     1                                                                                                    */
/* 2    b      0     2                                                                                                    */
/* 3    c      0     3                                                                                                    */
/* 4    a      1     2                                                                                                    */
/* 5    b      1    10                                                                                                    */
/* 6    c      1     9                                                                                                    */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___
| ___|  __      ___ __  ___   _ __ ___   ___ _ __ __ _  ___
|___ \  \ \ /\ / / `_ \/ __| | `_ ` _ \ / _ \ `__/ _` |/ _ \
 ___) |  \ V  V /| |_) \__ \ | | | | | |  __/ | | (_| |  __/
|____/    \_/\_/ | .__/|___/ |_| |_| |_|\___|_|  \__, |\___|
                 |_|                             |___/
*/

%utl_submit_wps64x('

libname sd1 "d:/sd1";

data sd1.want;

  merge sd1.have(where=(groups=0) rename=value=top)
        sd1.have(where=(groups=1) rename=value=bot)
  ;

  div = top/bot;

  drop groups;

run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/* SD1.WANT total obs=3                                                                                                   */
/*                                                                                                                        */
/* Obs    VAR1    TOP    BOT      DIV                                                                                     */
/*                                                                                                                        */
/*  1      a       1       2    0.50000                                                                                   */
/*  2      b       2      10    0.20000                                                                                   */
/*  3      c       3       9    0.33333                                                                                   */
/*                                                                                                                        */
/**************************************************************************************************************************/


/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/


