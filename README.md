# Program rules in Database

## Content
- `common_function.sql`: script to load some useful functions. These functions could be used by any program. There is no binding to data elements, program or stages.
- `mental_health.sql`: specific functions for Mental Health program. It also include the main execution function of Mental Health.
- `main.sql`: start point for the execution of sql scripts.

## Execution
### On demand
The routine can be executed on demand by using a GUI like PgAdminIII or directly in the command line. The execution order must be:
1. `common_function.sql`
2. `mental_health.sql`
3. `main.sql`

### Scheduled
#### Windows
The script `HMIS_program_rules.bat` runs the routine every 10 seconds in an endless loop. In order to work, the three sql scripts must be loaded into the HMIS like `resources` (in Apps > Reports > Resources). The resource name does not matter; the file name does (do not modify the file name).

This script can be scheduled using the "Scheduled tasks" tool in Windows to run the script when the system starts.

#### Linux
The script `HMIS_program_rules.sh` can be added as a cron job that is executed every minute.

Crontab configuration:
```
MAILTO=""
* * * * * /home/msf/HMIS_program_rules.sh* * * * * /home/msf/HMIS_program_rules.sh > /dev/null

```
