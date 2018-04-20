---

### Batch Queue Resource Scheduling for Workflow Applications
by Zhang, Koelbel, Cooper

#### Relevant quotes:

  - "Our work seeks to reduce the workflow turnaroud time by
intelligently using batch queues without relying on reservations"
  - "One advanced feature of Maui that we do use is the start time estimation functionality"
  - "Our work draws inspiration from [pilot jobs], but attempt to choose a more propitious size for the [pilot jobs]"
    
#### General approach:

  - Decide a group of next levels to run in the DAG based on
	  estimate wait time divided by runtime if run as a single job
  - Submit a corresponding pilot job
  - When the pilot job starts, start running the tasks and 
	  repeat the above to submit the next pilot job
 - So while a pilot job runs, another one travels 
	    through the queue
 - If a running job expires with tasks sill running / not done,
	  then "put them back in the DAG" and cancel all pilot jobs
 - Repeat until everything is executed

The main heuristic is in the first step: how to decide how many levels to aggregate in a job. Their answer is a greedy algorithm that looks the the configuration with the lowest waittime/runtime ratio, but stops at the first local minimum it finds (trying 1 level, 2 levels, etc...)


#### More details

```
  - submitPilotJob(DAG)
  - Wait for an event
  - If event is "pilot job started":
    - schedule all the tasks for that pilot job
    - submitPilotJob(DAG) 
  - If event is "pilot job has expired":
    - cancel all pending pilot jobs 
    - "undo" tasks that were killed or not executed 
      and put them back into 
    - submitPilotJob(DAG)
  - If event is "a task has finished"
  	 - mark it as done, make children ready
  	 - if the pilot job that did that task has no more tasks 
  	   to do, then terminate that pilot job
```

The above has at most one pending pilot job in the queue. The authors talk about "interference" between pilot jobs...
The "smarts" of the above algorithm are in the submitPilotJob() function. 

```
submitPilotJob():
	- job_description = groupByLevel()
	- If the number of consecutive levels is the full DAG
	  height then:
	    -  If the expected runtime <  expected wait time / 2
	       then run the whole DAG as on batch job 
	    - Otherwise, use one batch job per task
	- Otherwise, submit a pilot job for running the consecutive
	  levels together
```

The smarts are in the groupByLevel() function. In case that function says "run the whole DAG" then there are two options
using a simple heuristic to decide "as one job" or "as n jobs for n tasks".  

```
groupByLevel():
   - ratio[0] = + infty
   - For i=1 to DAG.height
     - Consider running the next i levels in one pilot job
   	    - (using as many host as parallelism? not specified)
   	  - ratio[i] = estimated wait time / execution time
   	  - if ratio[i] > ratio[i-1], break
   	  
   - return (i-1, estimated wait time, execution time, num_hosts)
```



