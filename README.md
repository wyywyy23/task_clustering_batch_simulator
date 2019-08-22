## Description 

This is a WRENCH-based simulator designed for exploring task
clustering strag\tegies when wanting to execute a workflow on
a batch scheduled service. 


## Example invocations


<tt>
  - ./simulator --log=root.threshold:critical --log=zhang_clustering_wms.threshold=info  10 ./NASA-iPSC-1993-3.swf 10 levels:42:10:10:1000 0 zhang:overlap:plimit conservative_bf

  - ./simulator 10 ./NASA-iPSC-1993-3.swf 10 levels:42:10:10:1000 0 static:one_job-2 conservative_bf

</tt>

### "Zhang" Algorithms

- zhang: based on the paper with "fixes":
  - fix #1: if workflow is too wide, don't abort, just pack tasks in an allocation that maxes out machine capacity
  - fix #2: fix the potentially infinite "leeway" loop (Fig.5, line 11)

- zhang_fixed: Zhang with the following two other fixes
  - potential fix #3: reverse the obviously broken '<' (Fig 5., lines 28 and 31) (contradicts the text in the paper)
  - potential fix #4: do not compare "1 level" to "all" as the baseline in the first iteration of Fig 5., line 8 (instead use "1 level" as the baseline)

- zhang_fixed_global: Zhang_fixed + this feature
    - pick the globally best ratio in the search (not as "greedy" as Zhang)

- zhang_fixed_global_prediction: Zhang_fixed_global + this feature
    - use of prediction to pick the best number of nodes for the by-level grouping

- test: A new cool idea!