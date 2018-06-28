## Description 

This is a WRENCH-based simulator designed for exploring task
clustering stragegies when wanting to execute a workflow on
a batch scheduled service. 


## Example invocations


<tt>
  - ./simulator --log=root.threshold:critical --log=static_clustering_wms.threshold=info  10 ./NASA-iPSC-1993-3.swf 10 levels:42:10:10:1000 0 zhang:overlap
  - ./simulator 10 ./NASA-iPSC-1993-3.swf 10 levels:42:10:10:1000 0 static:one_job-2

</tt>


 
 

