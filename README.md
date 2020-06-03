# spark-cbrain

CBRAIN plugin for SPARK.


[Boutiques](http://boutiques.github.io) descriptors are in `cbrain_task_descriptors`:
* `spark-sequential.json`: sequential implementation
* `spark-stage[1-3].json`: parallel implementation

The parallel implementation consists in 3 stages: setup (stage 1), parallel computation (stage 2), 
wrap up (stage 3). Examples of Boutiques invocations for each stage are in `examples`. 
