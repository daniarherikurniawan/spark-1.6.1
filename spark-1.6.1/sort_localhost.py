from pyspark import SparkContext,SparkConf
import shutil,os,  sys, time

# non-spark code
if os.path.exists("../generated_file/result_py"):
    shutil.rmtree("../generated_file/result_py")

conf = SparkConf().setAppName("Sorting").setMain("spark://daniar-X450JF:7077").set("spark.eventLog.enabled","true").set("spark.eventLog.dir","../generated_driver_log/").set("spark.speculation","true")

sc = SparkContext(conf=conf)
text_file = sc.textFile("../generated_file/list_int",2)
counts = text_file.map(lambda a : (int(a),a)).sortByKey("true")

print("start the delay. Inject TC!!")
time.sleep(30)
print("end the delay")
#print counts.collect()
counts.saveAsTextFile("../generated_file/result_py")

# ./bin/spark-submit sort_localhost.py --main spark://daniar-X450JF:7077 --deploy-mode cluster --num-executors 1
