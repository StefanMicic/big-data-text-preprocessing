from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from vocabulary_creator import VocabCreator


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def main():
    # HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

    conf = SparkConf().setAppName("example join").setMaster("local")
    # .setMaster("spark://spark-master:7077")

    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    quiet_logs(spark)

    # vocabulary_creator = VocabCreator(HDFS_NAMENODE + "/home/output.csv", spark)
    vocabulary_creator = VocabCreator("../data/train.csv", spark)
    vocabulary_creator.create_vocabulary()
    vocabulary_creator.clean_text()


if __name__ == "__main__":
    main()
