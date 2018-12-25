from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # create SparkContext, which is imported from pyspark API
    # this context is the entry point to Spark Core
    # our Spark App is word count
    # will run Spark app on embedded Spark instance on our local box, which could use up to 3 cores of our CPU
    # sc.setLogLevel("ERROR")
    # above not necessary if change Spark config files
    conf = SparkConf().setAppName("word count").setMaster("local[3]")
    sc = SparkContext(conf = conf)

    # load word count file as an RDD (resilient distrubted dataset)
    lines = sc.textFile("in/word_count.text")

    #split article into words, whitespace as delimiter
    words = lines.flatMap(lambda line: line.split(" "))

    #calculate occurrence of each word
    wordCounts = words.countByValue()

    #print out the results
    for word, count in wordCounts.items():
        print("{} : {}".format(word, count))
