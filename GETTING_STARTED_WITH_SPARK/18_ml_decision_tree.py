from pyspark.sql import SparkSession
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils

if __name__ == '__main__':
    """
    Classification is a task of identifying the features of an entity and classifying the entity to one of the 
    predefined classes/categories based on the previous knowledge.
    A decision tree has a structure like tree. It has a root which denotes a decision node and also the start of
    classifying a problem instance. A node can branch out. Each branch represents a possible outcome from the decision
    block. Each branch can end up with another node or a class label terminating the classification and ending up with
    the result – class label.
    """
    spark = SparkSession.builder.appName(__name__).master("local[*]").getOrCreate()
    sc = spark.sparkContext

    # Load and parse the data file into an RDD of LabeledPoint.
    path = "/home/username/Projects/MilitaryLocationMap/LearnApacheSpark/lib/python3.9/site-packages/pyspark/data/mllib/sample_libsvm_data.txt"
    data = MLUtils.loadLibSVMFile(sc, path)
    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Train a DecisionTree model.
    #  Empty categoricalFeaturesInfo indicates all features are continuous.
    model = DecisionTree.trainClassifier(trainingData, numClasses=2, categoricalFeaturesInfo={},
                                         impurity='gini', maxDepth=5, maxBins=32)

    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(
        lambda lp: lp[0] != lp[1]).count() / float(testData.count())
    print('Test Error = ' + str(testErr))
    print('Learned classification tree model:')
    print(model.toDebugString())

    # Save and load model
    model.save(sc, "myDecisionTreeClassificationModel")
    sameModel = DecisionTreeModel.load(sc, "myDecisionTreeClassificationModel")

