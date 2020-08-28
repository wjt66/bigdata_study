package cn.demo

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

object linearRegression {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("NewLinearRegression")
      .master("local")
      .getOrCreate()
    val data_path = "D:\\test004.txt"

    val training = spark.read.format("libsvm").load(data_path)   ///加载数据生成dataframe

    training.show()

    val lr = new LinearRegression()
      .setMaxIter(10000)         ///最大迭代次数
      .setRegParam(0.3)          ///设置正则项的参数
      .setElasticNetParam(0.8)    ///

    val lrModel = lr.fit(training)    ///拟合模型

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")    ///输出系数和截距
    ///模型信息总结输出
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")      ///迭代次数的计算
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")   ///每次迭代的目标值，即损失函数+正则化项
    trainingSummary.residuals.show()                 ///每个样本的误差值（即lbael减去预测值）
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")   ////均方根误差
    println(s"r2: ${trainingSummary.r2}")     ///最终的决定系数，0-1之间，值越大拟合程度越高
    trainingSummary.predictions.show()           ///训练集的预测

    spark.stop()
  }
}
