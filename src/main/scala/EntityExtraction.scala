import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.semgraph.SemanticGraph
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation
import edu.stanford.nlp.trees.Tree
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created by nico on 28/12/2015.
  */
object EntityExtraction {

  def main(args: Array[String]) {
    /*    if (args.length < 2) {
          System.err.println("Please set arguments for <s3_input_dir> <s3_output_dir>")
          System.exit(1)
        }
        val inputDir = args(0)
        val outputDir = args(1)*/
    val inputDir = "src/main/resources/sentences.txt"
    val conf = new SparkConf().setAppName("Entity Extraction").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputDir)

    //textFile.mapPartitions(lines => partitionGetTrees(lines)).foreach(println) // Rdd[Tree]

    //testing without spark
    //val str = "El Harrach refinery supplies Crude Oil to local Hassi Messaoud by pipeline."
  val str = "Paris regularly supplies Tokyo"
  val properties = new Properties()
    properties.setProperty("annotators", "tokenize, ssplit, parse")

   /* val trees = getTrees(str, new StanfordCoreNLP(properties))
    val root = trees.head
    root.indentedListPrint()
    val c = root.firstChild()
    println(c.nodeString())*/

    val dep = getDepedencies(str, new StanfordCoreNLP(properties))
    val f = dep.head
    sc.stop()
  }


  /**
    * method to initialize the Stanford coreNLP pipeline for each partitions
    * @param iter
    * @return a new Tree list RDD
    */
  def partitionGetTrees(iter: Iterator[String]): Iterator[List[Tree]] = {
    // CoreNLP Initialisation
    val properties = new Properties()
    // annotator parse needs ssplit and tokenize
    properties.setProperty("annotators", "tokenize, ssplit, parse")

    // not sure if transient lazy is better than mapPartition
    // val pipeline = new SparkCoreNLP(properties).get
    iter.map(line => getTrees(line, new StanfordCoreNLP(properties)))
  }

  def getTrees(s: String, pipeline: StanfordCoreNLP) : List[Tree] = {
    val document = new Annotation(s)
    pipeline.annotate(document)
    val sentences = document.get(classOf[SentencesAnnotation])
    var sentenceTrees = List[Tree]()
    // this is a scala map not an rdd map
    sentences.foreach(sentenceTrees ::= _.get(classOf[TreeAnnotation]))
    sentenceTrees
  }

  def getDepedencies(s: String, pipeline: StanfordCoreNLP) : List[SemanticGraph] = {
    val document = new Annotation(s)
    pipeline.annotate(document)
    val sentences = document.get(classOf[SentencesAnnotation])
    var sentenceDep = List[SemanticGraph]()
    // this is a scala map not an rdd map
    sentences.foreach(sentenceDep ::= _.get(classOf[CollapsedCCProcessedDependenciesAnnotation]))
    sentenceDep
  }
}
