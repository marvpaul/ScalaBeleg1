package wordcount
import org.jfree.data.xy.XYSeries
import org.jfree.data.xy.XYSeriesCollection
import org.jfree.chart.renderer.xy.XYDotRenderer
import org.jfree.chart.axis.NumberAxis
import org.jfree.chart.plot.XYPlot
import org.jfree.chart.JFreeChart
import org.jfree.ui.ApplicationFrame
import org.jfree.chart.ChartPanel
import org.jfree.chart.renderer.xy.XYSplineRenderer

import scala.collection.immutable.{ListMap, TreeMap}


/**
 * @author hendrik
 */
class Sentiments (sentiFile:String){
 
  val sentiments:Map[String,Int]= getSentiments(sentiFile)
  
  val proc= new Processing()
  
/**********************************************************************************************
   *
   *                          Aufgabe 5
   *   
   *********************************************************************************************
  */

  /**
    * get a list with lists of words inside. Each sublist has a maximum capacity of wordCount elements
    * @param filename a file where we wanna to read from
    * @param wordCount the capacity of each sublist
    * @return e.g. [[1, ["Hello", "my", "dear"]], [2, ["this", "is", "all"]]]
    */
  def getDocumentGroupedByCounts(filename:String, wordCount:Int):List[(Int, List[String])]= {
    //Load file
    val url=getClass.getResource("/"+filename).getPath
    val src = scala.io.Source.fromFile(url)
    val iter = src.getLines()
    val proc= new Processing()

    //Iterate over each line and seperate in words
    val words = iter.map(x=>proc.getWords(x)).foldLeft(List[String]())((list, line) => list ++ line)

    //Go throught each word
    words.foldLeft(List[(Int, List[String])]())((myList, word) => {
      //Just add the first entry, easy going so far
      if(myList.isEmpty) List((1, List(word)))
        //In case the last list element can take one more word, just add it to the list
      else if(myList.last._2.size < wordCount){
        myList.dropRight(1) ++ List((myList.last._1, myList.last._2 ++ List(word))) //add new word to last elem
      }
      //Section of list is full, add another section
      else {
        val newList = myList ++ List((myList.last._1 + 1, List(word)))
        newList
      }
    })
  }


  /**
    * Simple sentiment alaysis of a given file
    * @param l a given list with words
    * @return
    */
  def analyzeSentiments(l:List[(Int,List[String])]):List[(Int, Double, Double)]= {
    //Get the sentiments
    val sentiAnalyse= new Sentiments("AFINN-111.txt").sentiments
    //Get a list with each paragraph, the sentiment words and the number of total words
    val sentiWordsAndValues = l.foldLeft(List[(Int, List[(String, Int)], Int)]())((list, paragraph) => {
      list ++ List((paragraph._1, paragraph._2.foldLeft(List[(String, Int)]())((list, word) => sentiAnalyse.get(word) match{
        //Add the word in case its a sentiment word
        case Some(elem) => List.concat(list, List((word, sentiAnalyse.get(word).last)))
        case None => list
      }), paragraph._2.length))
    })
    //Reformat to fit into the required data format
    val result = sentiWordsAndValues.foldLeft(List[(Int, Double, Double)]())((list, paragraph) => {
      val sentiment_values = paragraph._2.foldLeft(List[Int]())((values, word) => word._2 :: values).sum.toDouble / paragraph._2.length.toDouble
      val rel_words_used = paragraph._2.length.toDouble / paragraph._3.toDouble
      List.concat(list, List((paragraph._1, sentiment_values, rel_words_used)))
    })
    result
  }
  
  /**********************************************************************************************
   *
   *                          Helper Functions
   *   
   *********************************************************************************************
  */
  
  def getSentiments(filename:String):Map[String,Int]={
        
    val url=getClass.getResource("/"+filename).getPath
    val src = scala.io.Source.fromFile(url)
    val iter = src.getLines()
    val result:Map[String,Int]= (for (row <- iter) yield {val seg= row.split("\t"); (seg(0) -> seg(1).toInt)}).toMap
    src.close()
    result
  }
  
  
  def createGraph(data:List[(Int,Double,Double)]):Unit={
    
    val series1:XYSeries = new XYSeries("Sentiment-Werte");
    for (el <- data){series1.add(el._1,el._2)}
    val series2:XYSeries  = new XYSeries("Relative Haeufigkeit der erkannten Worte");
    for (el <- data){series2.add(el._1,el._3)}
    
    val dataset1:XYSeriesCollection  = new XYSeriesCollection();
    dataset1.addSeries(series1);
    val dataset2:XYSeriesCollection  = new XYSeriesCollection();
    dataset2.addSeries(series2);
    
    val dot:XYDotRenderer = new XYDotRenderer();
    dot.setDotHeight(5);
    dot.setDotWidth(5);
    
    val spline:XYSplineRenderer = new XYSplineRenderer();
    spline.setPrecision(10);
    
    val x1ax:NumberAxis= new NumberAxis("Abschnitt");
    val y1ax:NumberAxis = new NumberAxis("Sentiment Werte");
    val x2ax:NumberAxis= new NumberAxis("Abschnitt");
    val y2ax:NumberAxis = new NumberAxis("Relative Haeufigfkeit");
    
    val plot1:XYPlot  = new XYPlot(dataset1,x1ax,y1ax, spline);
    val plot2:XYPlot  = new XYPlot(dataset2,x2ax,y2ax, dot);

    val chart1:JFreeChart  = new JFreeChart(plot1);
    val chart2:JFreeChart  = new JFreeChart(plot2);
    val frame1:ApplicationFrame = new ApplicationFrame("Sentiment-Analyse: Sentiment Werte"); 
    val frame2:ApplicationFrame = new ApplicationFrame("Sentiment-Analyse: Relative Häufigkeiten"); 
    val chartPanel1: ChartPanel = new ChartPanel(chart1);
    val chartPanel2: ChartPanel = new ChartPanel(chart2);
    
    frame1.setContentPane(chartPanel1);
    frame1.pack();
    frame1.setVisible(true);
    frame2.setContentPane(chartPanel2);
    frame2.pack();
    frame2.setVisible(true);
  }
}
