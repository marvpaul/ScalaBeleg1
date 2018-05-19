package wordcount
import common._
import mapreduce.BasicOperations

class Processing {
   
  /**********************************************************************************************
   *
   *                          Aufgabe 1
   *   
   *********************************************************************************************
  */
  def getWords(line:String):List[String]={
    /*
     * Extracts all words in a line
     * 
     * 1. Removes all characters which are not letters (A-Z or a-z)
     * 2. Shifts all words to lower case
     * 3. Extracts all words and put them into a list of strings
     */
    line.replaceAll("[^A-Za-z ]", " ").replaceAll("  *", " ").toLowerCase.split(" ").filter(x => x!="").toList
  } 
  
  def getAllWords(l:List[(Int,String)]):List[String]={
    
    /*
     * Extracts all Words from a List containing tupels consisting
     * of a line number and a string
     * The Words should be in the same order as they occur in the source document
     * 
     * Hint: Use the flatMap function
     */
	  l.flatMap(x => getWords(x._2))
  }
  
  def countTheWords(l:List[String]):List[(String,Int)]={

    /*
     *  Gets a list of words and counts the occurences of the individual words
     */
    l.groupBy(w => w).map(x => (x._1, x._2.count(t => true))).toList
  }
  
  /**********************************************************************************************
   *
   *                          Aufgabe 2
   *   
   *********************************************************************************************
  */
  
  def mapReduce[String,Tuple,MapStringInt](mapFun:(String=>Tuple),
      redFun:(MapStringInt,Tuple)=>MapStringInt,
      base:MapStringInt,
      l:List[String]):MapStringInt =

  l.map(mapFun).foldLeft(base)(redFun)
  
  def countTheWordsMR(l:List[String]):List[(String,Int)]= {
   mapReduce[String, (String, Int), Map[String, Int]](
	   s => (s, 1),
	   (map, tuple) => map.get(tuple._1) match {
		   case Some(elem) => map.filterKeys(_ != tuple._1) ++ List((tuple._1, tuple._2 + 1)).toMap
		   case None => map + tuple
	   },
	   Map[String, Int](),
	   l
   ).toList
  }
  
  
  /**********************************************************************************************
   *
   *                          Aufgabe 3
   *   
   *********************************************************************************************
  */      
  
    def getAllWordsWithIndex(l:List[(Int,String)]):List[(Int,String)]= {
	    /*
     * Extracts all Words from a List containing tupels consisting
     * of a line number and a string
     */
    var words_in_line_with_number = l.map(x=>getWords(x._2).map(z=>(x._1, z)))
    words_in_line_with_number.foldLeft(List[(Int, String)]())((list, line)=>line ++ list)
    }
    

    
  
   def createInverseIndex(l:List[(Int,String)]):Map[String,List[Int]]={

     l.foldLeft(Map[String, List[Int]]())((list, word_with_linenr) =>  list.get(word_with_linenr._2) match{
	     case Some(elem) => {
		     Map(word_with_linenr._2 -> List.concat(List(word_with_linenr._1),list.get(word_with_linenr._2).head)) ++ list.filterKeys(_ != word_with_linenr._2)
	     }//Add line number to existing extry in map
	     case None => list ++ Map(word_with_linenr._2 -> List(word_with_linenr._1)) //Add new word to map because it dosent exists at the moment
     }
     )
   } 
  
   def andConjunction(words:List[String], invInd:Map[String,List[Int]]):List[Int]={
	   val lines_where_word_is_in = getLinesWhereAWordIsIn(words, invInd)
	   val word_occurs_in_all_lines = lines_where_word_is_in.length == words.length
	   lines_where_word_is_in.foldLeft(List[Int]())((list, new_line_occurences) => {
		   if(list.isEmpty && word_occurs_in_all_lines){
			   List.concat(new_line_occurences, list) //Case the list is empty add the lines where a word is in to list
		   } else if(word_occurs_in_all_lines){
			   //Case some lines are already in list, check if the new word occurs in the same lines
			   List.concat(new_line_occurences, list).diff(List.concat(new_line_occurences, list).distinct).distinct
		   } else{
			   List() //Case no word exists in all lines
		   }
	   })
   }

	def getLinesWhereAWordIsIn(words:List[String], invInd:Map[String,List[Int]]):List[List[Int]] ={
		words.foldLeft(List[List[Int]]())((list, word_to_search_for) => invInd.get(word_to_search_for) match {
			case Some(elem) => {
				invInd.get(word_to_search_for).head :: list //Add all lines where the current word occures in
			}
			case None => list
		})
	}
  
   def orConjunction(words:List[String], invInd:Map[String, List[Int]]):List[Int]={
	   val lines_where_word_is_in = getLinesWhereAWordIsIn(words, invInd)
	   lines_where_word_is_in.foldLeft(List[Int]())((list, lines) =>{
		   List.concat(list, lines).distinct //Add each line where one word occurs to list, distinct makes each line number unique
	   })
   }
}


object Processing{
  
  def getData(filename:String):List[(Int,String)]={

    val url= getClass.getResource("/"+filename).getPath
    val src = scala.io.Source.fromFile(url)
    val iter = src.getLines()
    var c = -1
    val result= (for (row <- iter) yield {c=c+1;(c,row)}).toList
    src.close()
    result
  }
}